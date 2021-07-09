package memory

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	partitionCount int // number of partitions being used by PartitionedMemoryIdx
)

func init() {
	cluster.Init("test", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPartitions([]int32{0, 1})
	partitionCount = 2
}

func parseExpressionsMustCompile(t testing.TB, expressions []string) tagquery.Expressions {
	res, err := tagquery.ParseExpressions(expressions)
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions %+v: %s", expressions, err)
	}
	return res
}

func parseQueryMustCompile(t testing.TB, expressions []string) tagquery.Query {
	query, err := tagquery.NewQueryFromStrings(expressions, 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error when parsing query expressions: %s", err)
	}
	return query
}

func getPartition(md *schema.MetricData) int32 {
	return getPartitionFromName(md.Name)
}

func getPartitionFromName(name string) int32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	p := int32(h.Sum32() % uint32(partitionCount))
	if p < 0 {
		p = p * -1
	}
	return p
}

// getSeriesNames returns a count-length slice of random strings comprised of the prefix and count nodes.like.this
func getSeriesNames(depth, count int, prefix string) []string {
	series := make([]string, count)
	for i := 0; i < count; i++ {
		ns := make([]string, depth)
		for j := 0; j < depth; j++ {
			ns[j] = getRandomString(4)
		}
		series[i] = prefix + "." + strings.Join(ns, ".")
	}
	return series
}

// source: https://github.com/gogits/gogs/blob/9ee80e3e5426821f03a4e99fad34418f5c736413/modules/base/tool.go#L58
func getRandomString(n int, alphabets ...byte) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		if len(alphabets) == 0 {
			bytes[i] = alphanum[b%byte(len(alphanum))]
		} else {
			bytes[i] = alphabets[b%byte(len(alphabets))]
		}
	}
	return string(bytes)
}

// getMetricData returns a count-length slice of MetricData's with random Name and the given org id
func getMetricData(orgId uint32, depth, count, interval int, prefix string, tagged bool) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)

	for i, s := range series {
		data[i] = &schema.MetricData{
			Name:     s,
			OrgId:    int(orgId),
			Interval: interval,
		}
		if tagged {
			data[i].Tags = []string{fmt.Sprintf("series_id=%d", i)}
		}
		data[i].SetId()
	}

	return data
}

// getMetricDataWithCustomTags returns a count-length slice of MetricData's with random Name and the given orgId
// unique is a float between 0.0 and 1.0. A total of 10 tags will be added to each MetricData and unique determines how
// many will be identical vs unique. For example, a unique value of 0.4 will add 4 unique tags and 6 identical tags.
func getMetricDataWithCustomTags(orgId uint32, depth, count, interval int, prefix string, unique float32) []*schema.MetricData {
	if unique < 0.0 || unique > 1.0 {
		panic("getMetricDataWithCustomTags: unique must be a value between 0.0 and 1.0")
	}

	tags := []string{
		"secondkey=anothervalue",
		"thirdkey=onemorevalue",
		"region=west",
		"os=ubuntu",
		"anothertag=somelongervalue",
		"manymoreother=lotsoftagstointern",
		"afewmoretags=forgoodmeasure",
		"onetwothreefourfivesix=seveneightnineten",
		"lotsandlotsoftags=morefunforeveryone",
		"goodforpeoplewhojustusetags=forbasicallyeverything",
	}

	uniqueNumber := 0

	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)

	for i, s := range series {
		data[i] = &schema.MetricData{
			Name:     s,
			OrgId:    int(orgId),
			Interval: interval,
		}
		data[i].Tags = make([]string, 10)
		var j int
		for j = 0; j < int(unique*10); j++ {
			data[i].Tags[j] = fmt.Sprintf("unique_series_id%d=%d", uniqueNumber, uniqueNumber)
			uniqueNumber++
		}
		for j < 10 {
			data[i].Tags[j] = tags[j]
			j++
		}
		data[i].SetId()
	}

	return data
}

func printMemUsage(t *testing.T) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	t.Logf("HeapAlloc = \t\t%v MB\t\t %v KB\t %v B\n", (m.HeapAlloc / 1024 / 1024), (m.HeapAlloc / 1024), m.HeapAlloc)
	t.Logf("TotalAlloc = \t\t%v MB\t\t %v KB\t %v B\n", (m.TotalAlloc / 1024 / 1024), (m.TotalAlloc / 1024), m.TotalAlloc)
	t.Logf("Sys = \t\t\t%v MB\t\t %v KB\t %v B\n", (m.Sys / 1024 / 1024), (m.Sys / 1024), m.Sys)
	t.Logf("Live Heap Objects = \t%v\n", (m.Mallocs - m.Frees))
	t.Logf("NumGC = \t\t%v\n\n\n", m.NumGC)
}

// withAndWithoutTagSupport calls a test with the TagSupprt setting
// turned on and off. This is to verify that something works as expected
// no matter what this flag is set to, it does not mean that the behavior
// of a method should be changing dependent on that setting.
func withAndWithoutTagSupport(f func(*testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		_tagSupport := TagSupport
		defer func() { TagSupport = _tagSupport }()

		TagSupport = true
		t.Run("withTagSupport", f)
		TagSupport = false
		t.Run("withoutTagSupport", f)
	}
}

// withAndWithoutPartitionedIndex calls a test with the Partitioned setting
// turned on and off. This is to verify that something works as expected
// no for both the partitioned and non-partitioned index versions.
func withAndWithoutPartitionedIndex(f func(*testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		_partitioned := Partitioned
		defer func() { Partitioned = _partitioned }()

		Partitioned = false
		t.Run("unPartitioned", f)
		Partitioned = true
		t.Run("partitioned", f)
	}
}

// withAndWithoutMetaTagSupport calls a test with the MetaTagSupport
// setting turned on and off. This is to verify that something works as
// expected with both settings.
func withAndWithoutMetaTagSupport(f func(*testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		reset := enableMetaTagSupport()
		defer reset()

		t.Run("withMetaTagSupport", f)
		disableMetaTagSupport()
		t.Run("withoutMetaTagSupport", f)
	}
}

func TestGetAddKey(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutMetaTagSupport(withAndWithoutTagSupport(testGetAddKey)))(t)
}

func testGetAddKey(t *testing.T) {
	idx.OrgIdPublic = 100
	defer func() { idx.OrgIdPublic = 0 }()

	ix := New()
	ix.Init()
	defer ix.Stop()

	publicSeries := getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.public", false)
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1", false)
	org2Series := getMetricData(2, 2, 5, 10, "metric.org2", false)

	for _, series := range [][]*schema.MetricData{publicSeries, org1Series, org2Series} {
		orgId := uint32(series[0].OrgId)
		Convey(fmt.Sprintf("When indexing metrics for orgId %d", orgId), t, func() {
			for _, s := range series {
				mkey, _ := schema.MKeyFromString(s.Id)
				ix.AddOrUpdate(mkey, s, getPartition(s))
			}
			Convey(fmt.Sprintf("Then listing metrics for OrgId %d", orgId), func() {
				defs := ix.List(orgId)
				numSeries := len(series)
				if orgId != idx.OrgIdPublic {
					numSeries += 5
				}
				So(defs, ShouldHaveLength, numSeries)

			})
		})
	}

	Convey("When adding metricDefs with the same series name as existing metricDefs", t, func() {
		for _, series := range org1Series {
			series.Interval = 60
			series.SetId()
			mkey, _ := schema.MKeyFromString(series.Id)
			ix.AddOrUpdate(mkey, series, getPartition(series))
		}
		Convey("then listing metrics", func() {
			defs := ix.List(1)
			So(defs, ShouldHaveLength, 15)
		})
	})

	if TagSupport {
		Convey("When adding metricDefs with the same series name as existing metricDefs (tagged)", t, func() {
			Convey("then findByTag", func() {
				query, err := tagquery.NewQueryFromStrings([]string{"name!="}, 0, 0)
				So(err, ShouldBeNil)
				nodes := ix.FindByTag(1, query)
				defs := make([]idx.Archive, 0, len(nodes))
				for i := range nodes {
					defs = append(defs, nodes[i].Defs...)
				}
				So(defs, ShouldHaveLength, 2*len(org1Series))
			})
		})
	}
}

func TestFind(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testFind))(t)
}

// note: currently no testing for partitionedIndex
// because due to uneven partitioning and rounding errors it gets fuzzy to test that
func TestFindLimit(t *testing.T) {

	_partitioned := Partitioned
	Partitioned = false
	defer func() { Partitioned = _partitioned }()

	ix := New()
	ix.Init()
	defer ix.Stop()

	for _, s := range getMetricData(65, 2, 5, 10, "metric.demo", false) {
		s.Time = 10 * 86400
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("adding", s)
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	Convey("When searching with met limit", t, func() {
		nodes, err := ix.Find(65, "metric.demo.*.*", 0, 6)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
	})
	Convey("When searching with a barely met limit", t, func() {
		nodes, err := ix.Find(65, "metric.demo.*.*", 0, 5)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
	})
	/*
		Convey("When searching with a breached limit", t, func() {
			nodes, err := ix.Find(65, "metric.demo.*.*", 0, 4)
			So(err, ShouldNotBeNil)
			So(nodes, ShouldHaveLength, 0)
		})
		Convey("When searching with a breached limit", t, func() {
			nodes, err := ix.Find(65, "metric.demo.*.*", 0, 3)
			So(err, ShouldNotBeNil)
			So(nodes, ShouldHaveLength, 0)
		})
	*/
}
func testFind(t *testing.T) {
	idx.OrgIdPublic = 100
	defer func() { idx.OrgIdPublic = 0 }()

	ix := New()
	ix.Init()
	defer ix.Stop()

	for _, s := range getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.demo", false) {
		s.Time = 10 * 86400
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo", false) {
		s.Time = 10 * 86400
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo", false) {
		s.Time = 1 * 86400
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
		s.Time = 2 * 86400
		s.Interval = 60
		s.SetId()
		mkey, err = schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo", false) {
		s.Time = 1 * 86400
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("When listing root nodes", t, func() {
		Convey("root nodes for orgId 1", func() {
			nodes, err := ix.Find(1, "*", 0, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 2)
			So(nodes[0].Path, ShouldBeIn, "metric", "foo")
			So(nodes[1].Path, ShouldBeIn, "metric", "foo")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
		Convey("root nodes for orgId 2", func() {
			nodes, err := ix.Find(2, "*", 0, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 1)
			So(nodes[0].Path, ShouldEqual, "metric")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
	})

	Convey("When searching with GLOB", t, func() {
		nodes, err := ix.Find(2, "metric.{f*,demo}.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 10)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching with multiple wildcards", t, func() {
		nodes, err := ix.Find(1, "*.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 2)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching nodes not in public series", t, func() {
		nodes, err := ix.Find(1, "foo.demo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
		Convey("When searching for specific series", func() {
			found, err := ix.Find(1, nodes[0].Path, 0, 0)
			So(err, ShouldBeNil)
			So(found, ShouldHaveLength, 1)
			So(found[0].Path, ShouldEqual, nodes[0].Path)
		})
		Convey("When searching nodes that are children of a leaf", func() {
			found, err := ix.Find(1, nodes[0].Path+".*", 0, 0)
			So(err, ShouldBeNil)
			So(found, ShouldHaveLength, 0)
		})
	})

	Convey("When searching with multiple wildcards mixed leaf/branch", t, func() {
		nodes, err := ix.Find(1, "*.demo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 15)
		for _, n := range nodes {
			if strings.HasPrefix(n.Path, "foo.demo") {
				So(n.Leaf, ShouldBeTrue)
				So(n.Defs, ShouldHaveLength, 2)
			} else {
				So(n.Leaf, ShouldBeFalse)
			}
		}
	})
	Convey("When searching nodes for unknown orgId", t, func() {
		nodes, err := ix.Find(4, "foo.demo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})

	Convey("When searching nodes that don't exist", t, func() {
		nodes, err := ix.Find(1, "foo.demo.blah.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})

	Convey("When searching with from timestamp", t, func() {
		nodes, err := ix.Find(1, "*.demo.*", 4*86400, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 10)
		for _, n := range nodes {
			So(n.Path, ShouldNotContainSubstring, "foo.demo")
		}
		Convey("When searching with from timestamp on series with multiple defs.", func() {
			nodes, err := ix.Find(1, "*.demo.*", 2*86400, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 15)
			for _, n := range nodes {
				if strings.HasPrefix(n.Path, "foo.demo") {
					So(n.Defs, ShouldHaveLength, 1)
				}
			}
		})
	})

}

func TestDelete(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testDelete))(t)
}

func testDelete(t *testing.T) {
	Convey("given series for multiple orgs", t, func() {
		idx.OrgIdPublic = 100
		defer func() { idx.OrgIdPublic = 0 }()

		ix := New()
		ix.Init()
		defer ix.Stop()

		publicSeries := getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.public", false)
		org1Series := getMetricData(1, 2, 5, 10, "metric.org1", false)

		for _, s := range publicSeries {
			mkey, err := schema.MKeyFromString(s.Id)
			if err != nil {
				t.Fatal(err)
			}
			ix.AddOrUpdate(mkey, s, getPartition(s))
		}
		for _, s := range org1Series {
			mkey, err := schema.MKeyFromString(s.Id)
			if err != nil {
				t.Fatal(err)
			}
			ix.AddOrUpdate(mkey, s, getPartition(s))
		}

		Convey("when a pattern is deleted from an org", func() {
			archives, err := ix.Delete(idx.OrgIdPublic, "metric.*")

			Convey("the deleted archives should be returned", func() {
				So(err, ShouldBeNil)
				So(archives, ShouldHaveLength, 5)
				So(archives[0].Name, ShouldStartWith, "metric.public")
			})

			Convey("the deleted metrics should not be in the index", func() {
				nodes, err := ix.Find(idx.OrgIdPublic, "metric.*", 0, 0)
				So(err, ShouldBeNil)
				So(nodes, ShouldHaveLength, 0)
			})

			Convey("metrics from different orgs should not be impacted", func() {
				nodes, err := ix.Find(1, "metric.*", 0, 0)
				So(err, ShouldBeNil)
				So(nodes, ShouldHaveLength, 1)
			})
		})
	})
}

func TestDeleteTagged(t *testing.T) {
	withAndWithoutPartitionedIndex(testDeleteTagged)(t)
}

func testDeleteTagged(t *testing.T) {
	idx.OrgIdPublic = 100
	defer func() { idx.OrgIdPublic = 0 }()

	ix := New()
	ix.Init()
	defer ix.Stop()

	publicSeries := getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.public", true)
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1", true)

	for _, s := range publicSeries {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	for _, s := range org1Series {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("when deleting by tag", t, func() {
		testName := schema.MetricDefinitionFromMetricData(org1Series[3]).NameWithTags()
		tags, err := tagquery.ParseTagsFromMetricName(testName)
		So(err, ShouldBeNil)
		query, err := tagquery.NewQueryFromStrings(tags.Strings(), 0, 0)
		So(err, ShouldBeNil)
		ids, err := ix.DeleteTagged(1, query)
		So(err, ShouldBeNil)
		So(ids, ShouldHaveLength, 1)
		So(ids[0].Id.String(), ShouldEqual, org1Series[3].Id)
		Convey("series should not be present in the metricDef index", func() {
			query, err := tagquery.NewQueryFromStrings([]string{"series_id=3"}, 0, 0)
			So(err, ShouldBeNil)
			nodes := ix.FindByTag(1, query)
			So(nodes, ShouldHaveLength, 0)
			Convey("but others should still be present", func() {
				query, err := tagquery.NewQueryFromStrings([]string{"series_id=~[0-9]"}, 0, 0)
				So(err, ShouldBeNil)
				nodes := ix.FindByTag(1, query)
				So(err, ShouldBeNil)
				So(nodes, ShouldHaveLength, 4)
			})
		})
	})
}

func TestDeleteNodeWith100kChildren(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testDeleteNodeWith100kChildren))(t)
}

func testDeleteNodeWith100kChildren(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	var data *schema.MetricData
	var key string
	for i := 1; i <= 100000; i++ {
		key = fmt.Sprintf("some.metric.%d.%d", i, i)
		data = &schema.MetricData{
			Name:     key,
			OrgId:    1,
			Interval: 10,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}

	Convey("when deleting 100k series", t, func() {
		type resp struct {
			defs []idx.Archive
			err  error
		}
		done := make(chan *resp)
		go func() {
			defs, err := ix.Delete(1, "some.*")
			done <- &resp{
				defs: defs,
				err:  err,
			}
		}()

		select {
		case <-time.After(time.Second * 10):
			t.Fatal("deleting series took more then 10seconds.")
		case response := <-done:
			So(response.err, ShouldBeNil)
			So(response.defs, ShouldHaveLength, 100000)
		}
	})
}

func TestMixedBranchLeaf(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testMixedBranchLeaf))(t)
}

func testMixedBranchLeaf(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	first := &schema.MetricData{
		Name:     "foo.bar",
		OrgId:    1,
		Interval: 10,
	}
	second := &schema.MetricData{
		Name:     "foo.bar.baz",
		OrgId:    1,
		Interval: 10,
	}
	third := &schema.MetricData{
		Name:     "foo",
		OrgId:    1,
		Interval: 10,
	}
	first.SetId()
	second.SetId()
	third.SetId()

	Convey("when adding the first metric", t, func() {
		mkey, err := schema.MKeyFromString(first.Id)
		if err != nil {
			t.Fatal(err)
		}

		ix.AddOrUpdate(mkey, first, getPartition(first))
		Convey("we should be able to add a leaf under another leaf", func() {
			mkey, err := schema.MKeyFromString(second.Id)
			if err != nil {
				t.Fatal(err)
			}

			ix.AddOrUpdate(mkey, second, getPartition(second))
			_, ok := ix.Get(mkey)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 2)
		})
		Convey("we should be able to add a leaf that collides with an existing branch", func() {
			mkey, err := schema.MKeyFromString(third.Id)
			if err != nil {
				t.Fatal(err)
			}

			ix.AddOrUpdate(mkey, third, getPartition(third))
			_, ok := ix.Get(mkey)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 3)
		})
	})
}

func TestMixedBranchLeafDelete(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testMixedBranchLeafDelete))(t)
}

func testMixedBranchLeafDelete(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	series := []*schema.MetricData{
		{
			Name:     "a.b.c",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c.d",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2.d.e",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2.d2.e",
			OrgId:    1,
			Interval: 10,
		},
	}
	var mkeys []schema.MKey
	for _, s := range series {
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		mkeys = append(mkeys, mkey)
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("when deleting mixed leaf/branch", t, func() {
		defs, err := ix.Delete(1, "a.b.c")
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 2)
		deletedIds := make([]schema.MKey, len(defs))
		for i, d := range defs {
			deletedIds[i] = d.Id
		}
		So(test.MustMKeyFromString(series[0].Id), test.ShouldContainMKey, deletedIds)
		So(test.MustMKeyFromString(series[1].Id), test.ShouldContainMKey, deletedIds)
		Convey("series should not be present in the metricDef index", func() {
			_, ok := ix.Get(mkeys[0])
			So(ok, ShouldEqual, false)
			Convey("series should not be present in searches", func() {
				found, err := ix.Find(1, "a.b.c", 0, 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
				found, err = ix.Find(1, "a.b.c.d", 0, 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
			})
		})
	})
	Convey("when deleting from branch", t, func() {
		defs, err := ix.Delete(1, "a.b.c2.d.*")
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 1)
		if defs[0].Id != mkeys[3] {
			t.Fatalf("%v must equal %v", defs[0].Id, mkeys[3])
		}

		Convey("deleted series should not be present in the metricDef index", func() {
			_, ok := ix.Get(mkeys[3])
			So(ok, ShouldEqual, false)
			Convey("deleted series should not be present in searches", func() {
				found, err := ix.Find(1, "a.b.c2.*", 0, 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 1)
				found, err = ix.Find(1, "a.b.c2.d", 0, 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
			})
		})
	})
}

func TestPruneTaggedSeries(t *testing.T) {
	withAndWithoutPartitionedIndex(testPruneTaggedSeries)(t)
}

func testPruneTaggedSeries(t *testing.T) {

	IndexRules = conf.IndexRules{
		Rules: []conf.IndexRule{
			{
				Name:     "longterm",
				Pattern:  regexp.MustCompile("^long"),
				MaxStale: time.Minute,
			},
		},
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}
	ix := New()
	ix.Init()
	defer ix.Stop()

	// add old series
	series := getMetricData(1, 2, 5, 10, "longterm.old", true)
	for _, s := range series {
		s.Time = 1
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	series = getMetricData(1, 2, 5, 10, "longterm.more-recent", true)
	for _, s := range series {
		s.Time = 50
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	// data that will never expire due to the default
	series = getMetricData(1, 2, 5, 10, "metric.never.expire", true)
	for _, s := range series {
		s.Time = 1
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 15)
	})

	Convey("When pruning old series", t, func() {
		pruned, err := ix.Prune(time.Unix(100, 0)) // old series should be gone
		So(err, ShouldBeNil)
		So(pruned, ShouldHaveLength, 5)
		query, err := tagquery.NewQueryFromStrings([]string{"name=~longterm\\.old.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes := ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 0)
		query, err = tagquery.NewQueryFromStrings([]string{"name=~longterm.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes = ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 5)
		query, err = tagquery.NewQueryFromStrings([]string{"name=~metric\\.never\\.exp.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes = ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 5)
	})

	Convey("after pruning again but more aggressively", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 10)
		// find one of the longterm ones and update it
		// to a more recent time that will survive the next prune
		var data *schema.MetricData
		for _, def := range defs {
			if strings.HasPrefix(def.Name, "longterm") {
				data = &schema.MetricData{
					Name:     def.Name,
					Id:       def.Id.String(),
					Tags:     def.Tags,
					Mtype:    def.Mtype,
					OrgId:    1,
					Interval: 10,
					Time:     100,
				}
				data.SetId()
				break
			}
		}
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
		Convey("When pruning old series", func() {
			pruned, err := ix.Prune(time.Unix(120, 0))
			So(err, ShouldBeNil)
			So(pruned, ShouldHaveLength, 4)
			query, err := tagquery.NewQueryFromStrings([]string{"name=~longterm", "series_id=~[0-4]"}, 0, 0)
			So(err, ShouldBeNil)
			nodes := ix.FindByTag(1, query)
			So(nodes, ShouldHaveLength, 1)
			query, err = tagquery.NewQueryFromStrings([]string{"name=~metric\\.never.*", "series_id=~[0-4]"}, 0, 0)
			So(err, ShouldBeNil)
			nodes = ix.FindByTag(1, query)
			So(nodes, ShouldHaveLength, 5)
		})
	})
}

// this function just tests the collision aspect.
// it does not test matching over different rules or tag matching
// we have other tests for that
func TestPruneTaggedSeriesWithCollidingTagSets(t *testing.T) {
	withAndWithoutPartitionedIndex(testPruneTaggedSeriesWithCollidingTagSets)(t)
}

func testPruneTaggedSeriesWithCollidingTagSets(t *testing.T) {
	_tagSupport := TagSupport
	defer func() { TagSupport = _tagSupport }()
	TagSupport = true

	IndexRules = conf.IndexRules{
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: time.Second,
		},
	}

	ix := New()
	ix.Init()
	defer ix.Stop()

	series := getMetricData(1, 2, 1, 10, "metric.bah", true)
	serie2 := *series[0]
	series = append(series, &serie2)
	series[0].Interval = 1
	series[1].Interval = 2
	series[0].Time = 1
	series[1].Time = 10
	series[0].SetId()
	series[1].SetId()

	for _, s := range series {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 2)
	})

	findExpressions := []string{"name=" + series[1].Name}
	for _, tag := range series[1].Tags {
		findExpressions = append(findExpressions, tag)
	}

	Convey("When pruning old series", t, func() {
		pruned, err := ix.Prune(time.Unix(11, 0)) // time=1 is too old, time=10 is recent enough and causes first one to also stick around
		So(err, ShouldBeNil)
		So(pruned, ShouldHaveLength, 0)
	})

	Convey("After pruning", t, func() {
		query, err := tagquery.NewQueryFromStrings(findExpressions, 0, 0)
		So(err, ShouldBeNil)
		nodes := ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 1)
		defs := make([]idx.Archive, 0, len(nodes))
		for i := range nodes {
			defs = append(defs, nodes[i].Defs...)
		}
		So(defs, ShouldHaveLength, 2)
	})

	Convey("When pruning newer series", t, func() {
		pruned, err := ix.Prune(time.Unix(20, 0))
		So(err, ShouldBeNil)
		So(pruned, ShouldHaveLength, 2)
	})

	Convey("After pruning", t, func() {
		query, err := tagquery.NewQueryFromStrings(findExpressions, 0, 0)
		So(err, ShouldBeNil)
		nodes := ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 0)
	})
}

func TestPrune(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testPrune))(t)
}

func testPrune(t *testing.T) {
	IndexRules = conf.IndexRules{
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: time.Second,
		},
	}

	ix := New()
	ix.Init()
	defer ix.Stop()

	// add old series
	for _, s := range getSeriesNames(2, 5, "metric.bah") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     1,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}
	//new series
	for _, s := range getSeriesNames(2, 5, "metric.foo") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     10,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}
	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 10)
	})
	Convey("When pruning old series", t, func() {
		pruned, err := ix.Prune(time.Unix(11, 0))
		So(err, ShouldBeNil)
		So(pruned, ShouldHaveLength, 5)
		nodes, err := ix.Find(1, "metric.bah.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
		nodes, err = ix.Find(1, "metric.foo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)

	})
	Convey("after pruning", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 5)
		data := &schema.MetricData{
			Name:     defs[0].Name,
			Id:       defs[0].Id.String(),
			OrgId:    1,
			Interval: 30,
			Time:     100,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
		Convey("When pruning old series", func() {
			pruned, err := ix.Prune(time.Unix(12, 0))
			So(err, ShouldBeNil)
			So(pruned, ShouldHaveLength, 4)
			ix.ForceInvalidationFindCache()
			nodes, err := ix.Find(1, "metric.foo.*", 0, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 1)
		})
	})
}

func TestBasicFind(t *testing.T) {
	withAndWithoutPartitionedIndex(testBasicFind)(t)
}

func testBasicFind(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	// add old series
	for _, s := range getSeriesNames(2, 5, "metric.bah") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     1,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}
	//new series
	for _, s := range getSeriesNames(2, 5, "metric.foo") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     10,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}
	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 10)
	})
	Convey("should have no duplicates", t, func() {
		nodes, err := ix.Find(1, "metric.bah.*.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)

		names := make(map[string]struct{})
		for _, n := range nodes {
			for _, def := range n.Defs {
				names[def.Name] = struct{}{}
			}
		}

		So(names, ShouldHaveLength, len(nodes))
	})
}

func TestControlFuncs(t *testing.T) {
	withAndWithoutPartitionedIndex(withAndWithoutTagSupport(testControlFuncs))(t)
}

func testControlFuncs(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	// add bah series
	for _, s := range getSeriesNames(1, 5, "metric.bah") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     1,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}

	// foo series
	for _, s := range getSeriesNames(2, 5, "metric.foo") {
		d := &schema.MetricData{
			Name:     s,
			OrgId:    1,
			Interval: 10,
			Time:     10,
		}
		d.SetId()
		mkey, err := schema.MKeyFromString(d.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, d, getPartition(d))
	}

	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 10)
	})

	bahdefs := make([]schema.MetricDefinition, 0, 5)
	Convey("can delete some series", t, func() {
		nodes, err := ix.Find(1, "metric.bah.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)

		for _, node := range nodes {
			for _, def := range node.Defs {
				bahdefs = append(bahdefs, def.MetricDefinition)
			}
		}

		ix.DeleteDefs(bahdefs, true)
		ix.ForceInvalidationFindCache()

		nodes, err = ix.Find(1, "metric.bah.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
		nodes, err = ix.Find(1, "metric.foo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)

	})

	Convey("can add them back", t, func() {
		ix.AddDefs(bahdefs)
		// AddDefs invalidates the cache asynchronously, so add short pause
		time.Sleep(50 * time.Millisecond)
		ix.ForceInvalidationFindCache()

		nodes, err := ix.Find(1, "metric.bah.*", 0, 0)
		if len(nodes) != 5 {
			nodes, err = ix.Find(1, "metric.bah.*", 0, 0)
		}
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
		nodes, err = ix.Find(1, "metric.foo.*", 0, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
	})
}

func TestControlFuncsTagged(t *testing.T) {
	withAndWithoutPartitionedIndex(testControlFuncsTagged)(t)
}

func testControlFuncsTagged(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	// add old series
	series := getMetricData(1, 2, 5, 10, "longterm.old", true)
	for _, s := range series {
		s.Time = 1
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}
	series = getMetricData(1, 2, 5, 10, "longterm.more-recent", true)
	for _, s := range series {
		s.Time = 50
		s.SetId()
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, getPartition(s))
	}

	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 10)
	})

	deldefs := make([]schema.MetricDefinition, 0, 5)

	Convey("Can delete some series", t, func() {
		query, err := tagquery.NewQueryFromStrings([]string{"name=~longterm\\.old.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes := ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 5)

		for _, node := range nodes {
			for _, def := range node.Defs {
				deldefs = append(deldefs, def.MetricDefinition)
			}
		}

		ix.DeleteDefs(deldefs, true)

		query, err = tagquery.NewQueryFromStrings([]string{"name=~longterm\\.old.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes = ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 0)
		query, err = tagquery.NewQueryFromStrings([]string{"name=~longterm.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes = ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 5)
	})

	Convey("can add them back", t, func() {
		ix.AddDefs(deldefs)

		query, err := tagquery.NewQueryFromStrings([]string{"name=~longterm\\.old.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes := ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 5)
		query, err = tagquery.NewQueryFromStrings([]string{"name=~longterm.*", "series_id=~[0-4]"}, 0, 0)
		So(err, ShouldBeNil)
		nodes = ix.FindByTag(1, query)
		So(nodes, ShouldHaveLength, 10)
	})
}

func TestSingleNodeMetric(t *testing.T) {
	withAndWithoutPartitionedIndex(testSingleNodeMetric)(t)
}

func testSingleNodeMetric(t *testing.T) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	data := &schema.MetricData{
		Name:     "node1",
		Interval: 10,
		OrgId:    1,
	}
	data.SetId()
	mkey, err := schema.MKeyFromString(data.Id)
	if err != nil {
		t.Fatal(err)
	}
	ix.AddOrUpdate(mkey, data, getPartition(data))
}

func TestUpsertingMetaRecordsIntoIndex(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	ix := NewUnpartitionedMemoryIdx()

	record1 := parseMetaTagRecordMustCompile(t, []string{"a=b", "c=d"}, []string{"name=~a.+", "__tag^=a"})
	record2 := parseMetaTagRecordMustCompile(t, []string{"e=f", "g=h"}, []string{"other=queries", "some!=value"})
	record3 := parseMetaTagRecordMustCompile(t, []string{"i=j", "k=l"}, []string{"other=queries", "some!=value"})

	err := ix.MetaTagRecordUpsert(1, record1)
	if err != nil {
		t.Fatalf("Unexpected error when upserting meta tag record: %q", err)
	}

	err = ix.MetaTagRecordUpsert(1, record2)
	if err != nil {
		t.Fatalf("Unexpected error when upserting meta tag record: %q", err)
	}

	waitForMetaTagEnrichers(t, ix)

	metaTagRecords := ix.MetaTagRecordList(1)
	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected MetaTagRecordList to return 2 records for org 1, but it has:\n%+v\n", metaTagRecords)
	}

	var found1, found2 bool
	for _, mtr := range metaTagRecords {
		if mtr.Equals(&record1) {
			found1 = true
		}
		if mtr.Equals(&record2) {
			found2 = true
		}
	}
	if !(found1 && found2) {
		t.Fatalf("Expected MetaTagRecordList to return both records, but it has not: %t/%t", found1, found2)
	}

	metaTagRecords = ix.MetaTagRecordList(2)
	if len(metaTagRecords) != 0 {
		t.Fatalf("Expected MetaTagRecordList to return no records for org 2, but it has:\n%+v\n", metaTagRecords)
	}

	compareRecordCount := func(expression string, expectedRecordCount int) {
		recordCount := len(ix.metaTagIdx.getOrgMetaTagIndex(1).hierarchy.getByTagValue(parseExpressionsMustCompile(t, []string{expression})[0], false))
		if recordCount != expectedRecordCount {
			t.Fatalf("Expected that there are %d record(s) associated with expression %s, but there were %d", expectedRecordCount, expression, recordCount)
		}
	}

	compareRecordCount("a=b", 1)
	compareRecordCount("c=d", 1)
	compareRecordCount("e=f", 1)
	compareRecordCount("g=h", 1)

	// record3 has the same queries as record2, so it should completely replace it
	err = ix.MetaTagRecordUpsert(1, record3)
	if err != nil {
		t.Fatalf("Unexpected error when upserting meta tag record: %q", err)
	}

	waitForMetaTagEnrichers(t, ix)

	metaTagRecords = ix.MetaTagRecordList(1)
	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected MetaTagRecordList to return 2 records for org 1, but it has:\n%+v\n", metaTagRecords)
	}

	compareRecordCount("a=b", 1)
	compareRecordCount("c=d", 1)
	compareRecordCount("e=f", 0)
	compareRecordCount("g=h", 0)
	compareRecordCount("i=j", 1)
	compareRecordCount("k=l", 1)
}

func TestMetricNameStartingWithTilde(t *testing.T) {
	withAndWithoutPartitionedIndex(testMetricNameStartingWithTilde)(t)
}

func testMetricNameStartingWithTilde(t *testing.T) {
	ix := New()
	ix.Init()
	defer func() {
		ix.Stop()
		ix = nil
	}()

	metricName := "~~~weird~.~metric~"
	expectedNameTag := "weird~.~metric~"

	data := &schema.MetricData{
		Name:     metricName,
		Interval: 1,
		OrgId:    1,
	}
	data.SetId()
	mkey, err := schema.MKeyFromString(data.Id)
	if err != nil {
		t.Fatal(err)
	}

	arch, _, _ := ix.AddOrUpdate(mkey, data, getPartition(data))
	if arch.Name != metricName {
		t.Fatalf("Expected metric name to be %q, but it was %q", metricName, arch.Name)
	}

	// query by the name minus the leading ~ characters
	query, err := tagquery.NewQueryFromStrings([]string{"name=" + expectedNameTag}, 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error when parsing query expression: %q", err)
	}

	findResult := ix.FindByTag(1, query)
	if len(findResult) != 1 {
		t.Fatalf("Expected 1 result, but got %d", len(findResult))
	}

	if findResult[0].Path != metricName {
		t.Fatalf("Expected metric name to be %q, but it was %q", metricName, findResult[0].Path)
	}

	tagDetails := ix.TagDetails(1, "name", nil)
	if len(tagDetails) != 1 {
		t.Fatalf("Expected 1 result, but got %d", len(tagDetails))
	}

	count, ok := tagDetails[expectedNameTag]
	if !ok {
		t.Fatalf("Did not get expected name as tag value in result")
	}
	if count != 1 {
		t.Fatalf("Expected 1 result, but got %d", count)
	}
}

// TestMemoryIndexHeapUsageWithTagsUniqueNone5 gives a rough estimate of how much memory the index is using with tag support turned on.
// It uses 0 unique tags and 10 identical tags
func TestMemoryIndexHeapUsageWithTagsUniqueNone5(t *testing.T) {
	testMemoryIndexHeapUsageWithTags(t, 0.0, 5)
}

// TestMemoryIndexHeapUsageWithTagsUnique5 gives a rough estimate of how much memory the index is using with tag support turned on.
// It uses 5 unique tags and 5 identical tags
func TestMemoryIndexHeapUsageWithTagsUniqueHalf5(t *testing.T) {
	testMemoryIndexHeapUsageWithTags(t, 0.5, 5)
}

// TestMemoryIndexHeapUsageQithTagsUniqueAll5 gives a rough estimate of how much memory the index is using with tag support turned on.
// It uses 10 unique tags and 0 identical tags
func TestMemoryIndexHeapUsageWithTagsUniqueAll5(t *testing.T) {
	testMemoryIndexHeapUsageWithTags(t, 1.0, 5)
}

var globalMemoryIndex MemoryIndex

func testMemoryIndexHeapUsageWithTags(t *testing.T, unique float32, count int) {
	// turn partitioning off
	Partitioned = false

	// turn tag support on
	TagSupport = true

	globalMemoryIndex = New()
	globalMemoryIndex.Init()
	defer globalMemoryIndex.Stop()

	series := getMetricDataWithCustomTags(1, 2, count, 10, "somekindof.longereven.metricname.butinstead.ofashorterone.bunchofthingsandstuff", unique)

	t.Log("Initial memory stats after series has been allocated")
	printMemUsage(t)

	for _, data := range series {
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			t.Fatal(err)
		}
		globalMemoryIndex.AddOrUpdate(mkey, data, 0)
	}

	t.Log("Memory stats after AddOrUpdate called")
	printMemUsage(t)

	runtime.GC()

	t.Log("Memory stats after runtime.GC() called")
	printMemUsage(t)
}

// withAndWithoutPartitonedIndex calls a bench with the Partitioned setting
// turned on and off. This is to verify that something works as expected
// for both the partitioned and non-partitioned index versions.
func benchWithAndWithoutPartitonedIndex(f func(*testing.B)) func(*testing.B) {
	return func(b *testing.B) {
		b.Helper()
		_partitioned := Partitioned
		defer func() { Partitioned = _partitioned }()

		Partitioned = true
		b.Run("partitioned", f)
		Partitioned = false
		b.Run("unPartitioned", f)
	}
}

func benchWithAndWithoutMetaTagSupport(f func(*testing.B)) func(*testing.B) {
	return func(b *testing.B) {
		b.Helper()
		reset := enableMetaTagSupport()
		defer reset()

		b.Run("withMetaTagSupport", f)
		disableMetaTagSupport()
		b.Run("withoutMetaTagSupport", f)
	}
}

func BenchmarkIndexing(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkIndexing)(b)
}

func benchmarkIndexing(b *testing.B) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	var series string
	var data *schema.MetricData
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		series = "some.metric." + strconv.Itoa(n)
		data = &schema.MetricData{
			Name:     series,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			b.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
}

func BenchmarkDeletes(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkDeletes)(b)
}

func benchmarkDeletes(b *testing.B) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	var data *schema.MetricData
	var key string
	for i := 1; i <= b.N; i++ {
		key = fmt.Sprintf("some.metric.%d.%d", i, i)
		data = &schema.MetricData{
			Name:     key,
			OrgId:    1,
			Interval: 10,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			b.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	b.ReportAllocs()
	b.ResetTimer()

	ix.Delete(1, "some.*")
}

func BenchmarkPrune(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkPrune)(b)
}

func benchmarkPrune(b *testing.B) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	var data *schema.MetricData
	var key string
	for i := 1; i <= b.N; i++ {
		key = fmt.Sprintf("some.metric.%d.%d", i, i)
		data = &schema.MetricData{
			Name:     key,
			OrgId:    1,
			Interval: 10,
			Time:     100,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			b.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	b.ReportAllocs()
	b.ResetTimer()

	items, err := ix.Prune(time.Unix(200, 0))
	if err != nil {
		b.Fatal(err)
	}
	if len(items) != b.N {
		b.Fatalf("only %d of %d items pruned", len(items), b.N)
	}
}

func BenchmarkPruneLongSeriesNames(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkPruneLongSeriesNames)(b)
}

func benchmarkPruneLongSeriesNames(b *testing.B) {
	ix := New()
	ix.Init()
	defer ix.Stop()

	var data *schema.MetricData
	var key string
	for i := 1; i <= b.N; i++ {
		key = fmt.Sprintf("stats.%d.some.really.long.metric.that.is.slow.to.delete.and.really.hurts.pruning.performance", i)
		data = &schema.MetricData{
			Name:     key,
			OrgId:    1,
			Interval: 10,
			Time:     100,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			b.Fatal(err)
		}
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	b.ReportAllocs()
	b.ResetTimer()

	items, err := ix.Prune(time.Unix(200, 0))
	if err != nil {
		b.Fatal(err)
	}
	if len(items) != b.N {
		b.Fatalf("only %d of %d items pruned", len(items), b.N)
	}
}

func TestMatchSchemaWithTags(t *testing.T) {
	withAndWithoutPartitionedIndex(testMatchSchemaWithTags)(t)
}

func testMatchSchemaWithTags(t *testing.T) {
	_tagSupport := TagSupport
	_schemas := mdata.Schemas
	defer func() { TagSupport = _tagSupport }()
	defer func() { mdata.Schemas = _schemas }()

	TagSupport = true
	mdata.Schemas = conf.NewSchemas([]conf.Schema{
		{
			Name:       "tag1_is_value3_or_value5",
			Pattern:    regexp.MustCompile(".*;tag1=value[35](;.*|$)"),
			Retentions: conf.MustParseRetentions("1s:1d:10min:2:true"),
		},
	})

	ix := New()
	ix.Init()
	defer ix.Stop()

	data := make([]*schema.MetricDefinition, 10)
	archives := make([]*idx.Archive, 10)
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		data[i] = &schema.MetricDefinition{
			Name:      name,
			OrgId:     1,
			Interval:  1,
			Tags:      []string{fmt.Sprintf("tag1=value%d", i), "tag2=othervalue"},
			Partition: getPartitionFromName(name),
		}
		data[i].SetId()
		archives[i] = createArchive(data[i])
		ix.add(archives[i])
	}

	// only those MDs with tag1=value3 or tag1=value5 should get the first schema id
	expectedSchemas := []uint16{1, 1, 1, 0, 1, 0, 1, 1, 1, 1}
	for i := 0; i < 10; i++ {
		if archives[i].SchemaId != expectedSchemas[i] {
			t.Fatalf("Expected schema of archive %d to be %d, but it was %d", i, expectedSchemas[i], archives[i].SchemaId)
		}
	}
}

func BenchmarkAddingMetaRecordsToIdxWith100kMetrics(b *testing.B) {
	reset := enableMetaTagSupport()
	defer reset()

	metricCnt := 100000
	metaRecordCnt := b.N
	ix := NewUnpartitionedMemoryIdx()

	hasher := md5.New()
	generateMetricName := func(id int) string {
		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%d", id)))
		return string(hex.EncodeToString(hasher.Sum(nil)))
	}

	generateMetaRecordKeyTag := func(id int) string {
		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%d", id%metaRecordCnt)))
		sum := hex.EncodeToString(hasher.Sum(nil))
		return fmt.Sprintf("%s=%s", sum[:16], sum[16:32])
	}

	md := schema.MetricData{
		Interval: 1,
		Time:     123,
	}
	for i := 0; i < metricCnt; i++ {
		md.OrgId = 1
		md.Name = generateMetricName(i)

		// add some tags, their content isn't relevant
		md.Tags = []string{
			fmt.Sprintf("%s=%s", md.Name[0:3], md.Name[3:6]),
			fmt.Sprintf("%s=%s", md.Name[6:9], md.Name[9:12]),
		}

		// append the tag based on which we are going to assign meta records
		md.Tags = append(md.Tags, generateMetaRecordKeyTag(i))

		md.SetId()
		mkey, err := schema.MKeyFromString(md.Id)
		if err != nil {
			b.Fatalf("Unexpected error when parsing id (%s): %s", md.Id, err)
		}

		ix.AddOrUpdate(mkey, &md, 0)
	}

	metaRecords := make([]tagquery.MetaTagRecord, metaRecordCnt)
	for i := 0; i < metaRecordCnt; i++ {
		// append the tag by which we are going to assign meta records
		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%d", i)))
		sum := hex.EncodeToString(hasher.Sum(nil))
		var err error
		metaRecords[i].Expressions, err = tagquery.ParseExpressions([]string{generateMetaRecordKeyTag(i)})
		if err != nil {
			b.Fatalf("Unexpected error when parsing tag (%s): %s", fmt.Sprintf("%s=%s", sum[:8], sum[8:16]), err)
		}
		metaRecords[i].MetaTags = tagquery.Tags{
			{
				Key:   fmt.Sprintf("tag%d", i%10),
				Value: fmt.Sprintf("value%d", i%10),
			},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// add meta records to index
	err := ix.MetaTagRecordSwap(1, metaRecords)
	if err != nil {
		b.Fatalf("Error when swapping meta records: %s", err)
	}

	// delete & readd meta records
	for _, record := range metaRecords {
		err = ix.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
			Expressions: record.Expressions,
		})
		if err != nil {
			b.Fatalf("Error when upserting meta record: %s", err)
		}
		err = ix.MetaTagRecordUpsert(1, record)
		if err != nil {
			b.Fatalf("Error when upserting meta record: %s", err)
		}
	}

	// delete meta records from index
	err = ix.MetaTagRecordSwap(1, nil)
	if err != nil {
		b.Fatalf("Error when swapping meta records: %s", err)
	}

	waitForMetaTagEnrichers(b, ix)
}
