package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grafana/metrictank/schema"
)

var errTooSmall = errors.New("too small")
var errFmtBinWriteFailed = "binary write failed: %q"
var errFmtUnsupportedFormat = "unsupported format %d"

type MetricData struct {
	Id       int64
	Metrics  []*schema.MetricData
	Produced time.Time
	Format   Format
	Msg      []byte
}

// parses format and id (cheap), but doesn't decode metrics (expensive) just yet.
func (m *MetricData) InitFromMsg(msg []byte) error {
	if len(msg) < 9 {
		return errTooSmall
	}
	m.Msg = msg

	buf := bytes.NewReader(msg[1:9])
	binary.Read(buf, binary.BigEndian, &m.Id)
	m.Produced = time.Unix(0, m.Id)

	m.Format = Format(msg[0])
	if m.Format != FormatMetricDataArrayJson && m.Format != FormatMetricDataArrayMsgp {
		return fmt.Errorf(errFmtUnsupportedFormat, m.Format)
	}
	return nil
}

// sets m.Metrics to a []*schema.MetricData
// any subsequent call may however put different MetricData into our m.Metrics array
func (m *MetricData) DecodeMetricData() error {
	var err error
	switch m.Format {
	case FormatMetricDataArrayJson:
		err = json.Unmarshal(m.Msg[9:], &m.Metrics)
	case FormatMetricDataArrayMsgp:
		out := schema.MetricDataArray(m.Metrics)
		_, err = out.UnmarshalMsg(m.Msg[9:])
		m.Metrics = []*schema.MetricData(out)
	default:
		return fmt.Errorf("unrecognized format %d", m.Msg[0])
	}
	if err != nil {
		return fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", m.Format, err)
	}
	m.Msg = nil // no more need for the original input
	return nil
}

// CreateMsg is the legacy function to create messages. It's not very fast
func CreateMsg(metrics []*schema.MetricData, id int64, version Format) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint8(version))
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	err = binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	var msg []byte
	switch version {
	case FormatMetricDataArrayJson:
		msg, err = json.Marshal(metrics)
	case FormatMetricDataArrayMsgp:
		m := schema.MetricDataArray(metrics)
		msg, err = m.MarshalMsg(nil)
	default:
		return nil, fmt.Errorf(errFmtUnsupportedFormat, version)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal metrics payload: %s", err)
	}
	_, err = buf.Write(msg)
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	return buf.Bytes(), nil
}

// WritePointMsg is like CreateMsg, except optimized for MetricPoint and buffer re-use.
// caller must assure a cap-len diff of at least:
// 33B (for FormatMetricPoint)
// 29B (for FormatMetricPointWithoutOrg)
// no other formats supported.
func WritePointMsg(point schema.MetricPoint, buf []byte, version Format) (o []byte, err error) {
	b := buf[:1]
	switch version {
	case FormatMetricPoint:
		b[0] = byte(FormatMetricPoint)
		return point.Marshal32(b)
	case FormatMetricPointWithoutOrg:
		b[0] = byte(FormatMetricPointWithoutOrg)
		return point.MarshalWithoutOrg28(b)
	}
	return nil, fmt.Errorf(errFmtUnsupportedFormat, version)
}

func IsPointMsg(data []byte) (Format, bool) {
	l := len(data)
	if l == 0 {
		return 0, false
	}
	version := Format(data[0])
	if l == 29 && version == FormatMetricPointWithoutOrg {
		return FormatMetricPointWithoutOrg, true
	}
	if l == 33 && version == FormatMetricPoint {
		return FormatMetricPoint, true
	}
	return 0, false
}

func ReadPointMsg(data []byte, defaultOrg uint32) ([]byte, schema.MetricPoint, error) {
	var point schema.MetricPoint
	version := Format(data[0])
	if len(data) == 29 && version == FormatMetricPointWithoutOrg {
		o, err := point.UnmarshalWithoutOrg(data[1:])
		point.MKey.Org = defaultOrg
		return o, point, err
	}
	if len(data) == 33 && version == FormatMetricPoint {
		o, err := point.Unmarshal(data[1:])
		return o, point, err
	}
	return data, point, fmt.Errorf(errFmtUnsupportedFormat, version)
}

func IsIndexControlMsg(data []byte) bool {
	l := len(data)
	if l == 0 {
		return false
	}
	version := Format(data[0])
	return version == FormatIndexControlMessage
}

func ReadIndexControlMsg(data []byte) (schema.ControlMsg, error) {
	var control schema.ControlMsg
	_, err := control.UnmarshalMsg(data[1:])
	return control, err
}
