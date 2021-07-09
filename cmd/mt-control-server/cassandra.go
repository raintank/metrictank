package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cassandra"
)

// CliConfig is a cassandra IdxConfig. It is instantiated with default values which can then be changed.
var CliConfig = NewIdxConfig()

// IdxConfig stores configuration settings for a cassandra index
type IdxConfig struct {
	SSL                      bool
	Auth                     bool
	HostVerification         bool
	Keyspace                 string
	ArchiveTable             string
	Hosts                    string
	CaPath                   string
	Username                 string
	Password                 string
	Consistency              string
	Timeout                  time.Duration
	ConnectionCheckInterval  time.Duration
	ConnectionCheckTimeout   time.Duration
	NumConns                 int
	ProtoVer                 int
	DisableInitialHostLookup bool
}

// NewIdxConfig returns IdxConfig with default values set.
func NewIdxConfig() *IdxConfig {
	return &IdxConfig{
		Hosts:                    "localhost:9042",
		Keyspace:                 "metrictank",
		ArchiveTable:             "metric_idx_archive",
		Consistency:              "one",
		Timeout:                  time.Second,
		ConnectionCheckInterval:  time.Second * 5,
		ConnectionCheckTimeout:   time.Second * 30,
		NumConns:                 10,
		ProtoVer:                 4,
		DisableInitialHostLookup: false,
		SSL:                      false,
		CaPath:                   "/etc/metrictank/ca.pem",
		HostVerification:         true,
		Auth:                     false,
		Username:                 "cassandra",
		Password:                 "cassandra",
	}
}

func ConfigCassandra() {
	casIdx := flag.NewFlagSet("cassandra", flag.ExitOnError)

	casIdx.StringVar(&CliConfig.Hosts, "hosts", CliConfig.Hosts, "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&CliConfig.Keyspace, "keyspace", CliConfig.Keyspace, "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&CliConfig.ArchiveTable, "archive-table", CliConfig.ArchiveTable, "Cassandra table to archive metricDefinitions in.")
	casIdx.StringVar(&CliConfig.Consistency, "consistency", CliConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&CliConfig.Timeout, "timeout", CliConfig.Timeout, "cassandra request timeout")
	casIdx.DurationVar(&CliConfig.ConnectionCheckInterval, "connection-check-interval", CliConfig.ConnectionCheckInterval, "interval at which to perform a connection check to cassandra, set to 0 to disable.")
	casIdx.DurationVar(&CliConfig.ConnectionCheckTimeout, "connection-check-timeout", CliConfig.ConnectionCheckTimeout, "maximum total time to wait before considering a connection to cassandra invalid. This value should be higher than connection-check-interval.")
	casIdx.IntVar(&CliConfig.NumConns, "num-conns", CliConfig.NumConns, "number of concurrent connections to cassandra")
	casIdx.IntVar(&CliConfig.ProtoVer, "protocol-version", CliConfig.ProtoVer, "cql protocol version to use")
	casIdx.BoolVar(&CliConfig.DisableInitialHostLookup, "disable-initial-host-lookup", CliConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	casIdx.BoolVar(&CliConfig.SSL, "ssl", CliConfig.SSL, "enable SSL connection to cassandra")
	casIdx.StringVar(&CliConfig.CaPath, "ca-path", CliConfig.CaPath, "cassandra CA certficate path when using SSL")
	casIdx.BoolVar(&CliConfig.HostVerification, "host-verification", CliConfig.HostVerification, "host (hostname and server cert) verification when using SSL")

	casIdx.BoolVar(&CliConfig.Auth, "auth", CliConfig.Auth, "enable cassandra user authentication")
	casIdx.StringVar(&CliConfig.Username, "username", CliConfig.Username, "username for authentication")
	casIdx.StringVar(&CliConfig.Password, "password", CliConfig.Password, "password for authentication")

	globalconf.Register("cassandra", casIdx, flag.ExitOnError)
}

type Cassandra struct {
	Config  *IdxConfig
	cluster *gocql.ClusterConfig
	Session *cassandra.Session
}

func NewCassandra() (*Cassandra, error) {
	cfg := CliConfig

	cluster := gocql.NewCluster(strings.Split(cfg.Hosts, ",")...)
	cluster.Consistency = gocql.ParseConsistency(cfg.Consistency)
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cluster.Timeout
	cluster.NumConns = cfg.NumConns
	cluster.ProtoVersion = cfg.ProtoVer
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup
	cluster.Keyspace = cfg.Keyspace

	if cfg.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.CaPath,
			EnableHostVerification: cfg.HostVerification,
		}
	}
	if cfg.Auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	session, err := cassandra.NewSession(cluster, cfg.ConnectionCheckTimeout, cfg.ConnectionCheckInterval, cfg.Hosts, "cassandra")

	if err != nil {
		return nil, fmt.Errorf("cassandra-idx: failed to create cassandra session: %s", err)
	}

	return &Cassandra{
		Config:  cfg,
		cluster: cluster,
		Session: session,
	}, nil
}
