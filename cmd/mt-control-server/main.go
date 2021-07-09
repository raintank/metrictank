package main

import (
	"fmt"
	"os"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	producer *Producer
	cass     *Cassandra
)

func main() {
	path := "" // SEAN TODO - load config file
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: configuration file error: %s", err)
		os.Exit(1)
	}

	ConfigServer()
	ConfigKafka()
	ConfigCassandra()
	ConfigHandlers()

	config.ParseAll()

	apiServer, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to start API. %s", err.Error())
	}

	producer = NewProducer()
	cass, err = NewCassandra()
	if err != nil {
		log.Fatalf("Failed to config cassandra. %s", err.Error())
	}

	apiServer.Run()
	// SEAN TODO - handle shutdown somehow
}
