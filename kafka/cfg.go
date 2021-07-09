package kafka

import (
	"flag"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
	log "github.com/sirupsen/logrus"
)

type KafkaNet struct {
	tlsEnabled    bool
	tlsSkipVerify bool
	tlsClientCert string
	tlsClientKey  string
	saslEnabled   bool
	saslMechanism string
	saslUsername  string
	saslPassword  string
}

func (k *KafkaNet) Configure(config *sarama.Config) {
	if k.tlsEnabled {
		tlsConfig, err := tls.NewConfig(k.tlsClientCert, k.tlsClientKey)
		if err != nil {
			log.Fatalf("Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = k.tlsSkipVerify
	}

	if k.saslEnabled {
		if k.saslMechanism == "SCRAM-SHA-256" {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		} else if k.saslMechanism == "SCRAM-SHA-512" {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		} else if k.saslMechanism == "PLAINTEXT" {
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		} else {
			log.Fatalf("Failed to recognize saslMechanism: '%s'", k.saslMechanism)
		}
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.saslUsername
		config.Net.SASL.Password = k.saslPassword
	}
}

func ConfigNet(FlagSet *flag.FlagSet) *KafkaNet {
	kn := &KafkaNet{}
	FlagSet.BoolVar(&kn.tlsEnabled, "tls-enabled", false, "Whether to enable TLS")
	FlagSet.BoolVar(&kn.tlsSkipVerify, "tls-skip-verify", false, "Whether to skip TLS server cert verification")
	FlagSet.StringVar(&kn.tlsClientCert, "tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	FlagSet.StringVar(&kn.tlsClientKey, "tls-client-key", "", "Client key for client authentication (use with -tls-enabled and -tls-client-cert)")
	FlagSet.BoolVar(&kn.saslEnabled, "sasl-enabled", false, "Whether to enable SASL")
	FlagSet.StringVar(&kn.saslMechanism, "sasl-mechanism", "", "The SASL mechanism configuration (possible values: SCRAM-SHA-256, SCRAM-SHA-512, PLAINTEXT)")
	FlagSet.StringVar(&kn.saslUsername, "sasl-username", "", "Username for client authentication (use with -sasl-enabled and -sasl-password)")
	FlagSet.StringVar(&kn.saslPassword, "sasl-password", "", "Password for client authentication (use with -sasl-enabled and -sasl-user)")

	return kn
}
