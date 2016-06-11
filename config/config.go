package configuration

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/go-yaml/yaml"
)

const (
	// Kafka's usage Partition Schemes
	PARTIONER_HASH    = "hash"
	PARTIONER_RANDOM  = "random"
	PARTIONER_MANUAL  = "manual"
	COMPRESSION_NONE  = "compression_none"
	COMPRESSION_SNAPY = "compression_snappy"
	COMPRESSION_GZIP  = "compression_gzip"
)

// Configuration structure  of the Kafka Producer
type KafkaProducerConfig struct {
	BrokerList      string `yaml:"broker_list"`      // Kafka broker list of Kafka servers (comma separated)
	ClientId        string `yaml:"client_id"`        // The client's ID
	Topic           string `yaml:"topic"`            // Topic to listen
	Partitioner     string `yaml:"partitioner"`      // Partitioner mode 'random', 'hash' or 'manual'
	Partition       int    `yaml:"partition"`        // Partition to use (ony in 'manual' case)
	CompressionType string `yaml:"compression_type"` // Comppression type: 'compression_none', 'compression_snappy', 'compression_gzip'
	Verbose         bool   `yaml:"verbose"`          // Enable verbose mode on the Kafka API
}

// Configurattion structure of the Kafka consumer
type KafkaConsumerConfig struct {
	BrokerList string `yaml:"broker_list"` // Kafka broker list of Kafka servers (comma separated)
	Topic      string `yaml:"topic"`       // Topic to listen
	Partitions string `yaml:"partitions"`  // Partitions to listen: 'all' or comma separated numbers [0, #P-1]
	Offset     string `yaml:"offset"`      // Offset from which the consumer starts receiving messages
	BufferSize int    `yaml:"buffer_size"` // The consumer message buffer size
	Verbose    bool   `yaml:"verbose"`     // Enable verbose mode on the Kafka API
}

// Parses the producer YAML configuration file.
// Example"
//		broker_list: "localhost:9093,localhost:9095,localhost:9095"
//		topic: "kafka_topic"
//		partition: 0
//		offset: 1000
//		verbose: true
func ParseConfiguration(configFile string, config interface{}) (err error) {
	contents, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Print("Failed reading file...")
		return err
	}
	err = yaml.Unmarshal(contents, config)
	if err != nil {
		log.Print("Failed unmarshalling file...")
		return err
	}
	return err
}

// Validates producer YAML configuration file.
func ValidateConfiguration(configuration interface{}) error {
	var errors []string
	// Producer instance
	if config, ok := configuration.(KafkaProducerConfig); ok {
		if config.BrokerList == "" {
			errors = append(errors, "Broker list is undefined!")
		}
		if config.ClientId == "" {
			errors = append(errors, "ClientId is undefined!")
		}
		if config.Topic == "" {
			errors = append(errors, "Topic is undefined!")
		}
		if config.CompressionType == "" {
			errors = append(errors, "CompressionType is undefined!")
		}

		if config.Partitioner == PARTIONER_MANUAL && config.Partition == -1 {
			errors = append(errors, "Partition is required when partitioning manually")
		}
	}

	// Consumer instance
	if config, ok := configuration.(KafkaConsumerConfig); ok {
		if config.BrokerList == "" {
			errors = append(errors, "BrokerList is undefined!")
		}
		if config.Topic == "" {
			errors = append(errors, "Topic is undefined!")
		}
		if config.Partitions == "" {
			errors = append(errors, "Topics is undefined!")
		}
		if config.Offset == "" {
			errors = append(errors, "Offset is undefined!")
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, ";"))
	}

	return nil
}
