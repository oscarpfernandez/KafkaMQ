package producer

import (
	"errors"
	"sync"

	"time"

	"github.com/Shopify/sarama"
	"github.com/nu7hatch/gouuid"
	"github.com/oscarpfernandez/KafkaMQ/config"
)

func CreateNewClientConfiguration(producerConfig *configuration.KafkaProducerConfig) (*sarama.Config, error) {
	err := configuration.ValidateConfiguration(*producerConfig)
	if err != nil {
		return nil, errors.New("Client Configuration is not valid: " + err.Error())
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Waits for all the replicas to reply
	config.Producer.Timeout = 60 * time.Second       // The maximum duration the broker will wait for all RequiredAcks

	switch producerConfig.CompressionType {
	case configuration.COMPRESSION_NONE:
		config.Producer.Compression = sarama.CompressionNone
	case configuration.COMPRESSION_SNAPY:
		config.Producer.Compression = sarama.CompressionSnappy
	case configuration.COMPRESSION_GZIP:
		config.Producer.Compression = sarama.CompressionGZIP
	default:
		return nil, errors.New("Unrecognized compression scheme: ")
	}

	switch producerConfig.Partitioner {
	case "":
		if producerConfig.Partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case configuration.PARTIONER_HASH:
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case configuration.PARTIONER_RANDOM:
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case configuration.PARTIONER_MANUAL:
		config.Producer.Partitioner = sarama.NewManualPartitioner
	default:
		return nil, errors.New("Unrecognized partition scheme: ")
	}
	return config, nil
}

func CreateNewProducerMessage(configProducer configuration.KafkaProducerConfig, messageKey string, messageValue string) *sarama.ProducerMessage {
	// Define a new message to deliver to the configured topic and partition.
	message := &sarama.ProducerMessage{Topic: configProducer.Topic, Partition: int32(configProducer.Partition)}
	if messageKey != "" {
		//Key is not mandatory
		message.Key = sarama.StringEncoder(messageKey)
	} else {
		uuidVal, _ := uuid.NewV4()
		message.Key = sarama.StringEncoder(uuidVal.String())
	}
	message.Value = sarama.StringEncoder(messageValue)
	return message
}

func CreateNewSyncProducer(brokerList []string, kafkaConfiguration *sarama.Config) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(brokerList, kafkaConfiguration)
	return producer, err
}

func CloseSyncProducer(syncProducer sarama.SyncProducer) error {
	return syncProducer.Close()
}

func CreateNewASyncProducer(brokerList []string, kafkaConfiguration *sarama.Config) (sarama.AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokerList, kafkaConfiguration)
	return producer, err
}

func SendMessageToASyncProducerTopic(kProducer sarama.AsyncProducer, message *sarama.ProducerMessage, topicName string,
	wg *sync.WaitGroup, throttle chan int) {
	defer wg.Done() // Decrease the number of alive goroutines
	kProducer.Input() <- message
	<-throttle
}

func CloseAsyncProducer(asyncProducer sarama.AsyncProducer) error {
	return asyncProducer.Close()
}
