package consumer

import (
	"strconv"
	"strings"


	"sync"

	"time"

	"github.com/Shopify/sarama"
	"github.com/oscarpfernandez/KafkaMQ/utils"
)

func createNewConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.MaxWaitTime = 5 * time.Second
	config.Consumer.MaxProcessingTime = 20 * time.Second
	return config
}

// Creates new Kafka generic consumer attached to a set of brokers.
func CreateNewConsumer(brokerlist []string) (sarama.Consumer, error) {
	config, err := sarama.NewConsumer(brokerlist, createNewConsumerConfig())
	return config, err
}

// Attaches an initialized Kafka Consumer with a Kafka topic, in conjunction with a given topic partition.
// The topic will be read from the point specified by the initial offset.
func ConsumeFromPartition(consumer sarama.Consumer, topic string, partition int32, initialOffset int64) (sarama.PartitionConsumer, error) {
	pc, err := consumer.ConsumePartition(topic, partition, initialOffset)
	return pc, err
}

func ReceiveMessages(consumerInstance sarama.Consumer,
	topic string, initialOffset int64,
	partitionList []int32,
	messages chan *sarama.ConsumerMessage,
	wg sync.WaitGroup,
	closing chan struct{}) {

	for _, partition := range partitionList {
		pc, err := ConsumeFromPartition(
			consumerInstance,
			topic,
			partition,
			initialOffset)
		if err != nil {
			utils.PrintErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}
}

// Closes the Kafka Consumer.
func CloseKafkaConsumer(consumer sarama.Consumer) error {
	return consumer.Close()
}

// Retrieves the list of partitions available for a given Kafka topic.
// - c sarama.Consumer: the initialized Kafka consumer
// - topic string: the kafka topic to use
// - paritions string : the comma separated list of partitions ids to use. 'all' to use all the available.
func GetPartitions(c sarama.Consumer, topic string, partitions string) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}
