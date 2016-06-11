package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"


	"github.com/Shopify/sarama"
	"github.com/oscarpfernandez/KafkaMQ/config"
	"github.com/oscarpfernandez/KafkaMQ/utils"
	"github.com/oscarpfernandez/KafkaMQ/consumer"
)

var (
	configurationFile = flag.String("config", "", "The configuration file to easily use this client")
	brokerList        = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic             = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions        = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset            = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose           = flag.Bool("verbose", false, "Turn off printing the message's topic, partition, and offset to stdout")
	bufferSize        = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

	useConfigFile bool = false
	err           error

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	var configConsumer configuration.KafkaConsumerConfig

	if len(*configurationFile) > 0 {
		err = configuration.ParseConfiguration(*configurationFile, &configConsumer)

		log.Print("Loaded configuration file = %s\n", *configurationFile)

		useConfigFile = true

		if err != nil {
			log.Fatal(err)
			utils.PrintUsageErrorAndExit("The provided consifuration file is invalid or not found")
		}

		fmt.Printf("Loaded properties from config file: \n%+v\n", utils.InterfaceToString(configConsumer, utils.StructToString))

	} else if !useConfigFile {
		//populate the struct from the command line parameters.
		configConsumer.BrokerList = *brokerList
		configConsumer.Topic = *topic
		configConsumer.Offset = *offset
		configConsumer.Partitions = *partitions
		configConsumer.Verbose = *verbose

		fmt.Printf("Loaded properties: \n%+v\n", utils.InterfaceToString(configConsumer, utils.StructToString))
	}

	err := configuration.ValidateConfiguration(configConsumer)
	if err != nil {
		utils.PrintUsageErrorAndExit("The confguration provided is invalid or imcomplete.")
	}

	if configConsumer.Verbose {
		sarama.Logger = logger
	}

	var initialOffset int64
	switch configConsumer.Offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		utils.PrintUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	consumerInstance, err := consumer.CreateNewConsumer(strings.Split(configConsumer.BrokerList, ","))
	if err != nil {
		utils.PrintErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := consumer.GetPartitions(consumerInstance, configConsumer.Topic, configConsumer.Partitions)
	if err != nil {
		utils.PrintErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messagesChannel = make(chan *sarama.ConsumerMessage, *bufferSize)
		closingChannel  = make(chan struct{})
		waitgroup       sync.WaitGroup
	)

	// Creates hook to wait for signal from the command line to stop the consumer.
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closingChannel)
	}()

	// Reads from all the available partitions configured in the properties.
	for _, partition := range partitionList {
		pc, err := consumer.ConsumeFromPartition(
			consumerInstance,
			configConsumer.Topic,
			partition,
			initialOffset)
		if err != nil {
			utils.PrintErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		// When the channel is closing channel is closed it triggers the async close of the ProducerConsumer instance.
		go func(pc sarama.PartitionConsumer) {
			<-closingChannel
			pc.AsyncClose()
		}(pc)

		// Handles as many calls as entries in the messageChannel buffer.
		// When the ProducerConsumer is finally closed the
		waitgroup.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer waitgroup.Done()
			for message := range pc.Messages() {
				messagesChannel <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messagesChannel {
			fmt.Printf("Partition: %d\t ", msg.Partition)
			fmt.Printf("Offset: %d\t", msg.Offset)
			fmt.Printf("Key: %s\t", string(msg.Key))
			fmt.Printf("Value: %s\t", string(msg.Value))
			fmt.Println()
		}
	}()

	waitgroup.Wait()
	logger.Println("Done consuming topic: ", configConsumer.Topic)
	close(messagesChannel)

	if err := consumer.CloseKafkaConsumer(consumerInstance); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}
