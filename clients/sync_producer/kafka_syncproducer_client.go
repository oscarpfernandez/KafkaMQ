package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"time"

	"github.com/Shopify/sarama"
	"github.com/oscarpfernandez/KafkaMQ/config"
	"github.com/oscarpfernandez/KafkaMQ/utils"
	"github.com/oscarpfernandez/KafkaMQ/producer"
)

var (
	configurationFile = flag.String("config", "", "The configuration file to easily use this client")
	brokerList        = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	clientId          = flag.String("clientId", "clientId", "REQUIRED: this client's ID")
	topic             = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key               = flag.String("key", "", "The key of the message to produce. Can be empty.")
	value             = flag.String("value", "", "REQUIRED: the value of the message to produce. You can also provide the value on stdin.")
	partitioner       = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition         = flag.Int("partition", -1, "The partition to produce to.")
	verbose           = flag.Bool("verbose", false, "Turn off printing the message's topic, partition, and offset to stdout")
	messageResend     = flag.Int("messageResend", 1, "[*] Number of messages to send (Default 1)")

	logger = log.New(os.Stderr, "", log.LstdFlags)

	useConfigFile bool = false
	err           error
)

func main() {
	flag.Parse()

	var configProducer configuration.KafkaProducerConfig

	if len(*configurationFile) > 0 {
		err = configuration.ParseConfiguration(*configurationFile, &configProducer)

		log.Print("Loaded configuration file = %s\n", *configurationFile)

		useConfigFile = true

		if err != nil {
			log.Fatal(err)
			utils.PrintUsageErrorAndExit("The provided consifuration file is invalid or not found")
		}

		fmt.Printf("Loaded properties from config file: \n%+v\n", utils.InterfaceToString(configProducer, utils.StructToString))

	} else if !useConfigFile {
		//populate the struct from the command line parameters.
		configProducer.BrokerList = *brokerList
		configProducer.ClientId = *clientId
		configProducer.Topic = *topic
		configProducer.CompressionType = configuration.COMPRESSION_NONE
		configProducer.Partition = *partition
		configProducer.Partitioner = *partitioner
		configProducer.Verbose = *verbose

		fmt.Printf("Loaded properties: \n%+v\n", utils.InterfaceToString(configProducer, utils.StructToString))
	}

	if configProducer.Verbose {
		sarama.Logger = logger
	}

	/*
	 * Creates new client Kafka Client instance according to the KafkaProducerConfig
	 */
	config, err := producer.CreateNewClientConfiguration(&configProducer)

	/*
	 * Create a new Sync Producer that will use the provided broker servers.
	 */
	kafkaProducer, err := producer.CreateNewSyncProducer(strings.Split(configProducer.BrokerList, ","), config)

	if err != nil {
		utils.PrintErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.CloseSyncProducer(kafkaProducer); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	/*
	 * Send the message!
	 */
	for i := 0; i < *messageResend; i++ {
		/*
		 * Define a new message to deliver to the configured topic and partition.
		 */
		message := producer.CreateNewProducerMessage(configProducer, *key, *value)

		partition, offset, err := kafkaProducer.SendMessage(message)
		message.Metadata = time.Now().String()
		if err != nil {
			utils.PrintErrorAndExit(69, "Failed to produce message: %s", err)
		} else if configProducer.Verbose {
			fmt.Printf("Topic = %s\t", configProducer.Topic)
			fmt.Printf("Partition = %d\t", partition)
			fmt.Printf("partition = %d\t", offset)
			fmt.Printf("Metadata = %s\t", message.Metadata)
			fmt.Printf("Key = %s\t", message.Key)
			fmt.Printf("Value = %s\t", message.Value)
			fmt.Println()
		}
	}

	log.Print("All messages were sent succesfully")
}
