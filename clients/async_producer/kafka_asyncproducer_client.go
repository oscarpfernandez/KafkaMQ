package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"

	"time"

	"math"

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
	numOfThreads      = flag.Int("numThreads", 50, "The number of threads to be used in the producer.")
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
	kafkaProducer, err := producer.CreateNewASyncProducer(strings.Split(configProducer.BrokerList, ","), config)

	if err != nil {
		utils.PrintErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.CloseAsyncProducer(kafkaProducer); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	var saramaError chan *sarama.ProducerError     //Channel for to receive error from a submission
	var saramaSuccess chan *sarama.ProducerMessage //Channel for message handling

	/*
	 * Create and end the message!
	 */
	t1 := time.Now()
	wg := new(sync.WaitGroup)
	var throttle = make(chan int, *numOfThreads)
	defer close(throttle)

	// Use all the CPU's
	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)

	for i := 0; i < *messageResend; i++ {
		message := producer.CreateNewProducerMessage(configProducer, *key, *value)
		message.Metadata = time.Now().String()
		if configProducer.Verbose {
			fmt.Printf("Topic = %s\t", configProducer.Topic)
			fmt.Printf("Partition = %d\t", partition)
			fmt.Printf("Metadata = %s\t", message.Metadata)
			fmt.Printf("Key = %s\t", message.Key)
			fmt.Printf("Value = %s\t", message.Value)
			fmt.Println()
		}

		throttle <- 1 // whatever number
		wg.Add(1)
		go producer.SendMessageToASyncProducerTopic(kafkaProducer, message, configProducer.Topic, wg, throttle)

		go func() {
			select {
			case error := <-kafkaProducer.Errors():
				saramaError <- error
				logger.Print("Message was not delivered...")
			case success := <-kafkaProducer.Successes():
				saramaSuccess <- success
				logger.Print("Message sent succesfully...")
			}
		}()
	}
	wg.Wait() // Wait for all the children to die
	t2 := time.Since(t1)

	log.Printf("#Messages = %d\tTime = %f", *messageResend, float64(t2.Nanoseconds())/math.Pow(10, 9))
	log.Print("All messages were sent succesfully! ")
}
