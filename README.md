# Message Broker Queue Research API's

This includes some exploratory code in how to use and interact with Sarama's API for Kafka (under development).

## 1. KafkaMQ

Apache Kafka is publish-subscribe messaging rethought as a distributed commit log.

> Fast: A single Kafka broker can handle hundreds of megabytes of reads and writes per second from thousands of clients.

> Scalable: Kafka is designed to allow a single cluster to serve as the central data backbone for a large organization. It can be elastically and transparently expanded without downtime. Data streams are partitioned and spread over a cluster of machines to allow data streams larger than the capability of any single machine and to allow clusters of co-ordinated consumers

> Durable: Messages are persisted on disk and replicated within the cluster to prevent data loss. Each broker can handle terabytes of messages without performance impact.

> Distributed by Design: Kafka has a modern cluster-centric design that offers strong durability and fault-tolerance guarantees.

For more details about Kafka please consult http://kafka.apache.org

---

### 1.1 Installation & Usage

To compile and install the Kafka wrapper API and client tools:
```sh
$ build ./...
```

Please meake sure you have a Kafka environment running with N Brokers. The clients can be easily configured through the YAML configuration files under the **conf/** folder.

To execute the **consumer** client
```sh
$ $GOPATH/bin/kafkaConsumer -config ./conf/kafka-consumer.yaml
```

To execute the **synchronous producer** client:
```sh
$ $GOPATH/bin/kafkaAsyncProducer -config ./conf/kafka-producer.yaml -value "This is a brand new message" -messageResend 100000
```
To execute the **asynchronous producer** client:
```sh
$ $GOPATH/bin/kafkaAsyncProducer -config ./conf/kafka-producer.yaml -value "This is a brand new message" -messageResend 100000 -numThreads 250 
```

---
