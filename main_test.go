package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestProducer(t *testing.T) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        Broker,
		"security.protocol":        "SSL",
		"ssl.ca.location":          SslCa,
		"ssl.certificate.location": SslCer,
		"ssl.key.location":         SslKey,
		"delivery.timeout.ms":      2000,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	topic := Topic
	value := "你好^^"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "Timestamp", Value: []byte(time.Now().String())}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

}
