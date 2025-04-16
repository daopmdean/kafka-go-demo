package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	topic     = "demo-topic"
	brokerURL = "localhost:9092"
)

func main() {
	ctx := context.Background()

	// 1. Create Topic (Optional if already created)
	createTopic(topic)

	// 2. Write message
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("key-1"),
		Value: []byte("Hello Kafka from Go!"),
	})
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	fmt.Println("Message sent")

	// 3. Read message
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerURL},
		Topic:     topic,
		GroupID:   "my-group",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatal("failed to read message:", err)
	}
	fmt.Printf("Received: %s = %s\n", string(msg.Key), string(msg.Value))
	fmt.Println("Finished reading message")
}

func createTopic(topic string) {
	conn, err := kafka.Dial("tcp", brokerURL)
	if err != nil {
		log.Fatal("failed to connect to Kafka:", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatal("failed to get controller:", err)
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", controller.Host+":"+fmt.Sprint(controller.Port))
	if err != nil {
		log.Fatal("failed to connect to controller:", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Fatal("failed to create topic:", err)
	}
	fmt.Println("Topic created:", topic)
}
