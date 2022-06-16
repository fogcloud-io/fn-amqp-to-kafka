package main

import (
	"context"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	consumer "github.com/fogcloud-io/fog-amqp-consumer"
)

var (
	AMQP_HOST         = os.Getenv("AMQP_HOST")
	AMQP_PORT         = os.Getenv("AMQP_PORT")
	FOG_ACCESS_KEY    = os.Getenv("FOG_ACCESS_KEY")
	FOG_ACCESS_SECRET = os.Getenv("FOG_ACCESS_SECRET")
)

var (
	KAFKA_HOST  = os.Getenv("KAFKA_HOST")
	KAFKA_TOPIC = os.Getenv("KAFKA_TOPIC")

	producer *kafka.Producer
)

func init() {
	amqpCli := consumer.InitAMQPConsumer(context.Background(), AMQP_HOST, AMQP_PORT, FOG_ACCESS_KEY, FOG_ACCESS_SECRET)
	producer = initKafkaProducer()
	go amqpCli.ConsumeWithHanlder(context.Background(), saveMsgToKafka)
}

// fission function entrypoint
func Handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello World!"))
}

func initKafkaProducer() (p *kafka.Producer) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KAFKA_HOST,
	})
	if err != nil {
		panic(err)
	}
	return
}

func saveMsgToKafka(msg []byte) {
	producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &KAFKA_TOPIC, Partition: kafka.PartitionAny},
			Value:          msg,
		},
		nil,
	)
	producer.Flush(10 * 1000)
}
