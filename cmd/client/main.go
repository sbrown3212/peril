package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitMQURL = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ")
}
