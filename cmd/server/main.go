package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitMQURL = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %s", err)
	}

	err = pubsub.SubscribeGob(
		conn,
		string(routing.ExchangePerilTopic),
		string(routing.GameLogSlug),
		string(routing.GameLogSlug)+".*",
		pubsub.SimpleQueueDurable,
		handlerGameLog(),
	)
	if err != nil {
		log.Fatalf("Failed to create game log queue: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch cmd := input[0]; cmd {
		case "pause":
			log.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				channel,
				string(routing.ExchangePerilDirect),
				string(routing.PauseKey),
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Fatalf("failed to publish pause message: %v", err)
			}
			fmt.Println("Pause message sent")
		case "resume":
			log.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				channel,
				string(routing.ExchangePerilDirect),
				string(routing.PauseKey),
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				{
					log.Fatalf("Failed to publish resume message: %v", err)
				}
			}
			log.Println("Resume message sent")
		case "quit":
			log.Println("Peril server is shutting down...")
			return
		default:
			log.Printf("Invalid command: %s\n", cmd)
		}
	}
}
