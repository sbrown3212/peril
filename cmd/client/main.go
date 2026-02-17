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

	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer conn.Close()

	fmt.Println("Peril game client connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get username: %s", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to create channel for army_moves: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	// Subscribe to "pause" queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		"pause."+gameState.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to pause channel: %v", err)
	}

	// Subscribe to "army_move" channel
	err = pubsub.SubscribeJSON(
		conn,
		string(routing.ExchangePerilTopic),
		string(routing.ArmyMovesPrefix)+"."+gameState.GetUsername(),
		string(routing.ArmyMovesPrefix)+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to army_move channel: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch cmd := input[0]; cmd {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = pubsub.PublishJSON(
				publishCh,
				string(routing.ExchangePerilTopic),
				string(routing.ArmyMovesPrefix)+"."+gameState.GetUsername(),
				move,
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}

			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
			fmt.Println("Move was published successfully")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Unknown command: %v", input[0])
		}
	}
}
