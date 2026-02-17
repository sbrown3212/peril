package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient,
		queueType == SimpleQueueTransient,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	subscribeCh, queue, err := DeclareAndBind(
		conn, exchange, queueName, key, queueType,
	)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %w", err)
	}

	deliveryChan, err := subscribeCh.Consume(
		queue.Name, "", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	go func() {
		for message := range deliveryChan {
			var data T
			err := json.Unmarshal(message.Body, &data)
			if err != nil {
				log.Println(err)
				continue
			}
			handler(data)
			err = message.Ack(false)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}()

	return nil
}
