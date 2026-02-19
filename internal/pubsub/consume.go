package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
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
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
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

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
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
			data, err := unmarshaller(message.Body)
			if err != nil {
				log.Println(err)
				continue
			}
			acktype := handler(data)
			switch acktype {
			case Ack:
				err = message.Ack(false)
				if err != nil {
					fmt.Printf("failed to acknowledge message: %v\n", err)
				}
			case NackRequeue:
				err = message.Nack(false, true)
				if err != nil {
					fmt.Printf("failed to acknowledge message: %v\n", err)
				}
			case NackDiscard:
				err = message.Nack(false, false)
				if err != nil {
					fmt.Printf("failed to acknowledge message: %v\n", err)
				}
			}
		}
	}()

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(b []byte) (T, error) {
			var val T
			err := json.Unmarshal(b, &val)
			return val, err
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(b []byte) (T, error) {
			var val T
			decoder := gob.NewDecoder(bytes.NewReader(b))
			err := decoder.Decode(&val)
			return val, err
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}
	return nil
}
