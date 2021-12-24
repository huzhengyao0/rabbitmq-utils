package service

import (
	"errors"
	"github.com/streadway/amqp"
)

const (
	CONSUME_SUCCESS   = 0
	CONSUME_REDELIVER = 1
	CONSUME_REJECT    = 2

	DEFAULT_PREFETCH_COUNT = 30
	DEFAULT_GOROUTINE      = 10
)

type Consumer struct {
	AmqpUrl       string
	QueueName     string
	PrefetchCount int
	GoCount       int
	worker        func(args string) int
	consumerConn  *amqp.Connection
	channel       *amqp.Channel
	delivery      <-chan amqp.Delivery
}

func InitConsumer(amqpUrl, queueName string, prefetchCount, goCount int, args map[string]interface{}, worker func(args string) int) (*Consumer, error) {
	if prefetchCount == 0 {
		prefetchCount = DEFAULT_PREFETCH_COUNT
	}
	if goCount == 0 {
		goCount = DEFAULT_GOROUTINE
	}
	if amqpUrl == "" {
		return nil, errors.New("amqpUrl is not Empty ! ")
	}
	if queueName == "" {
		return nil, errors.New("queueName is not empty ! ")
	}
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = channel.Qos(prefetchCount, 0, false)
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args)
	if err != nil {
		return nil, err
	}
	delivery, err := channel.Consume(
		queue.Name,
		"",
		false, // 开启手动确认消息是否重新放回队列
		false,
		false,
		false,
		args)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		AmqpUrl:       amqpUrl,
		QueueName:     queueName,
		PrefetchCount: prefetchCount,
		GoCount:       goCount,
		worker:        worker,
		consumerConn:  conn,
		channel:       channel,
		delivery:      delivery,
	}, nil
}

func (consumer *Consumer) RegisterWorker(workerFunc func(args string) int) {
	consumer.worker = workerFunc
}

func (consumer *Consumer) Run() {
	for msg := range consumer.delivery {
		result := consumer.worker(string(msg.Body))
		if result == CONSUME_SUCCESS {
			// 消费成功
			_ = msg.Ack(true)
		} else if result == CONSUME_REDELIVER {
			// 消费失败 重新放回队列
			_ = msg.Reject(true)
		} else {
			// 消费失败 消息抛弃
			_ = msg.Reject(false)
		}
	}
	forever := make(chan bool)
	<-forever
}

func (consumer *Consumer) Close() {
	defer consumer.channel.Close()
	defer consumer.consumerConn.Close()
}
