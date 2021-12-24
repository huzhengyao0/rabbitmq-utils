package service

import (
	"github.com/streadway/amqp"
	"net"
	"time"
)

type RabbitMQReceive struct {
	amqpUrl      string
	exchange     string
	exchangeType string
	routingKey   string
	vHost        string
	conn         *amqp.Connection
	channel      *amqp.Channel
}

func InitReceive(amqpUrl, exchange, exchangeType, routingKey, vHost string, timeout time.Duration) (*RabbitMQReceive, error) {
	if timeout == 0 {
		timeout = 15
	}
	conn, err := amqp.DialConfig(amqpUrl, amqp.Config{Vhost: vHost,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout*time.Second) // 连接超时时间  15秒
		}})
	// 校验是否连接上rabbitMq
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMQReceive{
		amqpUrl:      amqpUrl,
		exchange:     exchange,
		exchangeType: exchangeType,
		routingKey:   routingKey,
		vHost:        vHost,
		conn:         conn,
		channel:      channel,
	}, nil
}

func (receive *RabbitMQReceive) Send(msg string, priority int8) error {
	//插队列
	err := receive.channel.Publish(
		receive.exchange,   // exchange
		receive.routingKey, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: 2,
			Priority:     uint8(priority),
			Body:         []byte(msg),
		})
	return err
}

func (receive *RabbitMQReceive) Close() {
	if receive.channel != nil {
		_ = receive.channel.Close()
	}
	if receive.conn != nil {
		_ = receive.conn.Close()
	}
}
