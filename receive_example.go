package utils

import (
	"fmt"
	"github.com/huzhengyao0/rabbitmq-utils/service"
	"os"
	"time"
)

func main() {
	ampqUrl := "amqp://test:test@127.0.0.1:5672/test"
	exchange := "e.text"
	exchangeType := "direct"
	routingKey := "test"
	vHost := "test"
	timeout := 30 * time.Second
	message := "{\"name\":\"aa\"}"
	rabbitMQReceive, err := service.InitReceive(ampqUrl, exchange, exchangeType, routingKey, vHost, timeout)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	err = rabbitMQReceive.Send(message, 1)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
