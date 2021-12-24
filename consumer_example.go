package main

import (
	"fmt"
	"os"
)

func main() {
	ampqUrl := "amqp://test:test@127.0.0.1:5672/test"
	queueName := "e.text"
	prefetchCount := 30 // 每次拉取数量
	goCount := 10  // 协程数量 暂未使用
	consumer, err := InitConsumer(ampqUrl, queueName, prefetchCount, goCount, nil, worker)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	consumer.Run()
}

func worker(msg string) int{
	fmt.Println(msg)
	return CONSUME_SUCCESS
}