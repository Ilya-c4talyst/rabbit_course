package main

import (
	"github.com/streadway/amqp"
	"log"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Не могу подключиться:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Не могу открыть канал:", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Не могу создать очередь:", err)
	}

	// Подписываемся на сообщения
	msgs, err := ch.Consume(
		q.Name, // queue - имя очереди
		"",     // consumer - имя консюмера (пустое = автогенерация)
		true,   // auto-ack - автоматическое подтверждение получения
		false,  // exclusive - эксклюзивный доступ
		false,  // no-local - не получать свои же сообщения
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatal("Не могу подписаться:", err)
	}

	log.Printf("Ожидаю сообщения. Нажмите CTRL+C для выхода")

	forever := make(chan bool)

	// Обрабатываем сообщения в отдельной горутине
	go func() {
		for d := range msgs {
			log.Printf("Получено: %s", d.Body)
		}
	}()

	<-forever // Блокируем выход из программы
}
