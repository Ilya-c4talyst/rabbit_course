package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	// Подключаемся к RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Не могу подключиться к RabbitMQ:", err)
	}
	defer conn.Close()

	// Открываем канал - через него идёт вся работа
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Не могу открыть канал:", err)
	}
	defer ch.Close()

	// Создаём очередь (если её ещё нет)
	q, err := ch.QueueDeclare(
		"hello", // название очереди
		false,   // durable - сохранять ли очередь при перезапуске
		false,   // delete when unused - удалять при отсутствии подписчиков
		false,   // exclusive - эксклюзивная для этого подключения
		false,   // no-wait - не ждать подтверждения от сервера
		nil,     // arguments - дополнительные параметры
	)
	if err != nil {
		log.Fatal("Не могу создать очередь:", err)
	}

	// Отправляем сообщение
	body := fmt.Sprintf("Сообщение от Ilya отправлено в %s", time.Now().Format("15:04:05"))
	err = ch.Publish(
		"",     // exchange - пока используем дефолтный
		q.Name, // routing key - имя очереди
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		log.Fatal("Не могу отправить сообщение:", err)
	}

	log.Printf("Отправлено: %s", body)
}
