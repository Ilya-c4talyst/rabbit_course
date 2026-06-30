package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Order - структура заказа
type Order struct {
	ID            string    `json:"id"`             // Уникальный идентификатор
	CustomerEmail string    `json:"customer_email"` // Email для уведомлений
	TotalAmount   float64   `json:"total_amount"`   // Сумма заказа
	Status        string    `json:"status"`         // Статус: pending, processed, failed
	CreatedAt     time.Time `json:"created_at"`     // Время создания
}

type OrderConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewOrderConsumer() (*OrderConsumer, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &OrderConsumer{
		conn:    conn,
		channel: ch,
	}, nil
}

func (oc *OrderConsumer) ProcessOrders() error {
	q, err := oc.channel.QueueDeclare(
		"processed_orders",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := oc.channel.Consume(
		q.Name,
		"",
		true, // auto-ack включён
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Printf("Ожидаю заказы...")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var order Order
			// Десериализуем JSON обратно в структуру
			err := json.Unmarshal(d.Body, &order)
			if err != nil {
				log.Printf("Ошибка парсинга JSON: %v", err)
				continue
			}

			// Обрабатываем заказ
			processOrder(order)
		}
	}()

	<-forever
	return nil
}

func processOrder(order Order) {
	log.Printf("Заказ %s обработан! Сумма: %.2f₽, Статус: %s", order.ID, order.TotalAmount, order.Status)
}

func (oc *OrderConsumer) Close() {
	oc.channel.Close()
	oc.conn.Close()
}

func main() {
	consumer, err := NewOrderConsumer()
	if err != nil {
		log.Fatal("Ошибка создания консюмера:", err)
	}
	defer consumer.Close()

	err = consumer.ProcessOrders()
	if err != nil {
		log.Fatal("Ошибка обработки:", err)
	}
}
