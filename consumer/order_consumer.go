package main

import (
	"encoding/json"
	"fmt"
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

// NewOrder - создаёт новый заказ с автоматической генерацией ID
func NewOrder(email string, amount float64) Order {
	return Order{
		ID:            fmt.Sprintf("ORD-%d", time.Now().Unix()),
		CustomerEmail: email,
		TotalAmount:   amount,
		Status:        "pending",
		CreatedAt:     time.Now(),
	}
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
		"orders",
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
		false, // auto-ack выключен: подтверждаем сообщения вручную
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
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Printf("Ошибка NACK: %v", nackErr)
				}
				continue
			}

			log.Printf("Обрабатываю заказ %s", order.ID)

			// Обрабатываем заказ
			err = processOrder(&order)
			if err != nil {
				log.Printf("Ошибка обработки: %v", err)
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Printf("Ошибка NACK: %v", nackErr)
				}
				continue
			}

			if ackErr := d.Ack(false); ackErr != nil {
				log.Printf("Ошибка ACK: %v", ackErr)
				continue
			}
			log.Printf("Заказ %s обработан и подтверждён", order.ID)
		}
	}()

	<-forever
	return nil
}

func processOrder(order *Order) error {
	order.Status = "processed"
	time.Sleep(500 * time.Millisecond)
	log.Printf("Обрабатан заказ %s. Статус %s", order.ID, order.Status)
	return nil
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
