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

			log.Printf("Обрабатываю заказ %s", order.ID)

			// Обрабатываем заказ
			order = processOrder(order)
			err = oc.SendOrder(order)
			if err != nil {
				log.Printf("Ошибка отправки обработанного заказа: %v", err)
				return
			}
		}
	}()

	<-forever
	return nil
}

func processOrder(order Order) Order {
	order.Status = "processed"
	time.Sleep(500 * time.Millisecond)
	return order
}

func (op *OrderConsumer) SendOrder(order Order) error {
	// Конвертируем структуру Order в JSON
	body, err := json.Marshal(order)
	if err != nil {
		return err
	}

	q, err := op.channel.QueueDeclare(
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

	// Отправляем JSON в очередь
	err = op.channel.Publish("", q.Name, false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		return err
	}

	log.Printf("Заказ %s отправлен в очередь 3", order.ID)
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
