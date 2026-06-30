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

type OrderProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewOrderProducer() (*OrderProducer, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"orders", // Имя очереди для заказов
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &OrderProducer{
		conn:    conn,
		channel: ch,
		queue:   q,
	}, nil
}

func (op *OrderProducer) SendOrder(order Order) error {
	// Конвертируем структуру Order в JSON
	body, err := json.Marshal(order)
	if err != nil {
		return err
	}

	// Отправляем JSON в очередь
	err = op.channel.Publish(
		"",
		op.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json", // Указываем тип контента
			Body:        body,
		})

	if err != nil {
		return err
	}

	log.Printf("Заказ %s отправлен в очередь", order.ID)
	return nil
}

func (op *OrderProducer) Close() {
	op.channel.Close()
	op.conn.Close()
}

func main() {
	producer, err := NewOrderProducer()
	if err != nil {
		log.Fatal("Ошибка создания продюсера:", err)
	}
	defer producer.Close()

	// Создаём несколько тестовых заказов
	orders := []Order{
		NewOrder("customer1@example.com", 1500.50),
		NewOrder("customer2@example.com", 2300.00),
		NewOrder("customer3@example.com", 750.25),
	}

	for _, order := range orders {
		err := producer.SendOrder(order)
		if err != nil {
			log.Printf("Ошибка отправки заказа: %v", err)
		}
	}

	log.Println("Все заказы отправлены!")
}
