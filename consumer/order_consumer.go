package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

const (
	ordersQueue          = "orders"
	invalidMessagesQueue = "invalid_messages"
	failedOrdersQueue    = "failed_orders"
	maxRetryCount        = 3
	retryDelay           = 5 * time.Second
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
	q, err := oc.declareQueue(ordersQueue)
	if err != nil {
		return err
	}

	if _, err := oc.declareQueue(invalidMessagesQueue); err != nil {
		return err
	}

	if _, err := oc.declareQueue(failedOrdersQueue); err != nil {
		return err
	}

	err = oc.channel.Qos(
		1,
		0,
		false,
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
				oc.handleInvalidMessage(d, err)
				continue
			}

			log.Printf("Обрабатываю заказ %s", order.ID)

			// Обрабатываем заказ
			err = processOrder(&order)
			if err != nil {
				oc.handleProcessingError(d, order, err)
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
	if order.TotalAmount <= 0 {
		return errors.New("сумма заказа должна быть больше 0")
	}

	order.Status = "processed"
	time.Sleep(500 * time.Millisecond)
	log.Printf("Обрабатан заказ %s. Статус %s", order.ID, order.Status)
	return nil
}

func (oc *OrderConsumer) declareQueue(name string) (amqp.Queue, error) {
	return oc.channel.QueueDeclare(
		name,
		true,  // durable = true - очередь переживёт перезапуск брокера
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

func (oc *OrderConsumer) handleInvalidMessage(d amqp.Delivery, parseErr error) {
	headers := copyHeaders(d.Headers)
	headers["error"] = parseErr.Error()
	headers["failed_at"] = time.Now().Format(time.RFC3339)
	headers["source_queue"] = ordersQueue

	err := oc.publishRaw(invalidMessagesQueue, d.Body, d.ContentType, headers)
	if err != nil {
		log.Printf("Ошибка отправки сообщения в %s: %v", invalidMessagesQueue, err)
		if nackErr := d.Nack(false, false); nackErr != nil {
			log.Printf("Ошибка NACK с requeue=false: %v", nackErr)
		}
		return
	}

	if nackErr := d.Nack(false, false); nackErr != nil {
		log.Printf("Ошибка NACK с requeue=false: %v", nackErr)
	}
	log.Printf("Некорректное сообщение отправлено в %s", invalidMessagesQueue)
}

func (oc *OrderConsumer) handleProcessingError(d amqp.Delivery, order Order, processErr error) {
	retryCount := getRetryCount(d.Headers)
	nextRetryCount := retryCount + 1

	log.Printf(
		"Ошибка обработки заказа %s: %v. Попытка %d из %d",
		order.ID,
		processErr,
		nextRetryCount,
		maxRetryCount,
	)

	if nextRetryCount >= maxRetryCount {
		order.Status = "failed"
		body, err := json.Marshal(order)
		if err != nil {
			log.Printf("Ошибка сериализации заказа %s для %s: %v", order.ID, failedOrdersQueue, err)
			if nackErr := d.Nack(false, true); nackErr != nil {
				log.Printf("Ошибка NACK с requeue=true: %v", nackErr)
			}
			return
		}

		headers := copyHeaders(d.Headers)
		headers["retry_count"] = nextRetryCount
		headers["error"] = processErr.Error()
		headers["failed_at"] = time.Now().Format(time.RFC3339)

		err = oc.publishRaw(failedOrdersQueue, body, "application/json", headers)
		if err != nil {
			log.Printf("Ошибка отправки заказа %s в %s: %v", order.ID, failedOrdersQueue, err)
			if nackErr := d.Nack(false, true); nackErr != nil {
				log.Printf("Ошибка NACK с requeue=true: %v", nackErr)
			}
			return
		}

		if nackErr := d.Nack(false, false); nackErr != nil {
			log.Printf("Ошибка NACK с requeue=false: %v", nackErr)
		}
		log.Printf("Заказ %s отправлен в %s после %d неудачных попыток. Ошибка: %v", order.ID, failedOrdersQueue, maxRetryCount, processErr)
		return
	}

	log.Printf("Повторная попытка заказа %s через %s", order.ID, retryDelay)
	time.Sleep(retryDelay)

	headers := copyHeaders(d.Headers)
	headers["retry_count"] = nextRetryCount

	err := oc.publishRaw(ordersQueue, d.Body, d.ContentType, headers)
	if err != nil {
		log.Printf("Ошибка повторной отправки заказа %s в %s: %v", order.ID, ordersQueue, err)
		if nackErr := d.Nack(false, true); nackErr != nil {
			log.Printf("Ошибка NACK с requeue=true: %v", nackErr)
		}
		return
	}

	if nackErr := d.Nack(false, false); nackErr != nil {
		log.Printf("Ошибка NACK с requeue=false: %v", nackErr)
	}
	log.Printf("Заказ %s отправлен на повторную попытку %d", order.ID, nextRetryCount)
}

func (oc *OrderConsumer) publishRaw(queueName string, body []byte, contentType string, headers amqp.Table) error {
	if contentType == "" {
		contentType = "application/json"
	}

	return oc.channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  contentType,
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
			Body:         body,
		},
	)
}

func copyHeaders(headers amqp.Table) amqp.Table {
	result := amqp.Table{}
	for key, value := range headers {
		result[key] = value
	}
	return result
}

func getRetryCount(headers amqp.Table) int {
	value, ok := headers["retry_count"]
	if !ok {
		return 0
	}

	switch retryCount := value.(type) {
	case int:
		return retryCount
	case int8:
		return int(retryCount)
	case int16:
		return int(retryCount)
	case int32:
		return int(retryCount)
	case int64:
		return int(retryCount)
	case uint8:
		return int(retryCount)
	case uint16:
		return int(retryCount)
	case uint32:
		return int(retryCount)
	case uint64:
		return int(retryCount)
	case string:
		parsed, err := strconv.Atoi(retryCount)
		if err == nil {
			return parsed
		}
	}

	return 0
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
