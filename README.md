# rabbit_course

### Запуск контейнера
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

### Очереди
- `orders` - основные заказы
- `invalid_messages` - сообщения, которые не удалось распарсить как JSON
- `failed_orders` - заказы, которые не удалось обработать за 3 попытки

Все очереди объявляются как durable, а сообщения публикуются как persistent.

### Повторные попытки
При ошибке обработки consumer увеличивает заголовок `retry_count`, ждёт 5 секунд и возвращает сообщение в `orders`.
После 3 неудачных попыток заказ отправляется в `failed_orders`.

При ошибке парсинга JSON сообщение отправляется в `invalid_messages` и не возвращается в `orders`.

### Запуск
```bash
cd producer
go run .
```

```bash
cd consumer
go run .
```
