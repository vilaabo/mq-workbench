# IBM MQ HTTP API

Небольшой HTTP-сервис на Java, который оборачивает IBM MQ-клиент: позволяет читать, отправлять и очищать очереди через REST-эндпоинты.

## Стек

- Java 21
- Gradle (Kotlin DSL)
- [Javalin 6](https://javalin.io/) — HTTP-сервер
- Jackson — JSON-сериализация
- IBM MQ Java client (`com.ibm.mq.allclient` 9.3.4.0)

## Требования

- JDK 21
- Доступный IBM MQ Queue Manager (хост, порт, channel, имя QM)

## Запуск

Параметры подключения передаются как CLI-аргументы:

```bash
gradle run --args="--host=localhost --port=1414 --channel=DEV.ADMIN.SVRCONN --qmgr=QM1"
```

| Аргумент | Обязательный | Дефолт | Описание |
|---|---|---|---|
| `--host` | да | — | Хост Queue Manager |
| `--port` | да | — | Порт listener-а |
| `--channel` | да | — | Server connection channel |
| `--qmgr` | да | — | Имя Queue Manager |
| `--api-port` | нет | `8080` | Порт HTTP API |

При старте сервис подключается к Queue Manager один раз и держит соединение всё время своей жизни. Каждое сообщение/операция открывает свою `MQQueue` (поток-безопасно).

## API

База: `http://localhost:{api-port}` (по умолчанию `8080`).
Имя очереди передаётся в URL: `{queue}` — например `DEV.QUEUE.1`.

### GET /messages/{queue}

Просмотр всех сообщений в очереди **без удаления** (browse). Можно вызывать сколько угодно раз — состояние очереди не меняется.

Ответ:

```json
{
  "queue": "DEV.QUEUE.1",
  "count": 1,
  "messages": [
    {
      "messageId": "414D5120514D31...",
      "correlationId": "0000000000...",
      "messageType": 2,
      "messageTypeText": "REPLY",
      "format": "MQSTR",
      "characterSet": 1208,
      "encoding": 273,
      "priority": 0,
      "persistence": 0,
      "putDateTime": "2026-05-12T20:30:45Z",
      "replyToQueue": "",
      "replyToQueueManager": "",
      "userId": "app",
      "putApplicationName": "MqSender",
      "messageLength": 25,
      "text": { "orderId": 42, "items": ["a", "b"] }
    }
  ]
}
```

Поле `text` парсится как JSON-объект (так как сервис рассчитан на JSON-payload). Если содержимое сообщения не распарсилось как JSON — возвращается строкой.

### POST /messages/{queue}

Положить JSON-сообщение в очередь. Тело запроса — payload (валидный JSON). Параметры MQMD передаются через опциональные HTTP-заголовки `X-MQ-*`.

| Заголовок | Тип | Дефолт | Назначение |
|---|---|---|---|
| `X-MQ-Message-Type` | `REPLY` / `REQUEST` / `DATAGRAM` / `REPORT` или число | `REPLY` (2) | MQMD MsgType |
| `X-MQ-Format` | строка до 8 символов | `MQSTR` | MQMD Format |
| `X-MQ-Character-Set` | число (CCSID) | `1208` (UTF-8) | MQMD CodedCharSetId |
| `X-MQ-Encoding` | число | платформенный | MQMD Encoding |
| `X-MQ-Correlation-Id` | hex-строка | нули | MQMD CorrelId |
| `X-MQ-Message-Id` | hex-строка | сгенерирует MQ | MQMD MsgId |
| `X-MQ-Reply-To-Queue` | строка | пусто | MQMD ReplyToQ |
| `X-MQ-Reply-To-Queue-Manager` | строка | пусто | MQMD ReplyToQMgr |
| `X-MQ-Priority` | число (0–9) | из определения очереди | MQMD Priority |
| `X-MQ-Persistence` | `0` или `1` | из определения очереди | MQMD Persistence |
| `X-MQ-Expiry` | число (в десятых долях секунды, `-1` = безлимит) | безлимит | MQMD Expiry |

Пример:

```
POST /messages/DEV.QUEUE.1
Content-Type: application/json
X-MQ-Message-Type: REPLY
X-MQ-Correlation-Id: 414D5120514D31202020202020202020A1B2C3D4
X-MQ-Reply-To-Queue: DEV.REPLY.Q
X-MQ-Priority: 5

{ "orderId": 42, "status": "OK" }
```

Ответ (`201 Created`):

```json
{ "queue": "DEV.QUEUE.1", "messageId": "414D5120..." }
```

Возможные ошибки:
- `400 Bad Request` — пустое тело, невалидный JSON или некорректное значение заголовка.
- `500 Internal Server Error` — ошибка MQ (содержит `CC` и `RC` коды).

### DELETE /messages/{queue}

Очистить очередь — destructive get всех сообщений. Выполняется без syncpoint (каждое удаление коммитится сразу), чтобы не копить большую транзакцию на очередях с десятками тысяч сообщений.

Ответ:

```json
{ "queue": "DEV.QUEUE.1", "purged": 42 }
```

## Структура

```
app/src/main/java/ru/ds/
├── MqApi.java        — точка входа: Javalin + эндпоинты
├── MqReader.java     — сервис чтения (browseAll, purge)
├── MqSender.java     — сервис отправки (send с SendOptions)
├── MessageDto.java   — DTO одного сообщения
├── SendOptions.java  — параметры MQMD для отправки
└── MqArgs.java       — парсер CLI-аргументов
```

Поток вызова: HTTP-запрос → handler в `MqApi` → `MqReader` / `MqSender` → IBM MQ → JSON-ответ.
