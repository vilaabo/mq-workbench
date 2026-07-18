# mq-workbench

[![build](https://github.com/vilaabo/mq-workbench/actions/workflows/build.yml/badge.svg)](https://github.com/vilaabo/mq-workbench/actions/workflows/build.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[English](README.md) | **Русский**

HTTP-верстак для очередей **IBM MQ**: browse, put, get, purge и синхронный запрос-ответ — из Postman, curl или любого HTTP-клиента, на любой ОС. Кроссплатформенная альтернатива классической Windows-утилите RFHUtil.

Философия API: **тело сообщения = HTTP-тело** (JSON, XML, бинарь — как есть), **метаданные (MQMD, RFH2) = HTTP-заголовки `X-MQ-*`**.

## Возможности

- Browse очередей без удаления сообщений, со свойствами RFH2/JMS и превью тел
- Отправка с полным контролем MQMD и usr-свойствами RFH2 (`X-MQ-Usr-*` / `X-MQ-Properties`)
- Чтение или удаление одного сообщения по MsgId; очистка очереди без выкачивания тел по сети
- **`POST /rpc`** — полный round-trip запрос-ответ одним HTTP-вызовом: put в очередь запросов, ожидание скоррелированного ответа
- Разбор цепочек заголовков: RFH2 (включая RFH2→RFH2) и MQDLH (в dead-letter очереди видна причина как `dlh.Reason`)
- Stateless: соединение с MQ на каждый запрос — обрыв сети или рестарт менеджера очередей не требуют рестарта сервиса
- Ошибки MQ маппятся на честные HTTP-статусы с символьными именами кодов (`MQRC_UNKNOWN_OBJECT_NAME` → 404)
- **Swagger UI** на `/swagger` (спека OpenAPI 3.0 — на `/openapi.json`) — можно смотреть и дёргать все эндпоинты прямо из браузера

## Быстрый старт

Нужен JDK 21.

```bash
./gradlew shadowJar
java -jar app/build/libs/mq-workbench.jar \
  --host=localhost --port=1414 \
  --channel=DEV.APP.SVRCONN --qmgr=QM1 \
  --user=app --password=passw0rd
```

Источники конфигурации в порядке приоритета: CLI-аргументы → переменные окружения → файл `.env` в рабочей директории (см. [.env.example](.env.example)):

| CLI | Переменная | Обязательный | Дефолт | Описание |
|---|---|---|---|---|
| `--host` | `MQ_HOST` | да | — | Хост менеджера очередей |
| `--port` | `MQ_PORT` | да | — | Порт listener-а |
| `--channel` | `MQ_CHANNEL` | да | — | Server connection channel |
| `--qmgr` | `MQ_QMGR` | да | — | Имя менеджера очередей |
| `--user` | `MQ_USER` | нет | — | Пользователь (включает MQCSP-аутентификацию) |
| `--password` | `MQ_PASSWORD` | нет | — | Пароль |
| `--api-port` | `API_PORT` | нет | `8080` | Порт HTTP API |

## API

Интерактивная документация: **`/swagger`** (Swagger UI, генерируется из аннотаций `@OpenApi` при сборке; машиночитаемая спека — `/openapi.json`).

### GET /health

Проверка соединения с MQ. `200 {"status":"ok",...}` либо `502` с кодом MQ-ошибки.

### GET /queues/{queue}

Атрибуты очереди: `depth`, `maxDepth`, `maxMessageLength`, `openInputCount`, `openOutputCount`, `getInhibited`, `putInhibited`.

### GET /queues/{queue}/messages?limit=200

Browse без удаления. Возвращает `queue`, `depth`, `returned`, `moreAvailable` и `messages[]` — MQMD-поля, `properties` и `bodyPreview` (первые 2000 символов текста; для бинарных тел — `null`).

Тела ограничены 256 КБ на сообщение (флаг `bodyTruncated`) и 64 МБ на ответ — глубокая очередь не положит сервис. Полное тело отдаёт единичный GET.

### GET /queues/{queue}/messages/{msgId}

Одно сообщение по MsgId (hex), **без удаления**. Тело сообщения — телом ответа (текст перекодируется в UTF-8, `Content-Type` определяется по содержимому; бинарь — `application/octet-stream`; `?raw=true` — без перекодировки). Метаданные — в заголовках ответа:

```
X-MQ-Message-Id, X-MQ-Correlation-Id, X-MQ-Message-Type, X-MQ-Format (формат тела),
X-MQ-Mqmd-Format, X-MQ-Character-Set, X-MQ-Encoding, X-MQ-Priority, X-MQ-Persistence,
X-MQ-Expiry, X-MQ-Put-DateTime, X-MQ-Reply-To-Queue, X-MQ-Reply-To-Queue-Manager,
X-MQ-User-Id, X-MQ-Put-Appl-Name, X-MQ-Usr-<имя> (каждое usr-свойство),
X-MQ-Properties (все свойства одним JSON-объектом)
```

### DELETE /queues/{queue}/messages/{msgId}

То же самое, но сообщение **забирается из очереди** (destructive get). Ответ — само сообщение.

### POST /queues/{queue}/messages

Положить сообщение. Тело запроса → тело сообщения:

- текст (JSON/XML/plain) перекодируется из кодировки запроса в целевой CCSID (`X-MQ-Character-Set`, дефолт 1208 = UTF-8);
- `Content-Type: application/octet-stream` или `X-MQ-Format: NONE` — байты уходят как есть.

Опциональные заголовки:

| Заголовок | Значение | Дефолт |
|---|---|---|
| `X-MQ-Message-Type` | `DATAGRAM` / `REQUEST` / `REPLY` / `REPORT` или число | `DATAGRAM` |
| `X-MQ-Format` | формат **тела** (до 8 символов, `NONE` = бинарь) | `MQSTR` |
| `X-MQ-Character-Set` | CCSID тела | `1208` |
| `X-MQ-Encoding` | MQMD Encoding | платформенный |
| `X-MQ-Message-Id` / `X-MQ-Correlation-Id` | hex до 48 символов (дополняется нулями до 24 байт) | генерирует MQ / нули |
| `X-MQ-Reply-To-Queue` / `X-MQ-Reply-To-Queue-Manager` | строка | пусто |
| `X-MQ-Priority` | 0–9 | из очереди |
| `X-MQ-Persistence` | 0 / 1 / 2 | из очереди |
| `X-MQ-Expiry` | десятые доли секунды, `-1` = безлимит | `-1` |
| `X-MQ-Usr-<имя>` | usr-свойство RFH2 | — |
| `X-MQ-Properties` | usr-свойства одним JSON-объектом | — |

Ответ `201`: `{"queue","messageId","correlationId"}`.

Замечание: MQ сам отклоняет `X-MQ-Message-Type: REQUEST` без `X-MQ-Reply-To-Queue` (RC 2027 `MQRC_MISSING_REPLY_TO_Q`).

### DELETE /queues/{queue}/messages

Очистить очередь. Тела сообщений **не передаются по сети** (truncated get с нулевым буфером) — очистка десятков тысяч сообщений занимает секунды. Ответ: `{"queue","purged","remaining"}`.

### POST /rpc?requestQueue=A&replyQueue=B&timeoutSeconds=30&correlation=msgId

Синхронный запрос-ответ одним вызовом — для интеграций, которые читают запрос из одной очереди и кладут ответ в другую:

1. Тело и `X-MQ-*` заголовки (как в POST) уходят сообщением в `requestQueue`; `ReplyToQ` автоматически = `replyQueue`, тип по умолчанию `REQUEST`.
2. Сервис ждёт (до `timeoutSeconds`, максимум 300) сообщение в `replyQueue` с подходящим `CorrelId` и возвращает его как единичный GET (+ заголовок `X-MQ-Request-Message-Id`).

`correlation=msgId` (дефолт, стандарт MQ) — ответ ищется по `CorrelId = MsgId запроса`; `correlation=corrId` (passthrough) — по `CorrelId = CorrelId запроса` (обязателен `X-MQ-Correlation-Id`; запрос уходит с `MQRO_PASS_CORREL_ID`, чтобы report-honoring респондеры ответили правильно).

Таймаут → `504`; сам запрос к этому моменту уже лежит в `requestQueue` (его MsgId — в `X-MQ-Request-Message-Id`).

## RFH2 и свойства сообщений (usr.*)

- **Чтение**: свойства сообщений принудительно материализуются как RFH2 (`MQGMO_PROPERTIES_FORCE_MQRFH2`) и разбираются сервисом — независимо от того, положены они JMS-приложением, IIB/ACE или физическим RFH2. Поля папки `usr` отдаются под своими именами (`operation`), поля других папок — с префиксом (`jms.Dst`). Формат/CCSID/encoding тела берутся из RFH2, тело отдаётся уже без заголовка.
- **Запись**: любой заголовок `X-MQ-Usr-<имя>` и/или `X-MQ-Properties: {"имя":"значение"}` добавляет сообщению RFH2 с usr-папкой. Ключи с точкой (`jms.Dst`) раскладываются обратно в родные папки — реплей сбраузенного сообщения сохраняет структуру.
- **Регистр**: имена свойств в MQ регистрозависимы (`operation` ≠ `Operation`), а имена HTTP-заголовков — нет, и некоторые клиенты их нормализуют (Python `urllib` превратит `X-MQ-Usr-operation` в `X-Mq-Usr-Operation`). Postman и curl регистр сохраняют. Если регистр критичен — используйте `X-MQ-Properties`: JSON в **значении** заголовка выживает всегда.
- Не-ASCII значения свойств в заголовках ответа URL-кодируются; какие именно — перечислено в `X-MQ-Encoded-Headers`.

## Маппинг ошибок

Все ошибки — JSON `{"error","details","mqCompletionCode","mqReasonCode","cause"}`, где `details` — символьное имя кода.

| MQ reason | HTTP |
|---|---|
| 2085 нет такой очереди, 2033 нет такого сообщения | 404 |
| 2035 нет прав | 403 |
| 2042 занята эксклюзивно, 2016/2051 get/put запрещён, 2053 очередь полна | 409 |
| 2030/2031/2218 сообщение больше лимита | 413 |
| 2043/2045/2068 неподходящий тип объекта (remote/alias), 2142 битый заголовок | 400 |
| 2009/2058/2059/2537/2538/2540 проблемы соединения/канала/QM | 502 |
| таймаут RPC | 504 |

## Тестирование

E2E-набор (43 проверки: put/browse/get/delete/purge, RFH2 usr-свойства, RPC round-trip и таймаут, бинарные тела, UTF-8/UTF-16, лимиты, коды ошибок) гоняется против IBM MQ Advanced for Developers в Docker:

```bash
docker run -d --name mq --platform linux/amd64 -e LICENSE=accept -e MQ_QMGR_NAME=QM1 \
  -e MQ_APP_PASSWORD=passw0rd -p 11414:1414 icr.io/ibm-messaging/mq:latest
java -jar app/build/libs/mq-workbench.jar --host=localhost --port=11414 \
  --channel=DEV.APP.SVRCONN --qmgr=QM1 --user=app --password=passw0rd --api-port=18080
```

## Планы

- **WireMock-респондер**: фоновый слушатель очереди запросов, который маршрутизирует каждое сообщение по свойству `usr.operation` в стаб WireMock (`POST {wiremock}/mq/{operation}`) и кладёт ответ стаба в очередь ответов с правильной корреляцией — превращая верстак в полноценную заглушку MQ-интеграций. Управление через `POST/GET/DELETE /responders`.

## Структура

```
app/src/main/java/me/waldemar/
├── MqApi.java               — HTTP-слой: маршруты, X-MQ-* заголовки, маппинг ошибок
├── MqService.java           — операции: browse/get/put/purge/rpc/queueInfo
├── Messages.java            — кодек MQMessage: MQMD, цепочки RFH2/MQDLH, кодировки
├── MqConnectionFactory.java — соединение на каждый запрос (+ MQCSP-аутентификация)
├── AppConfig.java           — конфиг из CLI / env / .env
├── StoredMessage.java       — прочитанное сообщение
├── PutOptions.java          — параметры отправки
└── MqArgs.java              — парсер CLI-аргументов
```
