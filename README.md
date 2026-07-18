# mq-workbench

[![build](https://github.com/vilaabo/mq-workbench/actions/workflows/build.yml/badge.svg)](https://github.com/vilaabo/mq-workbench/actions/workflows/build.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**English** | [Русский](README.ru.md)

HTTP workbench for **IBM MQ** queues: browse, put, get, purge and synchronous request-reply — from Postman, curl, or any HTTP client, on any OS. A cross-platform alternative to the classic Windows-only RFHUtil.

The API philosophy: **message body = HTTP body** (JSON, XML, binary — as is), **metadata (MQMD, RFH2) = `X-MQ-*` HTTP headers**.

## Features

- Browse queues without consuming messages, with RFH2/JMS properties and body previews
- Put messages with full MQMD control and RFH2 `usr` properties (`X-MQ-Usr-*` / `X-MQ-Properties`)
- Fetch or delete a single message by MsgId; purge a queue without transferring bodies over the network
- **`POST /rpc`** — a full request-reply round-trip in one HTTP call: put to the request queue, wait for the correlated reply
- Unwraps header chains: RFH2 (including RFH2→RFH2) and MQDLH (dead-letter queues show the reason as `dlh.Reason`)
- Stateless: a fresh MQ connection per request — network drops or queue manager restarts never require a service restart
- MQ errors mapped to honest HTTP statuses with symbolic reason names (`MQRC_UNKNOWN_OBJECT_NAME` → 404)

## Quick start

Requires JDK 21.

```bash
./gradlew shadowJar
java -jar app/build/libs/mq-workbench.jar \
  --host=localhost --port=1414 \
  --channel=DEV.APP.SVRCONN --qmgr=QM1 \
  --user=app --password=passw0rd
```

Configuration sources, in priority order: CLI arguments → environment variables → `.env` file in the working directory (see [.env.example](.env.example)):

| CLI | Env var | Required | Default | Description |
|---|---|---|---|---|
| `--host` | `MQ_HOST` | yes | — | Queue manager host |
| `--port` | `MQ_PORT` | yes | — | Listener port |
| `--channel` | `MQ_CHANNEL` | yes | — | Server connection channel |
| `--qmgr` | `MQ_QMGR` | yes | — | Queue manager name |
| `--user` | `MQ_USER` | no | — | User (enables MQCSP authentication) |
| `--password` | `MQ_PASSWORD` | no | — | Password |
| `--api-port` | `API_PORT` | no | `8080` | HTTP API port |

## API

### GET /health

Checks the MQ connection. `200 {"status":"ok",...}` or `502` with the MQ reason code.

### GET /queues/{queue}

Queue attributes: `depth`, `maxDepth`, `maxMessageLength`, `openInputCount`, `openOutputCount`, `getInhibited`, `putInhibited`.

### GET /queues/{queue}/messages?limit=200

Non-destructive browse. Returns `queue`, `depth`, `returned`, `moreAvailable` and `messages[]` — MQMD fields, `properties`, and `bodyPreview` (first 2000 chars of text; `null` for binary bodies) per message.

Bodies are capped at 256 KB per message (`bodyTruncated` flag) and 64 MB per response — a deep queue cannot exhaust the service. The full body is served by the single-message endpoint.

### GET /queues/{queue}/messages/{msgId}

One message by MsgId (hex), **without consuming it**. The message body is the response body (text is re-encoded to UTF-8, `Content-Type` sniffed from content; binary comes as `application/octet-stream`; `?raw=true` skips re-encoding). Metadata comes as response headers:

```
X-MQ-Message-Id, X-MQ-Correlation-Id, X-MQ-Message-Type, X-MQ-Format (body format),
X-MQ-Mqmd-Format, X-MQ-Character-Set, X-MQ-Encoding, X-MQ-Priority, X-MQ-Persistence,
X-MQ-Expiry, X-MQ-Put-DateTime, X-MQ-Reply-To-Queue, X-MQ-Reply-To-Queue-Manager,
X-MQ-User-Id, X-MQ-Put-Appl-Name, X-MQ-Usr-<name> (each usr property),
X-MQ-Properties (all properties as one JSON object)
```

### DELETE /queues/{queue}/messages/{msgId}

Same, but the message is **consumed** (destructive get). The response is the message itself.

### POST /queues/{queue}/messages

Put a message. Request body → message body:

- text (JSON/XML/plain) is transcoded from the request charset into the target CCSID (`X-MQ-Character-Set`, default 1208 = UTF-8);
- `Content-Type: application/octet-stream` or `X-MQ-Format: NONE` — bytes go through untouched.

Optional headers:

| Header | Value | Default |
|---|---|---|
| `X-MQ-Message-Type` | `DATAGRAM` / `REQUEST` / `REPLY` / `REPORT` or a number | `DATAGRAM` |
| `X-MQ-Format` | **body** format (8 chars max, `NONE` = binary) | `MQSTR` |
| `X-MQ-Character-Set` | body CCSID | `1208` |
| `X-MQ-Encoding` | MQMD Encoding | platform |
| `X-MQ-Message-Id` / `X-MQ-Correlation-Id` | hex up to 48 chars (zero-padded to 24 bytes) | MQ-generated / zeros |
| `X-MQ-Reply-To-Queue` / `X-MQ-Reply-To-Queue-Manager` | string | empty |
| `X-MQ-Priority` | 0–9 | queue default |
| `X-MQ-Persistence` | 0 / 1 / 2 | queue default |
| `X-MQ-Expiry` | tenths of a second, `-1` = unlimited | `-1` |
| `X-MQ-Usr-<name>` | RFH2 usr property | — |
| `X-MQ-Properties` | usr properties as one JSON object | — |

Response `201`: `{"queue","messageId","correlationId"}`.

Note: MQ itself rejects `X-MQ-Message-Type: REQUEST` without `X-MQ-Reply-To-Queue` (RC 2027 `MQRC_MISSING_REPLY_TO_Q`).

### DELETE /queues/{queue}/messages

Purge the queue. Message bodies are **not transferred over the network** (truncated get with a zero buffer), so clearing tens of thousands of messages takes seconds. Response: `{"queue","purged","remaining"}`.

### POST /rpc?requestQueue=A&replyQueue=B&timeoutSeconds=30&correlation=msgId

Synchronous request-reply in one call — for integrations that read requests from one queue and reply into another:

1. The body and `X-MQ-*` headers (same as POST) are sent to `requestQueue`; `ReplyToQ` is set to `replyQueue` automatically, message type defaults to `REQUEST`.
2. The service waits (up to `timeoutSeconds`, max 300) for a message in `replyQueue` with the matching `CorrelId` and returns it like the single-message GET (+ the `X-MQ-Request-Message-Id` header).

`correlation=msgId` (default, the MQ standard) matches the reply by `CorrelId = request MsgId`; `correlation=corrId` (passthrough) matches by `CorrelId = request CorrelId` (requires `X-MQ-Correlation-Id`; the request is sent with `MQRO_PASS_CORREL_ID` so report-honoring responders reply correctly).

Timeout → `504`; the request is already in `requestQueue` by then (its MsgId is in `X-MQ-Request-Message-Id`).

## RFH2 and message properties (usr.*)

- **Reading**: message properties are forced into RFH2 form (`MQGMO_PROPERTIES_FORCE_MQRFH2`) and parsed by the service — whether they were set by a JMS app, IIB/ACE, or a physical RFH2. `usr` folder fields come under their own names (`operation`); other folders are prefixed (`jms.Dst`). The body format/CCSID/encoding are taken from the RFH2, and the body is served without the header.
- **Writing**: any `X-MQ-Usr-<name>` header and/or `X-MQ-Properties: {"name":"value"}` adds an RFH2 with a usr folder. Dotted keys (`jms.Dst`) are written back into their native folders — replaying a browsed message preserves folder placement.
- **Case sensitivity**: MQ property names are case-sensitive (`operation` ≠ `Operation`), while HTTP header **names** are not — and some clients normalize them (Python's `urllib` turns `X-MQ-Usr-operation` into `X-Mq-Usr-Operation`). Postman and curl preserve case. When case matters, use `X-MQ-Properties`: JSON in a header **value** always survives intact.
- Non-ASCII property values in response headers are URL-encoded; the `X-MQ-Encoded-Headers` header lists which ones.

## Error mapping

All errors are JSON: `{"error","details","mqCompletionCode","mqReasonCode","cause"}`, where `details` is the symbolic reason name.

| MQ reason | HTTP |
|---|---|
| 2085 no such queue, 2033 no such message | 404 |
| 2035 not authorized | 403 |
| 2042 exclusive use, 2016/2051 get/put inhibited, 2053 queue full | 409 |
| 2030/2031/2218 message too big | 413 |
| 2043/2045/2068 wrong object type (remote/alias queue), 2142 malformed header | 400 |
| 2009/2058/2059/2537/2538/2540 connection/channel/QM issues | 502 |
| RPC timeout | 504 |

## Testing

The e2e suite (43 checks: put/browse/get/delete/purge, RFH2 usr properties, RPC round-trip and timeout, binary bodies, UTF-8/UTF-16, limits, error codes) runs against IBM MQ Advanced for Developers in Docker:

```bash
docker run -d --name mq --platform linux/amd64 -e LICENSE=accept -e MQ_QMGR_NAME=QM1 \
  -e MQ_APP_PASSWORD=passw0rd -p 11414:1414 icr.io/ibm-messaging/mq:latest
java -jar app/build/libs/mq-workbench.jar --host=localhost --port=11414 \
  --channel=DEV.APP.SVRCONN --qmgr=QM1 --user=app --password=passw0rd --api-port=18080
```

## Roadmap

- **WireMock-backed responder**: a background listener on a request queue that routes each message by its `usr.operation` property to a WireMock stub (`POST {wiremock}/mq/{operation}`) and puts the stub's response into the reply queue with proper correlation — turning the workbench into a full MQ integration stub. Managed via `POST/GET/DELETE /responders`.

## Project layout

```
app/src/main/java/me/waldemar/
├── MqApi.java               — HTTP layer: routes, X-MQ-* headers, error mapping
├── MqService.java           — operations: browse/get/put/purge/rpc/queueInfo
├── Messages.java            — MQMessage codec: MQMD, RFH2/MQDLH chains, CCSID handling
├── MqConnectionFactory.java — per-request connections (+ MQCSP authentication)
├── AppConfig.java           — config from CLI / env / .env
├── StoredMessage.java       — a parsed message
├── PutOptions.java          — put parameters
└── MqArgs.java              — CLI argument parser
```
