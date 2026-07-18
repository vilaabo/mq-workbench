package me.waldemar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.MQConstants;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.json.JavalinJackson;
import io.javalin.openapi.HttpMethod;
import io.javalin.openapi.OpenApi;
import io.javalin.openapi.OpenApiContent;
import io.javalin.openapi.OpenApiParam;
import io.javalin.openapi.OpenApiRequestBody;
import io.javalin.openapi.OpenApiResponse;
import io.javalin.openapi.plugin.OpenApiPlugin;
import io.javalin.openapi.plugin.swagger.SwaggerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** HTTP-слой: маршруты, разбор X-MQ-* заголовков, маппинг ошибок MQ на HTTP-статусы. */
public class MqApi {

    private static final Logger log = LoggerFactory.getLogger(MqApi.class);

    private static final int DEFAULT_BROWSE_LIMIT = 200;
    private static final int MAX_BROWSE_LIMIT = 1_000;
    private static final int PREVIEW_CHARS = 2_000;
    private static final int DEFAULT_RPC_TIMEOUT_SECONDS = 30;
    private static final int MAX_RPC_TIMEOUT_SECONDS = 300;
    private static final String USR_HEADER_PREFIX = "X-MQ-Usr-";
    private static final String PROPERTIES_HEADER = "X-MQ-Properties";

    /** JSON для значений HTTP-заголовков: только ASCII, не-ASCII символы экранируются \\uXXXX. */
    private static final ObjectMapper HEADER_JSON =
            JsonMapper.builder().enable(JsonWriteFeature.ESCAPE_NON_ASCII).build();

    private final AppConfig cfg;
    private final MqService mq;

    MqApi(AppConfig cfg, MqService mq) {
        this.cfg = cfg;
        this.mq = mq;
    }

    public static void main(String[] args) {
        AppConfig cfg;
        try {
            cfg = AppConfig.load(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println("Использование: --host=... --port=... --channel=... --qmgr=... "
                    + "[--user=... --password=... --api-port=8080]");
            System.err.println("Те же параметры читаются из переменных окружения MQ_HOST/MQ_PORT/MQ_CHANNEL/"
                    + "MQ_QMGR/MQ_USER/MQ_PASSWORD/API_PORT и файла .env в текущей директории.");
            System.exit(2);
            return;
        }

        MqApi api = new MqApi(cfg, new MqService(new MqConnectionFactory(cfg)));

        Javalin app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.useVirtualThreads = true; // RPC блокирует поток до 300 с — Loom делает это дешёвым
            config.http.maxRequestSize = 32L * 1024 * 1024;
            config.jsonMapper(new JavalinJackson().updateMapper(m -> m.enable(SerializationFeature.INDENT_OUTPUT)));
            config.registerPlugin(new OpenApiPlugin(openApi -> openApi
                    .withDocumentationPath("/openapi.json")
                    .withDefinitionConfiguration((version, definition) -> definition
                            .withInfo(info -> {
                                info.setTitle("mq-workbench");
                                info.setVersion("1.1.0");
                                info.setDescription("HTTP workbench for IBM MQ queues: browse, put, get, purge "
                                        + "and synchronous request-reply. Message body = HTTP body, "
                                        + "MQMD/RFH2 metadata = X-MQ-* headers.");
                            }))));
            config.registerPlugin(new SwaggerPlugin(swagger -> {
                swagger.setDocumentationPath("/openapi.json");
                swagger.setUiPath("/swagger");
            }));
        });

        app.get("/health", api::health);
        app.get("/queues/{queue}", api::queueInfo);
        app.get("/queues/{queue}/messages", api::browse);
        app.post("/queues/{queue}/messages", api::put);
        app.get("/queues/{queue}/messages/{messageId}", api::getMessage);
        app.delete("/queues/{queue}/messages/{messageId}", api::consumeMessage);
        app.delete("/queues/{queue}/messages", api::purge);
        app.post("/rpc", api::rpc);

        app.exception(BadInput.class, (e, ctx) -> {
            ctx.status(HttpStatus.BAD_REQUEST);
            ctx.json(new ErrorResponse("Bad request", e.getMessage(), null, null, null));
        });

        app.exception(MQException.class, (e, ctx) -> {
            String reason = MQConstants.lookup(e.reasonCode, "MQRC_.*");
            ctx.status(statusFor(e.reasonCode));
            ctx.json(new ErrorResponse("MQ error", reason, e.completionCode, e.reasonCode,
                    e.getCause() == null ? null : e.getCause().getMessage()));
            log.warn("{} {}: {} (CC={} RC={})", ctx.method(), ctx.path(), reason, e.completionCode, e.reasonCode);
        });

        app.exception(IOException.class, (e, ctx) -> {
            ctx.status(HttpStatus.BAD_REQUEST);
            ctx.json(new ErrorResponse("I/O error", e.toString(), null, null, null));
        });

        app.exception(Exception.class, (e, ctx) -> {
            log.error("Необработанная ошибка на {} {}", ctx.method(), ctx.path(), e);
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
            ctx.json(new ErrorResponse("Internal error", e.toString(), null, null, null));
        });

        app.start(cfg.apiPort());

        log.info("MQ HTTP API: {} @ {}:{} (канал {}), API-порт {}",
                cfg.qmgr(), cfg.host(), cfg.port(), cfg.channel(), cfg.apiPort());
        log.info("  Swagger UI: http://localhost:{}/swagger (спека: /openapi.json)", cfg.apiPort());
        log.info("  GET    /health");
        log.info("  GET    /queues/{{queue}}                     — атрибуты очереди (глубина и т.д.)");
        log.info("  GET    /queues/{{queue}}/messages?limit=200  — browse без удаления");
        log.info("  POST   /queues/{{queue}}/messages            — положить сообщение (X-MQ-* заголовки)");
        log.info("  GET    /queues/{{queue}}/messages/{{msgId}}  — одно сообщение (browse)");
        log.info("  DELETE /queues/{{queue}}/messages/{{msgId}}  — забрать одно сообщение (destructive)");
        log.info("  DELETE /queues/{{queue}}/messages            — очистить очередь");
        log.info("  POST   /rpc?requestQueue=A&replyQueue=B      — запрос-ответ одним вызовом");
    }

    // ---------- хендлеры ----------

    @OpenApi(
            path = "/health",
            methods = HttpMethod.GET,
            summary = "Проверка соединения с queue manager",
            tags = "service",
            responses = {
                    @OpenApiResponse(status = "200", description = "Соединение установлено"),
                    @OpenApiResponse(status = "502", description = "Queue manager недоступен",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void health(Context ctx) throws MQException, IOException {
        mq.ping();
        ctx.json(Map.of("status", "ok", "qmgr", cfg.qmgr(), "host", cfg.host() + ":" + cfg.port()));
    }

    @OpenApi(
            path = "/queues/{queue}",
            methods = HttpMethod.GET,
            summary = "Атрибуты очереди",
            description = "Глубина, лимиты, счётчики открытий, inhibit-флаги. Только локальные очереди "
                    + "(remote/alias дают 400).",
            tags = "queues",
            pathParams = @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
            responses = {
                    @OpenApiResponse(status = "200", content = @OpenApiContent(from = QueueInfoResponse.class)),
                    @OpenApiResponse(status = "404", description = "Нет такой очереди",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void queueInfo(Context ctx) throws MQException, IOException {
        String queue = ctx.pathParam("queue");
        MqService.QueueInfo info = mq.queueInfo(queue);
        ctx.json(new QueueInfoResponse(queue, info.depth(), info.maxDepth(), info.maxMessageLength(),
                info.openInputCount(), info.openOutputCount(), info.getInhibited(), info.putInhibited()));
    }

    @OpenApi(
            path = "/queues/{queue}/messages",
            methods = HttpMethod.GET,
            summary = "Browse очереди без удаления сообщений",
            description = "MQMD-поля, RFH2/JMS-свойства (properties) и превью тел. Тела ограничены 256 КБ "
                    + "на сообщение (bodyTruncated) и 64 МБ на ответ; порядок — как отдаёт MQ "
                    + "(при MSGDLVSQ(PRIORITY) приоритетные раньше).",
            tags = "messages",
            pathParams = @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
            queryParams = @OpenApiParam(name = "limit", type = Integer.class,
                    description = "Максимум сообщений в ответе (1..1000, дефолт 200)"),
            responses = {
                    @OpenApiResponse(status = "200", content = @OpenApiContent(from = BrowseResponse.class)),
                    @OpenApiResponse(status = "404", description = "Нет такой очереди",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void browse(Context ctx) throws MQException, IOException {
        String queue = ctx.pathParam("queue");
        int limit = intQuery(ctx, "limit", DEFAULT_BROWSE_LIMIT, 1, MAX_BROWSE_LIMIT);
        MqService.BrowseResult r = mq.browse(queue, limit);
        List<MessageSummary> messages = new ArrayList<>(r.messages().size());
        for (StoredMessage m : r.messages()) messages.add(toSummary(m));
        ctx.json(new BrowseResponse(queue, r.depth(), messages.size(), r.moreAvailable(), messages));
        log.info("BROWSE {}: {} из {}", queue, messages.size(), r.depth());
    }

    @OpenApi(
            path = "/queues/{queue}/messages",
            methods = HttpMethod.POST,
            summary = "Положить сообщение в очередь",
            description = "Тело запроса → тело сообщения (текст перекодируется в X-MQ-Character-Set, "
                    + "application/octet-stream или X-MQ-Format: NONE — байты как есть). "
                    + "usr-свойства RFH2 задаются заголовками X-MQ-Usr-<имя> или X-MQ-Properties "
                    + "(JSON-объект; сохраняет регистр имён — предпочтителен для автоматизации). "
                    + "Ключи с точкой (jms.Dst) уходят в родные папки RFH2.",
            tags = "messages",
            pathParams = @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
            headers = {
                    @OpenApiParam(name = "X-MQ-Message-Type",
                            description = "DATAGRAM (дефолт) / REQUEST / REPLY / REPORT или число"),
                    @OpenApiParam(name = "X-MQ-Format", description = "Формат тела (до 8 символов; NONE = бинарь), дефолт MQSTR"),
                    @OpenApiParam(name = "X-MQ-Character-Set", type = Integer.class, description = "CCSID тела, дефолт 1208"),
                    @OpenApiParam(name = "X-MQ-Encoding", type = Integer.class, description = "MQMD Encoding"),
                    @OpenApiParam(name = "X-MQ-Message-Id", description = "hex до 48 символов"),
                    @OpenApiParam(name = "X-MQ-Correlation-Id", description = "hex до 48 символов"),
                    @OpenApiParam(name = "X-MQ-Reply-To-Queue", description = "MQMD ReplyToQ (обязателен для REQUEST)"),
                    @OpenApiParam(name = "X-MQ-Reply-To-Queue-Manager", description = "MQMD ReplyToQMgr"),
                    @OpenApiParam(name = "X-MQ-Priority", type = Integer.class, description = "0–9"),
                    @OpenApiParam(name = "X-MQ-Persistence", type = Integer.class, description = "0 / 1 / 2"),
                    @OpenApiParam(name = "X-MQ-Expiry", type = Integer.class,
                            description = "Десятые доли секунды, -1 = безлимит"),
                    @OpenApiParam(name = "X-MQ-Properties",
                            description = "usr-свойства одним JSON-объектом: {\"operation\":\"CreateOrder\"}")
            },
            requestBody = @OpenApiRequestBody(description = "Тело сообщения как есть (JSON/XML/текст/бинарь)",
                    content = {
                            @OpenApiContent(mimeType = "application/json", type = "object"),
                            @OpenApiContent(mimeType = "application/xml", type = "string"),
                            @OpenApiContent(mimeType = "text/plain", type = "string"),
                            @OpenApiContent(mimeType = "application/octet-stream", type = "string", format = "binary")
                    }),
            responses = {
                    @OpenApiResponse(status = "201", content = @OpenApiContent(from = PutResponse.class)),
                    @OpenApiResponse(status = "400", description = "Невалидные заголовки",
                            content = @OpenApiContent(from = ErrorResponse.class)),
                    @OpenApiResponse(status = "413", description = "Сообщение больше MAXMSGL очереди/канала",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void put(Context ctx) throws MQException, IOException {
        String queue = ctx.pathParam("queue");
        PutOptions opts = parsePutOptions(ctx);
        byte[] body = requestBodyBytes(ctx, opts);
        MqService.PutResult r = mq.put(queue, Messages.build(body, opts));
        ctx.status(HttpStatus.CREATED);
        ctx.json(new PutResponse(queue, Messages.hex(r.messageId()), Messages.hex(r.correlationId())));
        log.info("PUT {}: msgId={}", queue, Messages.hex(r.messageId()));
    }

    @OpenApi(
            path = "/queues/{queue}/messages/{messageId}",
            methods = HttpMethod.GET,
            summary = "Одно сообщение по MsgId, без удаления",
            description = "Тело сообщения — телом ответа (текст перекодируется в UTF-8; ?raw=true — байты "
                    + "без перекодировки). MQMD и свойства — в заголовках ответа X-MQ-* "
                    + "(X-MQ-Message-Id, X-MQ-Correlation-Id, X-MQ-Format, X-MQ-Usr-<имя>, X-MQ-Properties и др.).",
            tags = "messages",
            pathParams = {
                    @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
                    @OpenApiParam(name = "messageId", description = "MsgId в hex (48 символов)", required = true)
            },
            queryParams = @OpenApiParam(name = "raw", type = Boolean.class,
                    description = "true — тело без перекодировки, application/octet-stream"),
            responses = {
                    @OpenApiResponse(status = "200", description = "Тело сообщения; метаданные в X-MQ-* заголовках",
                            content = @OpenApiContent(mimeType = "*/*")),
                    @OpenApiResponse(status = "404", description = "Нет сообщения с таким MsgId",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void getMessage(Context ctx) throws MQException, IOException {
        StoredMessage m = mq.getByMessageId(ctx.pathParam("queue"),
                parseId(ctx.pathParam("messageId")), false);
        respondMessage(ctx, m);
    }

    @OpenApi(
            path = "/queues/{queue}/messages/{messageId}",
            methods = HttpMethod.DELETE,
            summary = "Забрать одно сообщение по MsgId (destructive get)",
            description = "Ответ — само сообщение, в том же виде, что и GET; сообщение удаляется из очереди.",
            tags = "messages",
            pathParams = {
                    @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
                    @OpenApiParam(name = "messageId", description = "MsgId в hex (48 символов)", required = true)
            },
            queryParams = @OpenApiParam(name = "raw", type = Boolean.class,
                    description = "true — тело без перекодировки, application/octet-stream"),
            responses = {
                    @OpenApiResponse(status = "200", description = "Забранное сообщение",
                            content = @OpenApiContent(mimeType = "*/*")),
                    @OpenApiResponse(status = "404", description = "Нет сообщения с таким MsgId",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void consumeMessage(Context ctx) throws MQException, IOException {
        String queue = ctx.pathParam("queue");
        StoredMessage m = mq.getByMessageId(queue, parseId(ctx.pathParam("messageId")), true);
        respondMessage(ctx, m);
        log.info("GET(destructive) {}: msgId={}", queue, Messages.hex(m.messageId()));
    }

    @OpenApi(
            path = "/queues/{queue}/messages",
            methods = HttpMethod.DELETE,
            summary = "Очистить очередь",
            description = "Destructive get всех сообщений без передачи тел по сети (truncated get). "
                    + "Число итераций ограничено глубиной на момент старта; докинутое во время очистки "
                    + "видно в remaining.",
            tags = "messages",
            pathParams = @OpenApiParam(name = "queue", description = "Имя очереди", required = true),
            responses = {
                    @OpenApiResponse(status = "200", content = @OpenApiContent(from = PurgeResponse.class)),
                    @OpenApiResponse(status = "409", description = "Очередь занята (exclusive) или get запрещён",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void purge(Context ctx) throws MQException, IOException {
        String queue = ctx.pathParam("queue");
        MqService.PurgeResult r = mq.purge(queue);
        ctx.json(new PurgeResponse(queue, r.purged(), r.remaining()));
        log.info("PURGE {}: purged={} remaining={}", queue, r.purged(), r.remaining());
    }

    @OpenApi(
            path = "/rpc",
            methods = HttpMethod.POST,
            summary = "Синхронный запрос-ответ через пару очередей",
            description = "Кладёт сообщение (тело + X-MQ-* заголовки, как в POST /queues/{queue}/messages) "
                    + "в requestQueue и ждёт скоррелированный ответ из replyQueue. ReplyToQ автоматически = "
                    + "replyQueue, тип по умолчанию REQUEST. correlation=msgId (стандарт MQ): ответ ищется по "
                    + "CorrelId = MsgId запроса; corrId (passthrough): по CorrelId запроса, требуется "
                    + "X-MQ-Correlation-Id, запрос уходит с MQRO_PASS_CORREL_ID.",
            tags = "rpc",
            queryParams = {
                    @OpenApiParam(name = "requestQueue", description = "Очередь запросов", required = true),
                    @OpenApiParam(name = "replyQueue", description = "Очередь ответов", required = true),
                    @OpenApiParam(name = "timeoutSeconds", type = Integer.class,
                            description = "Ожидание ответа, 1..300 (дефолт 30)"),
                    @OpenApiParam(name = "correlation", description = "msgId (дефолт) или corrId")
            },
            requestBody = @OpenApiRequestBody(description = "Тело запроса как есть",
                    content = {
                            @OpenApiContent(mimeType = "application/json", type = "object"),
                            @OpenApiContent(mimeType = "application/xml", type = "string"),
                            @OpenApiContent(mimeType = "text/plain", type = "string"),
                            @OpenApiContent(mimeType = "application/octet-stream", type = "string", format = "binary")
                    }),
            responses = {
                    @OpenApiResponse(status = "200",
                            description = "Ответ интеграции: тело + X-MQ-* заголовки + X-MQ-Request-Message-Id",
                            content = @OpenApiContent(mimeType = "*/*")),
                    @OpenApiResponse(status = "504", description = "Ответ не пришёл за timeoutSeconds; "
                            + "запрос уже лежит в requestQueue",
                            content = @OpenApiContent(from = ErrorResponse.class))
            })
    private void rpc(Context ctx) throws MQException, IOException {
        String requestQueue = requireQuery(ctx, "requestQueue");
        String replyQueue = requireQuery(ctx, "replyQueue");
        int timeout = intQuery(ctx, "timeoutSeconds", DEFAULT_RPC_TIMEOUT_SECONDS, 1, MAX_RPC_TIMEOUT_SECONDS);
        String correlation = ctx.queryParam("correlation");
        boolean byCorrelId = "corrId".equalsIgnoreCase(correlation);
        if (correlation != null && !byCorrelId && !"msgId".equalsIgnoreCase(correlation)) {
            throw new BadInput("correlation должен быть msgId (стандарт: CorrelId ответа = MsgId запроса) или corrId (passthrough)");
        }
        PutOptions opts = parsePutOptions(ctx);
        if (byCorrelId && opts.correlationId() == null) {
            throw new BadInput("correlation=corrId требует заголовок X-MQ-Correlation-Id");
        }
        byte[] body = requestBodyBytes(ctx, opts);
        MQMessage request = Messages.build(body, opts);
        if (opts.replyToQueue() == null) request.replyToQueueName = replyQueue;
        if (opts.messageType() == null) request.messageType = MQConstants.MQMT_REQUEST;

        MqService.RpcResult r = mq.rpc(requestQueue, replyQueue, request, byCorrelId, timeout);
        ctx.header("X-MQ-Request-Message-Id", Messages.hex(r.requestMessageId()));
        if (r.reply() == null) {
            ctx.status(HttpStatus.GATEWAY_TIMEOUT);
            ctx.json(new ErrorResponse("RPC timeout",
                    "Ответ с CorrelId=" + Messages.hex(r.correlationUsed())
                            + " не появился в " + replyQueue + " за " + timeout + " с "
                            + "(запрос уже лежит в " + requestQueue + ", msgId=" + Messages.hex(r.requestMessageId()) + ")",
                    null, null, null));
            log.info("RPC {} -> {}: таймаут {} с", requestQueue, replyQueue, timeout);
            return;
        }
        respondMessage(ctx, r.reply());
        log.info("RPC {} -> {}: ответ msgId={}", requestQueue, replyQueue, Messages.hex(r.reply().messageId()));
    }

    // ---------- разбор запроса ----------

    private static PutOptions parsePutOptions(Context ctx) {
        Map<String, String> usr = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : ctx.headerMap().entrySet()) {
            String name = e.getKey();
            if (name.regionMatches(true, 0, USR_HEADER_PREFIX, 0, USR_HEADER_PREFIX.length())
                    && name.length() > USR_HEADER_PREFIX.length()) {
                usr.put(name.substring(USR_HEADER_PREFIX.length()), e.getValue());
            }
        }
        // HTTP-клиенты вправе менять регистр ИМЁН заголовков (urllib шлёт X-Mq-Usr-Operation),
        // а имена usr-свойств в MQ регистрозависимы. X-MQ-Properties с JSON-объектом в ЗНАЧЕНИИ
        // заголовка сохраняет регистр всегда и перекрывает одноимённые X-MQ-Usr-*.
        String propsJson = ctx.header(PROPERTIES_HEADER);
        if (propsJson != null && !propsJson.isBlank()) {
            try {
                JsonNode node = HEADER_JSON.readTree(propsJson);
                if (!node.isObject()) {
                    throw new BadInput(PROPERTIES_HEADER + " должен быть JSON-объектом со строковыми значениями");
                }
                node.fields().forEachRemaining(f -> usr.put(f.getKey(),
                        f.getValue().isValueNode() ? f.getValue().asText() : f.getValue().toString()));
            } catch (JsonProcessingException e) {
                throw new BadInput(PROPERTIES_HEADER + ": невалидный JSON — " + e.getOriginalMessage());
            }
        }
        String format = ctx.header("X-MQ-Format");
        if (format != null && Messages.padFormat(format).equals(MQConstants.MQFMT_RF_HEADER_2) && !usr.isEmpty()) {
            throw new BadInput("X-MQ-Format: MQHRF2 несовместим со свойствами (X-MQ-Usr-*/X-MQ-Properties) — "
                    + "RFH2 добавляется автоматически; MQHRF2 в формате нужен, только если тело уже содержит готовый RFH2");
        }
        return new PutOptions(
                parseMessageType(ctx.header("X-MQ-Message-Type")),
                ctx.header("X-MQ-Format"),
                headerInt(ctx, "X-MQ-Character-Set"),
                headerInt(ctx, "X-MQ-Encoding"),
                headerHex(ctx, "X-MQ-Message-Id"),
                headerHex(ctx, "X-MQ-Correlation-Id"),
                ctx.header("X-MQ-Reply-To-Queue"),
                ctx.header("X-MQ-Reply-To-Queue-Manager"),
                headerInt(ctx, "X-MQ-Priority"),
                headerInt(ctx, "X-MQ-Persistence"),
                headerInt(ctx, "X-MQ-Expiry"),
                usr);
    }

    /**
     * Тело запроса. Binary (Content-Type application/octet-stream или X-MQ-Format: NONE) уходит байт-в-байт;
     * текст перекодируется из кодировки HTTP-запроса в целевой CCSID сообщения.
     */
    private static byte[] requestBodyBytes(Context ctx, PutOptions opts) {
        byte[] raw = ctx.bodyAsBytes();
        String contentType = ctx.contentType();
        boolean binary = contentType != null
                && contentType.toLowerCase(Locale.ROOT).startsWith("application/octet-stream");
        if (opts.bodyFormat() != null && Messages.padFormat(opts.bodyFormat()).equals(MQConstants.MQFMT_NONE)) {
            binary = true;
        }
        if (binary || raw.length == 0) return raw;

        Charset requestCharset = requestCharset(contentType);
        int targetCcsid = opts.characterSet() != null ? opts.characterSet() : 1208;
        Charset target = Messages.charsetFor(targetCcsid,
                opts.encoding() != null ? opts.encoding() : MQConstants.MQENC_NATIVE);
        if (requestCharset.equals(target)) return raw;
        return new String(raw, requestCharset).getBytes(target);
    }

    private static Charset requestCharset(String contentType) {
        if (contentType != null) {
            int i = contentType.toLowerCase(Locale.ROOT).indexOf("charset=");
            if (i >= 0) {
                String cs = contentType.substring(i + "charset=".length()).trim();
                int semi = cs.indexOf(';');
                if (semi >= 0) cs = cs.substring(0, semi);
                try {
                    return Charset.forName(cs.replace("\"", "").trim());
                } catch (Exception ignored) {
                    // нераспознанный charset — считаем UTF-8
                }
            }
        }
        return StandardCharsets.UTF_8;
    }

    private static Integer headerInt(Context ctx, String name) {
        String s = ctx.header(name);
        if (s == null || s.isBlank()) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new BadInput(name + " должен быть числом, получено: " + s);
        }
    }

    private static byte[] headerHex(Context ctx, String name) {
        String s = ctx.header(name);
        if (s == null || s.isBlank()) return null;
        try {
            return Messages.fromHex(s);
        } catch (IllegalArgumentException e) {
            throw new BadInput(name + " должен быть hex-строкой, получено: " + s);
        }
    }

    private static byte[] parseId(String s) {
        try {
            return Messages.padId(Messages.fromHex(s));
        } catch (IllegalArgumentException e) {
            throw new BadInput("messageId в URL должен быть hex-строкой (до 48 символов), получено: " + s);
        }
    }

    private static Integer parseMessageType(String s) {
        if (s == null || s.isBlank()) return null;
        String t = s.trim();
        try {
            return Integer.parseInt(t);
        } catch (NumberFormatException ignored) {
            // не число — пробуем символьное имя
        }
        return switch (t.toUpperCase(Locale.ROOT)) {
            case "DATAGRAM" -> MQConstants.MQMT_DATAGRAM;
            case "REQUEST" -> MQConstants.MQMT_REQUEST;
            case "REPLY" -> MQConstants.MQMT_REPLY;
            case "REPORT" -> MQConstants.MQMT_REPORT;
            default -> throw new BadInput("X-MQ-Message-Type: ожидается DATAGRAM/REQUEST/REPLY/REPORT или число, получено: " + s);
        };
    }

    private static String messageTypeName(int type) {
        return switch (type) {
            case MQConstants.MQMT_DATAGRAM -> "DATAGRAM";
            case MQConstants.MQMT_REQUEST -> "REQUEST";
            case MQConstants.MQMT_REPLY -> "REPLY";
            case MQConstants.MQMT_REPORT -> "REPORT";
            default -> String.valueOf(type);
        };
    }

    private static int intQuery(Context ctx, String name, int def, int min, int max) {
        String s = ctx.queryParam(name);
        if (s == null || s.isBlank()) return def;
        int v;
        try {
            v = Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new BadInput(name + " должен быть числом, получено: " + s);
        }
        if (v < min || v > max) throw new BadInput(name + " должен быть в диапазоне " + min + ".." + max);
        return v;
    }

    private static String requireQuery(Context ctx, String name) {
        String s = ctx.queryParam(name);
        if (s == null || s.isBlank()) throw new BadInput("Не задан обязательный query-параметр: " + name);
        return s;
    }

    // ---------- формирование ответа ----------

    /** Одно сообщение: тело — телом HTTP-ответа, MQMD и свойства — заголовками X-MQ-*. */
    private static void respondMessage(Context ctx, StoredMessage m) {
        ctx.header("X-MQ-Message-Id", Messages.hex(m.messageId()));
        ctx.header("X-MQ-Correlation-Id", Messages.hex(m.correlationId()));
        ctx.header("X-MQ-Message-Type", messageTypeName(m.messageType()));
        // X-MQ-Format симметричен POST-у: это формат ТЕЛА (копирование заголовков в новый PUT
        // воспроизводит сообщение). Формат MQMD на проводе (MQHRF2/MQDEAD) — отдельным заголовком.
        ctx.header("X-MQ-Format", m.bodyFormat());
        ctx.header("X-MQ-Mqmd-Format", m.mqmdFormat());
        ctx.header("X-MQ-Character-Set", String.valueOf(m.bodyCharacterSet()));
        ctx.header("X-MQ-Encoding", String.valueOf(m.bodyEncoding()));
        ctx.header("X-MQ-Priority", String.valueOf(m.priority()));
        ctx.header("X-MQ-Persistence", String.valueOf(m.persistence()));
        ctx.header("X-MQ-Expiry", String.valueOf(m.expiry()));
        if (m.putDateTime() != null) ctx.header("X-MQ-Put-DateTime", m.putDateTime());
        if (!m.replyToQueue().isEmpty()) ctx.header("X-MQ-Reply-To-Queue", m.replyToQueue());
        if (!m.replyToQueueManager().isEmpty()) ctx.header("X-MQ-Reply-To-Queue-Manager", m.replyToQueueManager());
        if (!m.userId().isEmpty()) ctx.header("X-MQ-User-Id", m.userId());
        if (!m.putApplicationName().isEmpty()) ctx.header("X-MQ-Put-Appl-Name", m.putApplicationName());
        if (m.bodyTruncated()) ctx.header("X-MQ-Body-Truncated", "true");
        List<String> encodedHeaders = new ArrayList<>();
        for (Map.Entry<String, String> e : m.properties().entrySet()) {
            String prefix = e.getKey().contains(".") ? "X-MQ-Prop-" : USR_HEADER_PREFIX;
            String headerName = prefix + e.getKey();
            String value = e.getValue();
            if (!isAscii(value)) {
                value = java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
                encodedHeaders.add(headerName);
            }
            ctx.header(headerName, value);
        }
        if (!encodedHeaders.isEmpty()) {
            // маркер: значения перечисленных заголовков URL-закодированы (не-ASCII свойства)
            ctx.header("X-MQ-Encoded-Headers", String.join(",", encodedHeaders));
        }
        if (!m.properties().isEmpty()) {
            try {
                // дублируем свойства одним JSON-заголовком: точные имена с регистром, независимо от клиента
                ctx.header(PROPERTIES_HEADER, HEADER_JSON.writeValueAsString(m.properties()));
            } catch (JsonProcessingException ignored) {
                // Map<String,String> сериализуется всегда; X-MQ-Usr-* заголовки уже выставлены
            }
        }

        if ("true".equalsIgnoreCase(ctx.queryParam("raw"))) {
            ctx.contentType("application/octet-stream");
            ctx.result(m.body());
            return;
        }
        String text = Messages.tryDecodeText(m.body(), m.bodyCharacterSet(), m.bodyEncoding());
        if (text == null) {
            ctx.contentType("application/octet-stream");
            ctx.result(m.body());
        } else {
            ctx.contentType(sniffContentType(text) + "; charset=utf-8");
            ctx.result(text);
        }
    }

    private static boolean isAscii(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c > 0x7E) return false;
        }
        return true;
    }

    private static String sniffContentType(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) continue;
            if (c == '{' || c == '[') return "application/json";
            if (c == '<') return "application/xml";
            break;
        }
        return "text/plain";
    }

    private static MessageSummary toSummary(StoredMessage m) {
        String preview = Messages.tryDecodeText(m.body(), m.bodyCharacterSet(), m.bodyEncoding());
        boolean previewTruncated = false;
        if (preview != null && preview.length() > PREVIEW_CHARS) {
            preview = preview.substring(0, PREVIEW_CHARS);
            previewTruncated = true;
        }
        return new MessageSummary(
                Messages.hex(m.messageId()),
                Messages.hex(m.correlationId()),
                messageTypeName(m.messageType()),
                m.mqmdFormat(),
                m.bodyFormat(),
                m.bodyCharacterSet(),
                m.bodyEncoding(),
                m.priority(),
                m.persistence(),
                m.expiry(),
                m.putDateTime(),
                emptyToNull(m.replyToQueue()),
                emptyToNull(m.replyToQueueManager()),
                emptyToNull(m.userId()),
                emptyToNull(m.putApplicationName()),
                m.bodyLength(),
                m.bodyTruncated(),
                m.properties().isEmpty() ? null : m.properties(),
                preview,
                previewTruncated);
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private static HttpStatus statusFor(int reasonCode) {
        return switch (reasonCode) {
            case MQConstants.MQRC_UNKNOWN_OBJECT_NAME, MQConstants.MQRC_NO_MSG_AVAILABLE -> HttpStatus.NOT_FOUND;
            case MQConstants.MQRC_NOT_AUTHORIZED -> HttpStatus.FORBIDDEN;
            case MQConstants.MQRC_OBJECT_IN_USE, MQConstants.MQRC_GET_INHIBITED, MQConstants.MQRC_PUT_INHIBITED,
                    MQConstants.MQRC_Q_FULL -> HttpStatus.CONFLICT;
            // сообщение больше MAXMSGL очереди/канала/QM — ошибка клиента, не сервиса
            case MQConstants.MQRC_MSG_TOO_BIG_FOR_Q, MQConstants.MQRC_MSG_TOO_BIG_FOR_Q_MGR,
                    MQConstants.MQRC_MSG_TOO_BIG_FOR_CHANNEL -> HttpStatus.forStatus(413);
            // remote-очереди и алиасы: browse/inquire для них невозможны by design;
            // 2142 — пользователь прислал битый заголовок (X-MQ-Format: MQHRF2 с телом без валидного RFH2)
            case MQConstants.MQRC_OBJECT_TYPE_ERROR, MQConstants.MQRC_OPTION_NOT_VALID_FOR_TYPE,
                    MQConstants.MQRC_SELECTOR_NOT_FOR_TYPE, MQConstants.MQRC_HEADER_ERROR -> HttpStatus.BAD_REQUEST;
            case MQConstants.MQRC_Q_MGR_NOT_AVAILABLE, MQConstants.MQRC_Q_MGR_NAME_ERROR,
                    MQConstants.MQRC_CONNECTION_BROKEN, MQConstants.MQRC_HOST_NOT_AVAILABLE,
                    MQConstants.MQRC_CHANNEL_NOT_AVAILABLE, MQConstants.MQRC_UNKNOWN_CHANNEL_NAME ->
                    HttpStatus.BAD_GATEWAY;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    // ---------- DTO ----------

    static class BadInput extends RuntimeException {
        BadInput(String message) {
            super(message);
        }
    }

    public record BrowseResponse(String queue, int depth, int returned, boolean moreAvailable,
                                 List<MessageSummary> messages) {}

    public record MessageSummary(
            String messageId,
            String correlationId,
            String messageType,
            String format,
            String bodyFormat,
            int characterSet,
            int encoding,
            int priority,
            int persistence,
            int expiry,
            String putDateTime,
            String replyToQueue,
            String replyToQueueManager,
            String userId,
            String putApplicationName,
            int bodyLength,
            boolean bodyTruncated,
            Map<String, String> properties,
            String bodyPreview,
            boolean bodyPreviewTruncated) {}

    public record PutResponse(String queue, String messageId, String correlationId) {}

    public record PurgeResponse(String queue, int purged, int remaining) {}

    public record QueueInfoResponse(String queue, int depth, int maxDepth, int maxMessageLength,
                                    int openInputCount, int openOutputCount,
                                    boolean getInhibited, boolean putInhibited) {}

    public record ErrorResponse(String error, String details, Integer mqCompletionCode, Integer mqReasonCode,
                                String cause) {}
}
