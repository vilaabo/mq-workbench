package me.waldemar;

import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Фоновый слушатель очереди запросов: превращает WireMock в заглушку MQ-интеграции.
 *
 * Каждое сообщение из requestQueue уходит в WireMock как POST {wiremockUrl}{pathTemplate}
 * (плейсхолдер {operation} — значение usr-свойства operationProperty), тело запроса — как есть,
 * метаданные — заголовками X-MQ-*. Тело ответа стаба кладётся в очередь ответов с корреляцией;
 * стаб может управлять метаданными ответа через свои HTTP-заголовки (X-MQ-Properties, X-MQ-Format,
 * X-MQ-Character-Set, X-MQ-Message-Type, X-MQ-Expiry). HTTP 204 от стаба = «промолчать»
 * (негативные сценарии), не-2xx = ошибка (считается в статистике, сообщение не отвечается).
 *
 * Держит собственное соединение с MQ и переподключается с экспоненциальным бэкоффом —
 * рестарт queue manager переживается без вмешательства.
 */
public class Responder implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Responder.class);

    private static final ObjectMapper HEADER_JSON =
            JsonMapper.builder().enable(JsonWriteFeature.ESCAPE_NON_ASCII).build();

    private static final int WAIT_INTERVAL_MS = 5_000;
    private static final long MAX_BACKOFF_MS = 30_000;

    public enum State { STARTING, RUNNING, RECONNECTING, STOPPED }

    public record Status(
            String name,
            String requestQueue,
            String replyQueue,
            String wiremockUrl,
            String pathTemplate,
            String operationProperty,
            String correlation,
            boolean useRequestReplyToQueue,
            String state,
            long received,
            long replied,
            long silenced,
            long errors,
            String lastError,
            String lastActivityAt,
            String startedAt) {}

    private final ResponderConfig cfg;
    private final MqConnectionFactory mqFactory;
    private final HttpClient http;
    private final Thread thread;
    private final String startedAt = Instant.now().toString();

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong replied = new AtomicLong();
    private final AtomicLong silenced = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();

    private volatile boolean stopRequested;
    private volatile State state = State.STARTING;
    private volatile String lastError;
    private volatile String lastActivityAt;

    Responder(ResponderConfig cfg, MqConnectionFactory mqFactory) {
        this.cfg = cfg;
        this.mqFactory = mqFactory;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(cfg.wiremockTimeoutSeconds()))
                .build();
        this.thread = Thread.ofVirtual().name("responder-" + cfg.name()).unstarted(this);
    }

    void start() {
        thread.start();
    }

    /**
     * Останавливает цикл. Interrupt обрывает HTTP-вызов в WireMock и backoff-sleep;
     * MQGET interrupt не слушает, поэтому хвост — до ~5 с (интервал ожидания).
     */
    void stop() {
        stopRequested = true;
        thread.interrupt();
    }

    /** true, если поток завершился за отведённое время. */
    boolean awaitTermination(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return !thread.isAlive();
    }

    public Status status() {
        return new Status(cfg.name(), cfg.requestQueue(), cfg.replyQueue(), cfg.wiremockUrl(),
                cfg.pathTemplate(), cfg.operationProperty(), cfg.correlation(), cfg.useRequestReplyToQueue(),
                state.name(), received.get(), replied.get(), silenced.get(), errors.get(),
                lastError, lastActivityAt, startedAt);
    }

    @Override
    public void run() {
        long backoffMs = 1_000;
        while (!stopRequested) {
            MQQueueManager qMgr = null;
            MQQueue in = null;
            try {
                qMgr = mqFactory.connect();
                in = qMgr.accessQueue(cfg.requestQueue(),
                        MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING);
                state = State.RUNNING;
                backoffMs = 1_000;
                log.info("[{}] слушаю {} -> {}", cfg.name(), cfg.requestQueue(), cfg.wiremockUrl());

                while (!stopRequested) {
                    MQGetMessageOptions gmo = new MQGetMessageOptions();
                    gmo.options = MQConstants.MQGMO_WAIT
                            | MQConstants.MQGMO_NO_SYNCPOINT
                            | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
                            | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                    gmo.waitInterval = WAIT_INTERVAL_MS;
                    MQMessage msg = new MQMessage();
                    try {
                        in.get(msg, gmo);
                    } catch (MQException e) {
                        if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) continue;
                        throw e;
                    }
                    received.incrementAndGet();
                    lastActivityAt = Instant.now().toString();
                    try {
                        handle(qMgr, msg);
                    } catch (Exception e) {
                        // ошибка одного сообщения (стаб недоступен, некуда отвечать) не роняет слушателя
                        errors.incrementAndGet();
                        lastError = e.toString();
                        if (stopRequested) {
                            log.warn("[{}] остановлен в момент обработки — сообщение msgId={} уже забрано из очереди и потеряно",
                                    cfg.name(), Messages.hex(msg.messageId));
                        } else {
                            log.warn("[{}] сообщение не обработано: {}", cfg.name(), e.toString());
                        }
                    }
                }
            } catch (Exception e) {
                if (!stopRequested) {
                    errors.incrementAndGet();
                    lastError = e.toString();
                    state = State.RECONNECTING;
                    log.warn("[{}] соединение потеряно, повтор через {} мс: {}", cfg.name(), backoffMs, e.toString());
                    sleepQuietly(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                }
            } finally {
                closeQuietly(in);
                disconnectQuietly(qMgr);
            }
        }
        state = State.STOPPED;
        log.info("[{}] остановлен", cfg.name());
    }

    private void handle(MQQueueManager qMgr, MQMessage rawRequest) throws Exception {
        StoredMessage request = Messages.parse(rawRequest, false);
        String operation = request.properties().getOrDefault(cfg.operationProperty(), "unknown");

        HttpResponse<byte[]> resp = callWiremock(request, operation);

        if (resp.statusCode() == 204) {
            // стаб сознательно молчит — сценарий «система не ответила»
            silenced.incrementAndGet();
            log.info("[{}] operation={}: стаб вернул 204, ответ не отправляется", cfg.name(), operation);
            return;
        }
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("WireMock вернул HTTP " + resp.statusCode()
                    + " для operation=" + operation + " (нет подходящего стаба?)");
        }

        String replyQueueName;
        String replyQueueManager = "";
        if (cfg.useRequestReplyToQueue() && !request.replyToQueue().isEmpty()) {
            replyQueueName = request.replyToQueue();
            // запрос мог прийти с другого QM — ответ должен уйти на пару (ReplyToQ, ReplyToQMgr),
            // маршрутизацию через transmission queue выполнит сам queue manager
            replyQueueManager = request.replyToQueueManager();
        } else {
            replyQueueName = cfg.replyQueue();
        }
        if (replyQueueName == null || replyQueueName.isBlank()) {
            throw new IllegalStateException("Некуда отвечать: MQMD ReplyToQ запроса пуст и replyQueue у респондера не задан");
        }

        MQMessage reply = buildReply(request, resp);
        int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
        MQQueue out = replyQueueManager.isEmpty()
                ? qMgr.accessQueue(replyQueueName, openOptions)
                : qMgr.accessQueue(replyQueueName, openOptions, replyQueueManager, null, null);
        try {
            MQPutMessageOptions pmo = new MQPutMessageOptions();
            pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;
            out.put(reply, pmo);
        } finally {
            closeQuietly(out);
        }
        replied.incrementAndGet();
        log.info("[{}] operation={}: ответ {} -> {} (corrId={})", cfg.name(), operation,
                cfg.requestQueue(), replyQueueName, Messages.hex(reply.correlationId));
    }

    private HttpResponse<byte[]> callWiremock(StoredMessage request, String operation) throws Exception {
        String path = cfg.pathTemplate().replace("{operation}",
                URLEncoder.encode(operation, StandardCharsets.UTF_8));

        // текстовые тела уходят в стаб перекодированными в UTF-8 — матчинг по телу (matchesJsonPath и т.п.)
        // работает одинаково для сообщений в любом CCSID (1251, EBCDIC, UTF-16)
        String text = Messages.tryDecodeText(request.body(), request.bodyCharacterSet(), request.bodyEncoding());
        byte[] body = text != null ? text.getBytes(StandardCharsets.UTF_8) : request.body();
        String contentType = text != null
                ? sniffTextContentType(text) + "; charset=utf-8"
                : "application/octet-stream";

        HttpRequest.Builder rb = HttpRequest.newBuilder()
                .uri(URI.create(cfg.wiremockUrl() + path))
                .timeout(Duration.ofSeconds(cfg.wiremockTimeoutSeconds()))
                .header("Content-Type", contentType)
                .header("X-MQ-Operation", operation)
                .header("X-MQ-Message-Id", Messages.hex(request.messageId()))
                .header("X-MQ-Correlation-Id", Messages.hex(request.correlationId()));
        if (!request.properties().isEmpty()) {
            rb.header("X-MQ-Properties", HEADER_JSON.writeValueAsString(request.properties()));
        }
        return http.send(rb.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String sniffTextContentType(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) continue;
            if (c == '{' || c == '[') return "application/json";
            if (c == '<') return "application/xml";
            break;
        }
        return "text/plain";
    }

    /**
     * Собирает ответное сообщение: тело — из ответа стаба, метаданные — из его X-MQ-* заголовков,
     * корреляция — по режиму респондера (auto уважает MQMD Report-флаги запроса, как MQReply в IIB/ACE).
     */
    private MQMessage buildReply(StoredMessage request, HttpResponse<byte[]> resp) throws IOException {
        Map<String, String> usr = new LinkedHashMap<>();
        String propsJson = resp.headers().firstValue("X-MQ-Properties").orElse(null);
        if (propsJson != null && !propsJson.isBlank()) {
            JsonNode node = HEADER_JSON.readTree(propsJson);
            if (node.isObject()) {
                node.fields().forEachRemaining(f -> usr.put(f.getKey(),
                        f.getValue().isValueNode() ? f.getValue().asText() : f.getValue().toString()));
            }
        }
        Integer ccsid = headerInt(resp, "X-MQ-Character-Set");
        Integer expiry = headerInt(resp, "X-MQ-Expiry");
        Integer messageType = resp.headers().firstValue("X-MQ-Message-Type")
                .map(MqApi::parseMessageType).orElse(MQConstants.MQMT_REPLY);
        String format = resp.headers().firstValue("X-MQ-Format").orElse(null);

        byte[] body = resp.body();
        boolean binary = resp.headers().firstValue("Content-Type")
                .map(ct -> ct.toLowerCase(Locale.ROOT).startsWith("application/octet-stream"))
                .orElse(false);
        if (!binary && body.length > 0) {
            // тело стаба приходит в кодировке его Content-Type (дефолт UTF-8); в сообщение оно должно лечь
            // в целевом CCSID (X-MQ-Character-Set стаба, иначе 1208) — иначе метаданные MQMD будут врать
            Charset from = responseCharset(resp);
            Charset to = ccsid != null
                    ? Messages.charsetFor(ccsid, MQConstants.MQENC_NATIVE)
                    : StandardCharsets.UTF_8;
            if (!from.equals(to)) body = new String(body, from).getBytes(to);
        }

        String mode = cfg.correlation().toLowerCase(Locale.ROOT);
        boolean auto = ResponderConfig.CORRELATION_AUTO.equalsIgnoreCase(mode);
        byte[] correlId = switch (mode) {
            case "corrid" -> request.correlationId();
            case "msgid" -> request.messageId();
            default -> (request.report() & MQConstants.MQRO_PASS_CORREL_ID) != 0
                    ? request.correlationId()
                    : request.messageId();
        };
        byte[] messageId = auto && (request.report() & MQConstants.MQRO_PASS_MSG_ID) != 0
                ? request.messageId()
                : null;

        PutOptions opts = new PutOptions(messageType, format, ccsid, null,
                messageId, correlId, null, null, null, null, expiry, usr);
        return Messages.build(body, opts);
    }

    private static Charset responseCharset(HttpResponse<byte[]> resp) {
        String ct = resp.headers().firstValue("Content-Type").orElse("");
        int i = ct.toLowerCase(Locale.ROOT).indexOf("charset=");
        if (i >= 0) {
            String cs = ct.substring(i + "charset=".length()).trim();
            int semi = cs.indexOf(';');
            if (semi >= 0) cs = cs.substring(0, semi);
            try {
                return Charset.forName(cs.replace("\"", "").trim());
            } catch (Exception ignored) {
                // нераспознанный charset — считаем UTF-8
            }
        }
        return StandardCharsets.UTF_8;
    }

    private static Integer headerInt(HttpResponse<byte[]> resp, String name) {
        return resp.headers().firstValue(name).map(s -> {
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }).orElse(null);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(MQQueue q) {
        if (q == null) return;
        try {
            q.close();
        } catch (MQException ignored) {
            // соединение закрывается целиком
        }
    }

    private static void disconnectQuietly(MQQueueManager qMgr) {
        if (qMgr == null) return;
        try {
            qMgr.disconnect();
        } catch (MQException ignored) {
            // уже не критично
        }
    }
}
