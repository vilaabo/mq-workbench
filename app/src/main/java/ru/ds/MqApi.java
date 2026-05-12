package ru.ds;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;

import java.util.HexFormat;
import java.util.Hashtable;
import java.util.List;

public class MqApi {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) {
        MqArgs cli = new MqArgs(args);
        String host = cli.required("host");
        int port = cli.requiredInt("port");
        String channel = cli.required("channel");
        String qmgrName = cli.required("qmgr");
        int apiPort = cli.optionalInt("api-port", 8080);

        MQQueueManager qMgr;
        try {
            Hashtable<String, Object> props = new Hashtable<>();
            props.put(MQConstants.HOST_NAME_PROPERTY, host);
            props.put(MQConstants.PORT_PROPERTY, port);
            props.put(MQConstants.CHANNEL_PROPERTY, channel);
            System.out.println("Подключение к Queue Manager: " + qmgrName + " (" + host + ":" + port + ")");
            qMgr = new MQQueueManager(qmgrName, props);
        } catch (MQException e) {
            System.err.println("Не удалось подключиться к Queue Manager: CC=" + e.completionCode + " RC=" + e.reasonCode);
            if (e.getCause() != null) {
                System.err.println("Причина: " + e.getCause().getMessage());
            }
            return;
        }

        final MQQueueManager qMgrFinal = qMgr;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                qMgrFinal.disconnect();
            } catch (MQException e) {
                e.printStackTrace();
            }
        }));

        MqReader reader = new MqReader(qMgr);
        MqSender sender = new MqSender(qMgr);

        Javalin app = Javalin.create()
                .get("/messages/{queue}", ctx -> {
                    String queueName = ctx.pathParam("queue");
                    try {
                        List<MessageDto> messages = reader.browseAll(queueName);
                        ctx.json(new BrowseResponse(queueName, messages.size(), messages));
                    } catch (MQException e) {
                        mqError(ctx, e);
                    }
                })
                .delete("/messages/{queue}", ctx -> {
                    String queueName = ctx.pathParam("queue");
                    try {
                        int purged = reader.purge(queueName);
                        ctx.json(new PurgeResponse(queueName, purged));
                    } catch (MQException e) {
                        mqError(ctx, e);
                    }
                })
                .post("/messages/{queue}", ctx -> {
                    String queueName = ctx.pathParam("queue");
                    String body = ctx.body();
                    if (body == null || body.isBlank()) {
                        badRequest(ctx, "Empty body", "Тело запроса должно содержать JSON");
                        return;
                    }
                    try {
                        JSON.readTree(body);
                    } catch (JsonProcessingException e) {
                        badRequest(ctx, "Invalid JSON", e.getMessage());
                        return;
                    }

                    SendOptions opts;
                    try {
                        opts = parseSendOptions(ctx);
                    } catch (BadInputException e) {
                        badRequest(ctx, "Bad header", e.getMessage());
                        return;
                    }

                    try {
                        String messageId = sender.send(queueName, body, opts);
                        ctx.status(HttpStatus.CREATED);
                        ctx.json(new SendResponse(queueName, messageId));
                    } catch (MQException e) {
                        mqError(ctx, e);
                    }
                })
                .start(apiPort);

        System.out.println("API запущен на http://localhost:" + apiPort);
        System.out.println("  GET    /messages/{queue}  — прочитать сообщения (browse)");
        System.out.println("  POST   /messages/{queue}  — положить JSON-сообщение (опции в X-MQ-* headers)");
        System.out.println("  DELETE /messages/{queue}  — очистить очередь (destructive get всех сообщений)");
    }

    private static SendOptions parseSendOptions(Context ctx) {
        return new SendOptions(
                parseMessageType(ctx.header("X-MQ-Message-Type")),
                ctx.header("X-MQ-Format"),
                parseHeaderInt(ctx, "X-MQ-Character-Set"),
                parseHeaderHex(ctx, "X-MQ-Correlation-Id"),
                parseHeaderHex(ctx, "X-MQ-Message-Id"),
                ctx.header("X-MQ-Reply-To-Queue"),
                ctx.header("X-MQ-Reply-To-Queue-Manager"),
                parseHeaderInt(ctx, "X-MQ-Priority"),
                parseHeaderInt(ctx, "X-MQ-Persistence"),
                parseHeaderInt(ctx, "X-MQ-Expiry"),
                parseHeaderInt(ctx, "X-MQ-Encoding")
        );
    }

    private static Integer parseHeaderInt(Context ctx, String name) {
        String s = ctx.header(name);
        if (s == null || s.isBlank()) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new BadInputException(name + " должен быть числом, получено: " + s);
        }
    }

    private static byte[] parseHeaderHex(Context ctx, String name) {
        String s = ctx.header(name);
        if (s == null || s.isBlank()) return null;
        try {
            return HexFormat.of().parseHex(s.replaceAll("\\s", ""));
        } catch (IllegalArgumentException e) {
            throw new BadInputException(name + " должен быть hex-строкой, получено: " + s);
        }
    }

    private static Integer parseMessageType(String s) {
        if (s == null || s.isBlank()) return null;
        String trimmed = s.trim();
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
        }
        return switch (trimmed.toUpperCase()) {
            case "DATAGRAM" -> MQConstants.MQMT_DATAGRAM;
            case "REQUEST" -> MQConstants.MQMT_REQUEST;
            case "REPLY" -> MQConstants.MQMT_REPLY;
            case "REPORT" -> MQConstants.MQMT_REPORT;
            default -> throw new BadInputException("X-MQ-Message-Type: неизвестное значение " + s);
        };
    }

    private static void badRequest(Context ctx, String error, String details) {
        ctx.status(HttpStatus.BAD_REQUEST);
        ctx.json(new ErrorResponse(error, details, null));
    }

    private static void mqError(Context ctx, MQException e) {
        ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
        ctx.json(new ErrorResponse(
                "MQ error",
                "CC=" + e.completionCode + " RC=" + e.reasonCode,
                e.getCause() == null ? null : e.getCause().getMessage()
        ));
    }

    private static class BadInputException extends RuntimeException {
        BadInputException(String msg) {
            super(msg);
        }
    }

    public record BrowseResponse(String queue, int count, List<MessageDto> messages) {}

    public record SendResponse(String queue, String messageId) {}

    public record PurgeResponse(String queue, int purged) {}

    public record ErrorResponse(String error, String details, String cause) {}
}
