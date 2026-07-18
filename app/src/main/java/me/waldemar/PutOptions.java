package me.waldemar;

import java.util.Map;

/** Параметры отправки: поля MQMD плюс usr-свойства RFH2 (заголовки X-MQ-Usr-*). Null = не задано, действует дефолт. */
public record PutOptions(
        Integer messageType,
        String bodyFormat,
        Integer characterSet,
        Integer encoding,
        byte[] messageId,
        byte[] correlationId,
        String replyToQueue,
        String replyToQueueManager,
        Integer priority,
        Integer persistence,
        Integer expiry,
        Map<String, String> usrProperties
) {}
