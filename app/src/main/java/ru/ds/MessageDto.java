package ru.ds;

public record MessageDto(
        String messageId,
        String correlationId,
        int messageType,
        String messageTypeText,
        String format,
        int characterSet,
        int encoding,
        int priority,
        int persistence,
        String putDateTime,
        String replyToQueue,
        String replyToQueueManager,
        String userId,
        String putApplicationName,
        int messageLength,
        Object text
) {}
