package ru.ds;

public record SendOptions(
        Integer messageType,
        String format,
        Integer characterSet,
        byte[] correlationId,
        byte[] messageId,
        String replyToQueue,
        String replyToQueueManager,
        Integer priority,
        Integer persistence,
        Integer expiry,
        Integer encoding
) {}
