package me.waldemar;

import java.util.Map;

/**
 * Прочитанное из очереди сообщение: поля MQMD, свойства из RFH2 и тело (уже без заголовка RFH2).
 * properties: поля папки usr — под своими именами, поля других папок — с префиксом "папка." (jms.Dst и т.п.).
 */
public record StoredMessage(
        byte[] messageId,
        byte[] correlationId,
        int messageType,
        String mqmdFormat,
        String bodyFormat,
        int bodyCharacterSet,
        int bodyEncoding,
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
        byte[] body
) {}
