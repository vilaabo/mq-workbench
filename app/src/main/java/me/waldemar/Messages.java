package me.waldemar;

import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.CCSID;
import com.ibm.mq.headers.MQDLH;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQRFH2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/** Разбор и сборка MQMessage: MQMD, заголовок RFH2 (usr-свойства) и кодировка тела. */
public final class Messages {

    private static final Logger log = LoggerFactory.getLogger(Messages.class);
    private static final HexFormat HEX = HexFormat.of().withUpperCase();

    public static final int MQ_ID_LENGTH = 24;

    private Messages() {}

    /**
     * Разбирает полученное сообщение, снимая цепочку известных заголовков:
     * MQDLH (dead-letter, причина уходит в properties как dlh.*) и MQRFH2 (возможна цепочка
     * RFH2->RFH2: свойства сообщения + физический заголовок). Формат/CCSID/encoding тела
     * берутся из последнего снятого заголовка.
     */
    public static StoredMessage parse(MQMessage msg, boolean bodyTruncated) throws IOException {
        String mqmdFormat = msg.format;
        String bodyFormat = mqmdFormat.trim();
        int bodyCcsid = msg.characterSet;
        int bodyEncoding = msg.encoding;
        Map<String, String> properties = new LinkedHashMap<>();

        try {
            String fmt = mqmdFormat;
            int ccsid = msg.characterSet;
            int encoding = msg.encoding;
            while (true) {
                if (MQConstants.MQFMT_RF_HEADER_2.equals(fmt)) {
                    MQRFH2 rfh2 = new MQRFH2(msg, encoding, ccsid);
                    fmt = rfh2.getFormat();
                    ccsid = rfh2.getCodedCharSetId() > 0 ? rfh2.getCodedCharSetId() : ccsid;
                    encoding = rfh2.getEncoding();
                    collectProperties(rfh2, properties);
                } else if (MQConstants.MQFMT_DEAD_LETTER_HEADER.equals(fmt)) {
                    MQDLH dlh = new MQDLH(msg, encoding, ccsid);
                    fmt = dlh.getFormat();
                    ccsid = dlh.getCodedCharSetId() > 0 ? dlh.getCodedCharSetId() : ccsid;
                    encoding = dlh.getEncoding();
                    String reason = MQConstants.lookup(dlh.getReason(), "MQRC_.*|MQFB_.*");
                    properties.putIfAbsent("dlh.Reason",
                            reason != null ? reason : String.valueOf(dlh.getReason()));
                    properties.putIfAbsent("dlh.DestQueue", dlh.getDestQName().trim());
                    properties.putIfAbsent("dlh.DestQueueManager", dlh.getDestQMgrName().trim());
                } else {
                    break;
                }
            }
            bodyFormat = fmt.trim();
            bodyCcsid = ccsid;
            bodyEncoding = encoding;
        } catch (MQDataException | IOException e) {
            // заголовок не разобрался (например, обрезан при browse) — отдаём сообщение как есть
            log.warn("Не удалось разобрать заголовки (msgId={}): {}", hex(msg.messageId), e.toString());
            msg.seek(0);
            bodyFormat = mqmdFormat.trim();
            bodyCcsid = msg.characterSet;
            bodyEncoding = msg.encoding;
            properties.clear();
        }

        byte[] body = new byte[msg.getDataLength()];
        msg.readFully(body);

        return new StoredMessage(
                msg.messageId,
                msg.correlationId,
                msg.messageType,
                mqmdFormat.trim(),
                bodyFormat,
                bodyCcsid,
                bodyEncoding,
                msg.priority,
                msg.persistence,
                msg.expiry,
                msg.report,
                msg.putDateTime == null ? null : msg.putDateTime.toInstant().toString(),
                msg.replyToQueueName.trim(),
                msg.replyToQueueManagerName.trim(),
                msg.userId.trim(),
                msg.putApplicationName.trim(),
                body.length,
                bodyTruncated,
                properties,
                body);
    }

    private static void collectProperties(MQRFH2 rfh2, Map<String, String> out) throws IOException {
        for (MQRFH2.Element folder : rfh2.getFolders()) {
            String folderName = folder.getName();
            for (MQRFH2.Element field : folder.getChildren()) {
                String key = "usr".equals(folderName) ? field.getName() : folderName + "." + field.getName();
                Object value = field.getValue();
                // при цепочке RFH2->RFH2 внешний заголовок (настоящие свойства) главнее внутреннего
                out.putIfAbsent(key, value == null ? "" : String.valueOf(value));
            }
        }
    }

    /**
     * Собирает сообщение для отправки. Тело передаётся уже в целевой кодировке (CCSID).
     * Если заданы usr-свойства, перед телом пишется RFH2 (MQMD Format становится MQHRF2,
     * а формат тела уходит в поле Format самого RFH2 — стандартная цепочка заголовков MQ).
     */
    public static MQMessage build(byte[] body, PutOptions o) throws IOException {
        MQMessage msg = new MQMessage();
        msg.messageType = o.messageType() != null ? o.messageType() : MQConstants.MQMT_DATAGRAM;
        int ccsid = o.characterSet() != null ? o.characterSet() : 1208;
        String bodyFormat = padFormat(o.bodyFormat() != null ? o.bodyFormat() : MQConstants.MQFMT_STRING);

        if (o.messageId() != null) msg.messageId = padId(o.messageId());
        if (o.correlationId() != null) msg.correlationId = padId(o.correlationId());
        if (o.replyToQueue() != null) msg.replyToQueueName = o.replyToQueue();
        if (o.replyToQueueManager() != null) msg.replyToQueueManagerName = o.replyToQueueManager();
        if (o.priority() != null) msg.priority = o.priority();
        if (o.persistence() != null) msg.persistence = o.persistence();
        if (o.expiry() != null) msg.expiry = o.expiry();
        if (o.encoding() != null) msg.encoding = o.encoding();

        if (o.usrProperties() == null || o.usrProperties().isEmpty()) {
            msg.format = bodyFormat;
            msg.characterSet = ccsid;
        } else {
            MQRFH2 rfh2 = new MQRFH2();
            rfh2.setEncoding(msg.encoding);
            rfh2.setCodedCharSetId(ccsid);
            rfh2.setFormat(bodyFormat);
            rfh2.setNameValueCCSID(1208);
            for (Map.Entry<String, String> e : o.usrProperties().entrySet()) {
                // ключи вида "jms.Dst" возвращаются чтением из чужих папок — при записи
                // раскладываем их обратно в родную папку, а не в usr (иначе реплей ломает JMS/IIB-потоки)
                String key = e.getKey();
                if (key.startsWith("dlh.")) continue; // синтетическая диагностика из MQDLH, не свойство
                int dot = key.indexOf('.');
                if (dot > 0 && dot < key.length() - 1) {
                    rfh2.setFieldValue(key.substring(0, dot), key.substring(dot + 1), e.getValue());
                } else {
                    rfh2.setFieldValue("usr", key, e.getValue());
                }
            }
            msg.format = MQConstants.MQFMT_RF_HEADER_2;
            msg.characterSet = 1208; // CCSID строковых полей самого заголовка RFH2
            rfh2.write(msg);
        }
        msg.write(body);
        return msg;
    }

    /** MsgId/CorrelId в MQ — ровно 24 байта: дополняем нулями или обрезаем. */
    public static byte[] padId(byte[] src) {
        if (src.length == MQ_ID_LENGTH) return src;
        byte[] dst = new byte[MQ_ID_LENGTH];
        System.arraycopy(src, 0, dst, 0, Math.min(src.length, MQ_ID_LENGTH));
        return dst;
    }

    /** MQMD Format — ровно 8 символов, пробелы справа. "NONE" превращается в MQFMT_NONE. */
    public static String padFormat(String s) {
        String t = s == null ? "" : s.trim();
        if (t.isEmpty() || t.equalsIgnoreCase("NONE")) return MQConstants.MQFMT_NONE;
        if (t.length() >= 8) return t.substring(0, 8);
        return t + " ".repeat(8 - t.length());
    }

    /**
     * Charset для CCSID. Для UTF-16 (1200/13488/17584) порядок байт берётся из поля Encoding —
     * сообщения от little-endian продюсеров (Windows/.NET) декодируются как UTF-16LE.
     */
    public static Charset charsetFor(int ccsid, int encoding) {
        switch (ccsid) {
            case 0, 1208 -> { return StandardCharsets.UTF_8; }
            case 1200, 13488, 17584 -> {
                boolean littleEndian = (encoding & MQConstants.MQENC_INTEGER_MASK) == MQConstants.MQENC_INTEGER_REVERSED;
                return littleEndian ? StandardCharsets.UTF_16LE : StandardCharsets.UTF_16BE;
            }
            case 819 -> { return StandardCharsets.ISO_8859_1; }
            default -> {
                try {
                    return Charset.forName(CCSID.getCodepage(ccsid));
                } catch (Exception ignored) {
                    // не все CCSID известны хелперу IBM — пробуем алиас JVM
                }
                try {
                    return Charset.forName("Cp" + ccsid);
                } catch (Exception ignored) {
                    // сработает фолбэк ниже
                }
                log.warn("Неизвестный CCSID {}, декодирую как UTF-8", ccsid);
                return StandardCharsets.UTF_8;
            }
        }
    }

    /**
     * Пытается декодировать тело как текст. Возвращает null, если результат похож на бинарные данные
     * (заметная доля control-символов или символов замены).
     */
    public static String tryDecodeText(byte[] body, int ccsid, int encoding) {
        if (body.length == 0) return "";
        String s = new String(body, charsetFor(ccsid, encoding));
        int suspicious = 0;
        int checked = Math.min(s.length(), 1000);
        if (checked == 0) return null;
        for (int i = 0; i < checked; i++) {
            char c = s.charAt(i);
            if (c == '�' || (c < 0x20 && c != '\r' && c != '\n' && c != '\t')) suspicious++;
        }
        return suspicious * 20 > checked ? null : s; // больше 5% мусора — не текст
    }

    public static String hex(byte[] bytes) {
        return HEX.formatHex(bytes);
    }

    public static byte[] fromHex(String s) {
        return HexFormat.of().parseHex(s.replaceAll("\\s", "").toLowerCase());
    }
}
