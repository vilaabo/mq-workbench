package ru.ds;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

public class MqReader {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HexFormat HEX = HexFormat.of().withUpperCase();

    private final MQQueueManager qMgr;

    public MqReader(MQQueueManager qMgr) {
        this.qMgr = qMgr;
    }

    public int purge(String queueName) throws MQException {
        synchronized (qMgr) {
            int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING;
            MQQueue queue = qMgr.accessQueue(queueName, openOptions);
            try {
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = MQConstants.MQGMO_NO_WAIT
                        | MQConstants.MQGMO_NO_SYNCPOINT
                        | MQConstants.MQGMO_FAIL_IF_QUIESCING;

                int count = 0;
                while (true) {
                    MQMessage msg = new MQMessage();
                    try {
                        queue.get(msg, gmo);
                        count++;
                    } catch (MQException e) {
                        if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) break;
                        throw e;
                    }
                }
                return count;
            } finally {
                queue.close();
            }
        }
    }

    public List<MessageDto> browseAll(String queueName) throws MQException, IOException {
        synchronized (qMgr) {
            int openOptions = MQConstants.MQOO_BROWSE | MQConstants.MQOO_FAIL_IF_QUIESCING;
            MQQueue queue = qMgr.accessQueue(queueName, openOptions);
            try {
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = MQConstants.MQGMO_BROWSE_FIRST
                        | MQConstants.MQGMO_NO_WAIT
                        | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                gmo.matchOptions = MQConstants.MQMO_NONE;

                List<MessageDto> result = new ArrayList<>();
                while (true) {
                    MQMessage msg = new MQMessage();
                    try {
                        queue.get(msg, gmo);
                    } catch (MQException e) {
                        if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) break;
                        throw e;
                    }
                    result.add(toDto(msg));
                    gmo.options = MQConstants.MQGMO_BROWSE_NEXT
                            | MQConstants.MQGMO_NO_WAIT
                            | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                }
                return result;
            } finally {
                queue.close();
            }
        }
    }

    private static MessageDto toDto(MQMessage msg) throws IOException {
        byte[] bytes = new byte[msg.getMessageLength()];
        msg.readFully(bytes);
        String text = new String(bytes, charsetForCcsid(msg.characterSet));
        return new MessageDto(
                HEX.formatHex(msg.messageId),
                HEX.formatHex(msg.correlationId),
                msg.messageType,
                messageTypeName(msg.messageType),
                msg.format.trim(),
                msg.characterSet,
                msg.encoding,
                msg.priority,
                msg.persistence,
                msg.putDateTime.toInstant().toString(),
                msg.replyToQueueName.trim(),
                msg.replyToQueueManagerName.trim(),
                msg.userId.trim(),
                msg.putApplicationName.trim(),
                bytes.length,
                parseJsonOrString(text)
        );
    }

    private static Object parseJsonOrString(String text) {
        if (text == null || text.isBlank()) return null;
        try {
            return JSON.readTree(text);
        } catch (JsonProcessingException e) {
            return text;
        }
    }

    private static Charset charsetForCcsid(int ccsid) {
        return switch (ccsid) {
            case 1208 -> StandardCharsets.UTF_8;
            case 1200, 13488, 17584 -> StandardCharsets.UTF_16BE;
            case 819 -> StandardCharsets.ISO_8859_1;
            default -> {
                try {
                    yield Charset.forName("Cp" + ccsid);
                } catch (Exception e) {
                    yield StandardCharsets.UTF_8;
                }
            }
        };
    }

    private static String messageTypeName(int type) {
        return switch (type) {
            case MQConstants.MQMT_DATAGRAM -> "DATAGRAM";
            case MQConstants.MQMT_REQUEST -> "REQUEST";
            case MQConstants.MQMT_REPLY -> "REPLY";
            case MQConstants.MQMT_REPORT -> "REPORT";
            default -> "UNKNOWN(" + type + ")";
        };
    }
}
