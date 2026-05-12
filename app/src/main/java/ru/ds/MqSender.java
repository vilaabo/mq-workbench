package ru.ds;

import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;

import java.io.IOException;
import java.util.HexFormat;

public class MqSender {

    private static final HexFormat HEX = HexFormat.of().withUpperCase();
    private static final int MQ_ID_LENGTH = 24;

    private final MQQueueManager qMgr;

    public MqSender(MQQueueManager qMgr) {
        this.qMgr = qMgr;
    }

    public String send(String queueName, String text, SendOptions options) throws MQException, IOException {
        synchronized (qMgr) {
            int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
            MQQueue queue = qMgr.accessQueue(queueName, openOptions);
            try {
                MQMessage msg = new MQMessage();

                msg.messageType  = options.messageType()  != null ? options.messageType()  : MQConstants.MQMT_REPLY;
                msg.format       = padFormat(options.format() != null ? options.format() : MQConstants.MQFMT_STRING);
                msg.characterSet = options.characterSet() != null ? options.characterSet() : 1208;

                if (options.correlationId() != null) msg.correlationId = padOrTrunc(options.correlationId(), MQ_ID_LENGTH);
                if (options.messageId() != null)     msg.messageId     = padOrTrunc(options.messageId(),     MQ_ID_LENGTH);
                if (options.replyToQueue() != null)         msg.replyToQueueName        = options.replyToQueue();
                if (options.replyToQueueManager() != null)  msg.replyToQueueManagerName = options.replyToQueueManager();
                if (options.priority() != null)    msg.priority    = options.priority();
                if (options.persistence() != null) msg.persistence = options.persistence();
                if (options.expiry() != null)      msg.expiry      = options.expiry();
                if (options.encoding() != null)    msg.encoding    = options.encoding();

                msg.writeString(text);

                System.out.println("[MqSender] PUT → " + queueName);
                System.out.println("  messageType        = " + msg.messageType);
                System.out.println("  format             = '" + msg.format + "'");
                System.out.println("  characterSet       = " + msg.characterSet);
                System.out.println("  encoding           = " + msg.encoding);
                System.out.println("  priority           = " + msg.priority);
                System.out.println("  persistence        = " + msg.persistence);
                System.out.println("  expiry             = " + msg.expiry);
                System.out.println("  messageId (before) = " + HEX.formatHex(msg.messageId));
                System.out.println("  correlationId      = " + HEX.formatHex(msg.correlationId));
                System.out.println("  replyToQueue       = '" + msg.replyToQueueName + "'");
                System.out.println("  replyToQMgr        = '" + msg.replyToQueueManagerName + "'");
                System.out.println("  totalLength        = " + msg.getMessageLength() + " bytes");

                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;
                queue.put(msg, pmo);

                System.out.println("  messageId (after)  = " + HEX.formatHex(msg.messageId));
                System.out.println("  → put returned without exception");

                return HEX.formatHex(msg.messageId);
            } finally {
                queue.close();
            }
        }
    }

    private static String padFormat(String s) {
        if (s.length() >= 8) return s.substring(0, 8);
        return s + " ".repeat(8 - s.length());
    }

    private static byte[] padOrTrunc(byte[] src, int len) {
        if (src.length == len) return src;
        byte[] dst = new byte[len];
        System.arraycopy(src, 0, dst, 0, Math.min(src.length, len));
        return dst;
    }
}
