package me.waldemar;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Операции над очередями. Каждая операция выполняется на собственном соединении (см. MqConnectionFactory). */
public class MqService {

    /** Тела в browse-списке обрезаются до этого размера, чтобы глубокая очередь не съела память. */
    static final int BROWSE_BODY_CAP = 256 * 1024;

    /** Суммарный бюджет тел одного browse: дальше отдаём moreAvailable=true, а не копим гигабайты в куче. */
    static final long BROWSE_TOTAL_CAP = 64L * 1024 * 1024;

    private final MqConnectionFactory mq;

    public MqService(MqConnectionFactory mq) {
        this.mq = mq;
    }

    public record BrowseResult(int depth, List<StoredMessage> messages, boolean moreAvailable) {}
    public record PutResult(byte[] messageId, byte[] correlationId) {}
    public record PurgeResult(int purged, int remaining) {}
    public record QueueInfo(int depth, int maxDepth, int maxMessageLength,
                            int openInputCount, int openOutputCount,
                            boolean getInhibited, boolean putInhibited) {}
    /** reply == null означает таймаут ожидания ответа. */
    public record RpcResult(byte[] requestMessageId, byte[] correlationUsed, StoredMessage reply) {}

    public void ping() throws MQException, IOException {
        mq.withQueueManager(qMgr -> Boolean.TRUE);
    }

    public QueueInfo queueInfo(String queueName) throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            MQQueue q = qMgr.accessQueue(queueName, MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                return new QueueInfo(
                        q.getCurrentDepth(),
                        q.getMaximumDepth(),
                        q.getMaximumMessageLength(),
                        q.getOpenInputCount(),
                        q.getOpenOutputCount(),
                        q.getInhibitGet() == MQConstants.MQQA_GET_INHIBITED,
                        q.getInhibitPut() == MQConstants.MQQA_PUT_INHIBITED);
            } finally {
                closeQuietly(q);
            }
        });
    }

    /** Просмотр без удаления. Свойства сообщений принудительно материализуются как RFH2 и разбираются сами. */
    public BrowseResult browse(String queueName, int limit) throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            MQQueue q = qMgr.accessQueue(queueName,
                    MQConstants.MQOO_BROWSE | MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                int depth = q.getCurrentDepth();
                List<StoredMessage> messages = new ArrayList<>();
                boolean first = true;
                boolean exhausted = false;
                long totalBytes = 0;
                while (messages.size() < limit && totalBytes < BROWSE_TOTAL_CAP) {
                    MQGetMessageOptions gmo = new MQGetMessageOptions();
                    gmo.options = (first ? MQConstants.MQGMO_BROWSE_FIRST : MQConstants.MQGMO_BROWSE_NEXT)
                            | MQConstants.MQGMO_NO_WAIT
                            | MQConstants.MQGMO_ACCEPT_TRUNCATED_MSG
                            | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
                            | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                    first = false;
                    MQMessage msg = new MQMessage();
                    boolean truncated = false;
                    try {
                        q.get(msg, gmo, BROWSE_BODY_CAP);
                    } catch (MQException e) {
                        if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                            exhausted = true;
                            break;
                        }
                        if (e.completionCode == MQConstants.MQCC_WARNING
                                && e.reasonCode == MQConstants.MQRC_TRUNCATED_MSG_ACCEPTED) {
                            truncated = true;
                        } else {
                            throw e;
                        }
                    }
                    StoredMessage parsed = Messages.parse(msg, truncated);
                    messages.add(parsed);
                    totalBytes += parsed.body().length;
                }
                boolean more = !exhausted && depth > messages.size();
                return new BrowseResult(depth, messages, more);
            } finally {
                closeQuietly(q);
            }
        });
    }

    /** Одно сообщение по MsgId: browse (destructive=false) или destructive get (destructive=true). */
    public StoredMessage getByMessageId(String queueName, byte[] messageId, boolean destructive)
            throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            int openOptions = (destructive ? MQConstants.MQOO_INPUT_AS_Q_DEF : MQConstants.MQOO_BROWSE)
                    | MQConstants.MQOO_FAIL_IF_QUIESCING;
            MQQueue q = qMgr.accessQueue(queueName, openOptions);
            try {
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = (destructive ? 0 : MQConstants.MQGMO_BROWSE_FIRST)
                        | MQConstants.MQGMO_NO_WAIT
                        | MQConstants.MQGMO_NO_SYNCPOINT
                        | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
                        | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                gmo.matchOptions = MQConstants.MQMO_MATCH_MSG_ID;
                MQMessage msg = new MQMessage();
                msg.messageId = messageId;
                q.get(msg, gmo);
                return Messages.parse(msg, false);
            } finally {
                closeQuietly(q);
            }
        });
    }

    public PutResult put(String queueName, MQMessage msg) throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            MQQueue q = qMgr.accessQueue(queueName, MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;
                q.put(msg, pmo);
                return new PutResult(msg.messageId, msg.correlationId);
            } finally {
                closeQuietly(q);
            }
        });
    }

    /**
     * Очистка очереди truncated get-ами с нулевым буфером — тела сообщений не передаются по сети.
     * Число итераций ограничено глубиной очереди на момент старта: сообщения, положенные во время
     * очистки, не задерживают ответ (их видно по полю remaining).
     */
    public PurgeResult purge(String queueName) throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            MQQueue q = qMgr.accessQueue(queueName,
                    MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                int depth = q.getCurrentDepth();
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = MQConstants.MQGMO_NO_WAIT
                        | MQConstants.MQGMO_NO_SYNCPOINT
                        | MQConstants.MQGMO_ACCEPT_TRUNCATED_MSG
                        | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                int purged = 0;
                for (int i = 0; i < depth; i++) {
                    MQMessage msg = new MQMessage();
                    try {
                        q.get(msg, gmo, 0);
                        purged++;
                    } catch (MQException e) {
                        if (e.reasonCode == MQConstants.MQRC_TRUNCATED_MSG_ACCEPTED) {
                            purged++;
                        } else if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                            break;
                        } else {
                            throw e;
                        }
                    }
                }
                return new PurgeResult(purged, q.getCurrentDepth());
            } finally {
                closeQuietly(q);
            }
        });
    }

    /**
     * Запрос-ответ: put в requestQueue, затем get с ожиданием из replyQueue по CorrelId.
     * correlateByCorrelId=false — стандартная схема (CorrelId ответа = MsgId запроса),
     * true — passthrough (CorrelId ответа = CorrelId запроса).
     */
    public RpcResult rpc(String requestQueue, String replyQueue, MQMessage request,
                         boolean correlateByCorrelId, int timeoutSeconds) throws MQException, IOException {
        return mq.withQueueManager(qMgr -> {
            // просим report-honoring респондеров (MQReply в IIB/ACE и т.п.) вернуть CorrelId запроса,
            // а не MsgId — иначе passthrough-режим никогда не сматчит ответ
            if (correlateByCorrelId) {
                request.report |= MQConstants.MQRO_PASS_CORREL_ID;
            }
            MQQueue rq = qMgr.accessQueue(requestQueue, MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;
                rq.put(request, pmo);
            } finally {
                closeQuietly(rq);
            }

            byte[] match = correlateByCorrelId ? request.correlationId : request.messageId;

            MQQueue replyQ = qMgr.accessQueue(replyQueue,
                    MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING);
            try {
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = MQConstants.MQGMO_WAIT
                        | MQConstants.MQGMO_NO_SYNCPOINT
                        | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
                        | MQConstants.MQGMO_FAIL_IF_QUIESCING;
                gmo.waitInterval = timeoutSeconds * 1000;
                gmo.matchOptions = MQConstants.MQMO_MATCH_CORREL_ID;
                MQMessage reply = new MQMessage();
                reply.correlationId = match;
                try {
                    replyQ.get(reply, gmo);
                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        return new RpcResult(request.messageId, match, null);
                    }
                    throw e;
                }
                return new RpcResult(request.messageId, match, Messages.parse(reply, false));
            } finally {
                closeQuietly(replyQ);
            }
        });
    }

    private static void closeQuietly(MQQueue q) {
        try {
            q.close();
        } catch (MQException ignored) {
            // соединение всё равно закрывается целиком после операции
        }
    }
}
