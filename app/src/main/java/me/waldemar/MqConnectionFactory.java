package me.waldemar;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import java.io.IOException;
import java.util.Hashtable;

/**
 * Соединение на каждый запрос: MQQueueManager создаётся, используется и закрывается внутри withQueueManager.
 * Сервис остаётся stateless — обрыв соединения не требует рестарта, параллельные запросы ничего не делят.
 */
public class MqConnectionFactory {

    private final AppConfig cfg;

    public MqConnectionFactory(AppConfig cfg) {
        this.cfg = cfg;
    }

    public interface MqCallable<T> {
        T apply(MQQueueManager qMgr) throws MQException, IOException;
    }

    /** Открывает новое соединение. Вызывающий отвечает за disconnect (респондеры держат его подолгу). */
    public MQQueueManager connect() throws MQException {
        return new MQQueueManager(cfg.qmgr(), connectionProperties());
    }

    public <T> T withQueueManager(MqCallable<T> body) throws MQException, IOException {
        MQQueueManager qMgr = connect();
        try {
            return body.apply(qMgr);
        } finally {
            try {
                qMgr.disconnect();
            } catch (MQException ignored) {
                // ошибка disconnect не должна маскировать результат операции
            }
        }
    }

    private Hashtable<String, Object> connectionProperties() {
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, cfg.host());
        props.put(MQConstants.PORT_PROPERTY, cfg.port());
        props.put(MQConstants.CHANNEL_PROPERTY, cfg.channel());
        if (cfg.user() != null && !cfg.user().isBlank()) {
            props.put(MQConstants.USER_ID_PROPERTY, cfg.user());
            props.put(MQConstants.PASSWORD_PROPERTY, cfg.password() == null ? "" : cfg.password());
            props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
        }
        return props;
    }
}
