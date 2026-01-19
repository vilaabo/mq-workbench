package ru.ds;

import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;

public class MqReader {

    private static final String OUTPUT_FILE = "mq_dump.txt";

    public static void main(String[] args) {
        // Загружаем .env файл
        // Если файла .env нет физически, библиотека сама кинет исключение
        Dotenv dotenv = Dotenv.load();

        // Валидируем и читаем настройки (если нет — упадем с ошибкой)
        String host = getRequiredEnv(dotenv, "MQ_HOST");
        int port = Integer.parseInt(getRequiredEnv(dotenv, "MQ_PORT"));
        String channel = getRequiredEnv(dotenv, "MQ_CHANNEL");
        String qmgrName = getRequiredEnv(dotenv, "MQ_QMGR");
        String queueName = getRequiredEnv(dotenv, "MQ_QUEUE");

        MQQueueManager qMgr = null;
        MQQueue queue = null;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE, true))) {

            Hashtable<String, Object> props = new Hashtable<>();
            props.put(MQConstants.HOST_NAME_PROPERTY, host);
            props.put(MQConstants.PORT_PROPERTY, port);
            props.put(MQConstants.CHANNEL_PROPERTY, channel);

            // Пример для авторизации (если нужны — раскомментируй)
            // if (dotenv.get("MQ_USER") != null) {
            //     props.put(MQConstants.USER_ID_PROPERTY, dotenv.get("MQ_USER"));
            //     props.put(MQConstants.PASSWORD_PROPERTY, dotenv.get("MQ_PASSWORD"));
            // }

            System.out.println("Подключение к Queue Manager: " + qmgrName + " (" + host + ":" + port + ")");
            qMgr = new MQQueueManager(qmgrName, props);

            int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING;
            queue = qMgr.accessQueue(queueName, openOptions);

            System.out.println("Очередь " + queueName + " открыта. Начинаем вычитывание...");

            MQGetMessageOptions gmo = new MQGetMessageOptions();
            gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
            gmo.waitInterval = 1000;

            int count = 0;

            while (true) {
                MQMessage msg = new MQMessage();
                try {
                    queue.get(msg, gmo);

                    // Читаем как UTF-8, чтобы не зависеть от CCSID сообщения
                    byte[] bytes = new byte[msg.getMessageLength()];
                    msg.readFully(bytes);
                    String msgText = new String(bytes, StandardCharsets.UTF_8);

                    writer.write("=== MSG #" + (++count) + " | " + dtf.format(LocalDateTime.now()) + " ===");
                    writer.newLine();
                    writer.write(msgText);
                    writer.newLine();
                    writer.write("---------------------------------------------------");
                    writer.newLine();
                    writer.flush();

                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        System.out.println("Очередь пуста. Вычитано сообщений: " + count);
                        break;
                    } else {
                        throw e;
                    }
                }
            }

        } catch (MQException e) {
            System.err.println("Ошибка MQ: CC=" + e.completionCode + " RC=" + e.reasonCode);
            // Если ошибка подключения, часто полезно видеть вложенную причину
            if (e.getCause() != null) {
                System.err.println("Причина: " + e.getCause().getMessage());
            }
        } catch (IOException e) {
            System.err.println("Ошибка ввода-вывода: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Ошибка конфига: MQ_PORT должен быть числом!");
        } finally {
            try {
                if (queue != null) queue.close();
                if (qMgr != null) qMgr.disconnect();
            } catch (MQException e) {
                e.printStackTrace();
            }
        }
    }

    // Метод-гардиан: проверяет наличие переменной
    private static String getRequiredEnv(Dotenv dotenv, String key) {
        String value = dotenv.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException("CRITICAL ERROR: В файле .env не найдена настройка: " + key);
        }
        return value;
    }
}