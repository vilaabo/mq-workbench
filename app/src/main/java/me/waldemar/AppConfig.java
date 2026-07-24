package me.waldemar;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация подключения. Приоритет источников: CLI-аргументы > переменные окружения > .env в текущей директории.
 */
public record AppConfig(
        String host,
        int port,
        String channel,
        String qmgr,
        String user,
        String password,
        int apiPort,
        String respondersFile
) {

    public static AppConfig load(String[] args) {
        MqArgs cli = new MqArgs(args);
        Map<String, String> env = new HashMap<>(System.getenv());
        loadDotEnv(env);

        String host = pick(cli, "host", env, "MQ_HOST");
        String portStr = pick(cli, "port", env, "MQ_PORT");
        String channel = pick(cli, "channel", env, "MQ_CHANNEL");
        String qmgr = pick(cli, "qmgr", env, "MQ_QMGR");
        String user = pick(cli, "user", env, "MQ_USER");
        String password = pick(cli, "password", env, "MQ_PASSWORD");
        String apiPortStr = pick(cli, "api-port", env, "API_PORT");
        String respondersFile = pick(cli, "responders", env, "RESPONDERS_FILE");

        require(host, "--host / MQ_HOST");
        require(portStr, "--port / MQ_PORT");
        require(channel, "--channel / MQ_CHANNEL");
        require(qmgr, "--qmgr / MQ_QMGR");

        return new AppConfig(
                host,
                parseInt(portStr, "--port / MQ_PORT"),
                channel,
                qmgr,
                user,
                password,
                apiPortStr == null ? 8080 : parseInt(apiPortStr, "--api-port / API_PORT"),
                respondersFile);
    }

    /** .env дополняет окружение, но не перекрывает его. */
    private static void loadDotEnv(Map<String, String> env) {
        Path dotenv = Path.of(".env");
        if (!Files.isRegularFile(dotenv)) return;
        try {
            for (String line : Files.readAllLines(dotenv, StandardCharsets.UTF_8)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) continue;
                int eq = t.indexOf('=');
                if (eq <= 0) continue;
                env.putIfAbsent(t.substring(0, eq).trim(), t.substring(eq + 1).trim());
            }
        } catch (IOException e) {
            System.err.println("Не удалось прочитать .env: " + e.getMessage());
        }
    }

    private static String pick(MqArgs cli, String cliKey, Map<String, String> env, String envKey) {
        String v = cli.optional(cliKey);
        if (v != null && !v.isBlank()) return v;
        v = env.get(envKey);
        return v == null || v.isBlank() ? null : v;
    }

    private static void require(String value, String name) {
        if (value == null) {
            throw new IllegalArgumentException("Не задан обязательный параметр: " + name);
        }
    }

    private static int parseInt(String value, String name) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " должен быть числом, получено: " + value);
        }
    }
}
