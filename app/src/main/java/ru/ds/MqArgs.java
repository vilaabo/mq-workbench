package ru.ds;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MqArgs {

    private final Map<String, String> values = new LinkedHashMap<>();
    private final List<String> positional = new ArrayList<>();

    public MqArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                String body = a.substring(2);
                int eq = body.indexOf('=');
                if (eq >= 0) {
                    values.put(body.substring(0, eq), body.substring(eq + 1));
                } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    values.put(body, args[++i]);
                } else {
                    values.put(body, "");
                }
            } else {
                positional.add(a);
            }
        }
    }

    public String required(String key) {
        String v = values.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Не передан обязательный CLI-аргумент: --" + key);
        }
        return v;
    }

    public int requiredInt(String key) {
        String v = required(key);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CLI-аргумент --" + key + " должен быть числом, получено: " + v);
        }
    }

    public int optionalInt(String key, int defaultValue) {
        String v = values.get(key);
        if (v == null || v.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CLI-аргумент --" + key + " должен быть числом, получено: " + v);
        }
    }

    public List<String> positional() {
        return positional;
    }
}
