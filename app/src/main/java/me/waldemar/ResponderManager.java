package me.waldemar;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Реестр респондеров: создание, список, остановка. Живёт в памяти процесса. */
public class ResponderManager {

    static class Conflict extends RuntimeException {
        Conflict(String message) {
            super(message);
        }
    }

    private final MqConnectionFactory mqFactory;
    private final Map<String, Responder> responders = new ConcurrentHashMap<>();

    public ResponderManager(MqConnectionFactory mqFactory) {
        this.mqFactory = mqFactory;
    }

    public Responder.Status create(ResponderConfig raw) {
        ResponderConfig cfg = raw.normalized();
        cfg.validate();
        Responder responder = new Responder(cfg, mqFactory);
        if (responders.putIfAbsent(cfg.name(), responder) != null) {
            throw new Conflict("Респондер с именем '" + cfg.name() + "' уже существует");
        }
        responder.start();
        return responder.status();
    }

    public List<Responder.Status> list() {
        return responders.values().stream()
                .map(Responder::status)
                .sorted(Comparator.comparing(Responder.Status::name))
                .toList();
    }

    /** null, если респондера с таким именем нет. */
    public Responder.Status status(String name) {
        Responder r = responders.get(name);
        return r == null ? null : r.status();
    }

    /**
     * Останавливает респондера и дожидается выхода потока (до 10 с) — после ответа 200
     * очередь гарантированно свободна, и рецепт «DELETE, затем POST с новым конфигом»
     * не оставляет двух конкурирующих слушателей.
     */
    public boolean delete(String name) {
        Responder r = responders.remove(name);
        if (r == null) return false;
        r.stop();
        r.awaitTermination(10_000);
        return true;
    }

    /** Останавливает всех (shutdown hook): сначала сигнал всем, потом короткое ожидание. */
    public void stopAll() {
        responders.values().forEach(Responder::stop);
        responders.values().forEach(r -> r.awaitTermination(6_000));
        responders.clear();
    }
}
