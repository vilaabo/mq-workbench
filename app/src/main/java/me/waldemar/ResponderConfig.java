package me.waldemar;

/**
 * Конфигурация респондера — фонового слушателя, который превращает WireMock в заглушку MQ-интеграции.
 * Null-поля заменяются дефолтами в {@link #normalized()}.
 */
public record ResponderConfig(
        String name,
        String requestQueue,
        String replyQueue,
        String wiremockUrl,
        String pathTemplate,
        String operationProperty,
        String correlation,
        Integer wiremockTimeoutSeconds,
        Boolean useRequestReplyToQueue
) {

    public static final String CORRELATION_AUTO = "auto";
    public static final String CORRELATION_MSG_ID = "msgId";
    public static final String CORRELATION_CORR_ID = "corrId";

    public ResponderConfig normalized() {
        String path = pathTemplate == null || pathTemplate.isBlank() ? "/mq/{operation}" : pathTemplate.trim();
        if (!path.startsWith("/")) path = "/" + path;
        return new ResponderConfig(
                name,
                requestQueue,
                replyQueue,
                wiremockUrl == null ? null : wiremockUrl.trim().replaceAll("/+$", ""),
                path,
                operationProperty == null || operationProperty.isBlank() ? "operation" : operationProperty,
                correlation == null || correlation.isBlank() ? CORRELATION_AUTO : correlation,
                wiremockTimeoutSeconds == null ? 10 : wiremockTimeoutSeconds,
                useRequestReplyToQueue == null || useRequestReplyToQueue);
    }

    /** Бросает IllegalArgumentException с понятным текстом, если конфигурация неполна. */
    public void validate() {
        require(name, "name");
        require(requestQueue, "requestQueue");
        require(wiremockUrl, "wiremockUrl");
        try {
            java.net.URI uri = java.net.URI.create(wiremockUrl);
            String scheme = uri.getScheme();
            if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                    || uri.getHost() == null) {
                throw new IllegalArgumentException(
                        "wiremockUrl должен быть полным http(s)-URL с хостом (например http://wiremock:8080), получено: " + wiremockUrl);
            }
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("wiremockUrl")) throw e;
            throw new IllegalArgumentException("wiremockUrl не разбирается как URL: " + wiremockUrl);
        }
        if (!CORRELATION_AUTO.equalsIgnoreCase(correlation)
                && !CORRELATION_MSG_ID.equalsIgnoreCase(correlation)
                && !CORRELATION_CORR_ID.equalsIgnoreCase(correlation)) {
            throw new IllegalArgumentException("correlation должен быть auto, msgId или corrId, получено: " + correlation);
        }
        if (wiremockTimeoutSeconds != null && (wiremockTimeoutSeconds < 1 || wiremockTimeoutSeconds > 120)) {
            throw new IllegalArgumentException("wiremockTimeoutSeconds должен быть в диапазоне 1..120");
        }
        if ((replyQueue == null || replyQueue.isBlank()) && Boolean.FALSE.equals(useRequestReplyToQueue)) {
            throw new IllegalArgumentException(
                    "Нужен replyQueue либо useRequestReplyToQueue=true (брать очередь ответа из MQMD ReplyToQ запроса)");
        }
    }

    private static void require(String v, String field) {
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Не задано обязательное поле респондера: " + field);
        }
    }
}
