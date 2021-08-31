package ru.yandex.yt.ytclient.proxy;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class OutageController {
    private final Map<String, Queue<Throwable>> requestTypeToErrors = new HashMap<>();

    /**
     * Planning more fails of 'requestType' queries.
     * So 'count' calls of 'requestType' queries will fail with error = 'exception'.
     * @return self
     */
    public OutageController addFails(String requestType, int count, Exception exception) {
        synchronized (this) {
            for (int idx = 0; idx < count; ++idx) {
                if (!requestTypeToErrors.containsKey(requestType)) {
                    requestTypeToErrors.put(requestType, new LinkedList<>());
                }
                requestTypeToErrors.get(requestType).add(Objects.requireNonNull(exception));
            }
        }
        return this;
    }

    /**
     * Cancel all scheduled fails.
     * @return self
     */
    public OutageController clear() {
        synchronized (this) {
            requestTypeToErrors.clear();
        }
        return this;
    }

    Optional<Throwable> pollError(String requestType) {
        Throwable error = null;
        synchronized (this) {
            if (requestTypeToErrors.containsKey(requestType)) {
                error = requestTypeToErrors.get(requestType).poll();
            }
        }
        return error != null ? Optional.of(error) : Optional.empty();
    }
}
