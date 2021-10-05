package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
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
    private final Map<String, Queue<Duration>> requestTypeToDelays = new HashMap<>();

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
     * Planning more delays of 'requestType' queries.
     * So 'count' calls of 'requestType' queries will postpone with delay.
     * @return self
     */
    public OutageController addDelays(String requestType, int count, Duration delay) {
        synchronized (this) {
            for (int idx = 0; idx < count; ++idx) {
                if (!requestTypeToDelays.containsKey(requestType)) {
                    requestTypeToDelays.put(requestType, new LinkedList<>());
                }
                requestTypeToDelays.get(requestType).add(Objects.requireNonNull(delay));
            }
        }
        return this;
    }

    /**
     * Cancel all scheduled fails and delays.
     * @return self
     */
    public OutageController clear() {
        synchronized (this) {
            requestTypeToErrors.clear();
            requestTypeToDelays.clear();
        }
        return this;
    }

    Optional<Duration> pollDelay(String requestType) {
        Duration delay = null;
        synchronized (this) {
            if (requestTypeToDelays.containsKey(requestType)) {
                delay = requestTypeToDelays.get(requestType).poll();
            }
        }
        return delay != null ? Optional.of(delay) : Optional.empty();
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
