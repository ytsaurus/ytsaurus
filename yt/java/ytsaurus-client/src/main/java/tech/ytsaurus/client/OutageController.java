package tech.ytsaurus.client;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class OutageController {
    private final Map<String, Queue<Optional<Throwable>>> requestTypeToErrors = new HashMap<>();
    private final Map<String, Queue<Duration>> requestTypeToDelays = new HashMap<>();
    private final Map<GUID, Optional<Throwable>> requestToError = new HashMap<>();

    /**
     * Planning more fails of 'requestType' queries.
     * So 'count' calls of 'requestType' queries will fail with error = 'exception'.
     *
     * @return self
     */
    public OutageController addFails(String requestType, int count, Throwable exception) {
        synchronized (this) {
            for (int idx = 0; idx < count; ++idx) {
                if (!requestTypeToErrors.containsKey(requestType)) {
                    requestTypeToErrors.put(requestType, new LinkedList<>());
                }
                requestTypeToErrors.get(requestType).add(Optional.of(Objects.requireNonNull(exception)));
            }
        }
        return this;
    }

    /**
     * Planning more OKs of 'requestType' queries.
     * So 'count' calls of 'requestType' queries won't fail
     *
     * @return self
     */
    public OutageController addOk(String requestType, int count) {
        synchronized (this) {
            for (int idx = 0; idx < count; ++idx) {
                if (!requestTypeToErrors.containsKey(requestType)) {
                    requestTypeToErrors.put(requestType, new LinkedList<>());
                }
                requestTypeToErrors.get(requestType).add(Optional.empty());
            }
        }
        return this;
    }

    /**
     * Planning more delays of 'requestType' queries.
     * So 'count' calls of 'requestType' queries will postpone with delay.
     *
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
     *
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

    Optional<Throwable> pollError(String requestType, GUID requestId) {
        Optional<Throwable> error = Optional.empty();
        synchronized (this) {
            if (requestToError.containsKey(requestId)) {
                error = requestToError.get(requestId);
            } else {
                if (requestTypeToErrors.containsKey(requestType)) {
                    Optional<Throwable> maybeError = requestTypeToErrors.get(requestType).poll();
                    if (maybeError != null) {
                        error = maybeError;
                    }
                }
                requestToError.put(requestId, error);
            }

        }
        return error;
    }
}
