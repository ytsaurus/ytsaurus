package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public abstract class PenaltyProvider {
    /**
     * Allow getting penalty for current cluster
     * @param clusterName: current cluster
     * @return penalty duration
     */
    abstract Duration getPenalty(String clusterName);

    /**
     * @return dummy penalty provider, always return zero penalty.
     */
    public static DummyPenaltyProvider.Builder dummyPenaltyProviderBuilder() {
        return new DummyPenaltyProvider.Builder();
    }

    @NonNullApi
    @NonNullFields
    static class DummyPenaltyProvider extends PenaltyProvider {
        DummyPenaltyProvider(Builder builder) {
        }

        Duration getPenalty(String clusterName) {
            return Duration.ZERO;
        }

        public static class Builder {
            public PenaltyProvider build() {
                return new DummyPenaltyProvider(this);
            }
        }
    }
}
