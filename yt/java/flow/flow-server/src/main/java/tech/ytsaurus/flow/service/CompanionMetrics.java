package tech.ytsaurus.flow.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TResponseData;
import tech.ytsaurus.flow.rpc.TState;

/**
 * Metrics for requests handled by the Java companion.
 */
final class CompanionMetrics {
    static final String REQUEST_COUNT = "yt.flow.companion.request.count";
    static final String REQUEST_SIZE = "yt.flow.companion.request.size";
    static final String REQUEST_DURATION = "yt.flow.companion.request.duration";
    static final String STATE_SIZE = "yt.flow.companion.state.size";
    static final String JOB_RECREATION_COUNT = "yt.flow.companion.job.recreation.count";

    private static final String PROCESS_BATCH = "process_batch";

    private final MeterRegistry registry;
    private final Map<String, RequestMeters> requestMeters = new ConcurrentHashMap<>();
    private final Map<StateMeterKey, DistributionSummary> stateSizeMeters = new ConcurrentHashMap<>();

    CompanionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    RequestMeasurement startProcessBatch(TReqProcessBatch request) {
        String computationId = request.getComputationId();
        RequestMeters meters = requestMeters.computeIfAbsent(computationId, this::createRequestMeters);

        meters.requestCount().increment();
        meters.requestSize().record(request.getSerializedSize());
        recordStateSizes(request.getInternalStatesList(), computationId, "request", "internal");
        recordStateSizes(request.getExternalStatesList(), computationId, "request", "external");
        recordStateSizes(request.getJoinedExternalStatesList(), computationId, "request", "joined_external");

        return new RequestMeasurement(Timer.start(registry), meters.requestDuration());
    }

    void recordJobRecreation(String computationId) {
        requestMeters.computeIfAbsent(computationId, this::createRequestMeters)
                .jobRecreationCount()
                .increment();
    }

    void recordResponseStates(String computationId, TResponseData response) {
        recordStateSizes(response.getInternalStatesList(), computationId, "response", "internal");
        recordStateSizes(response.getExternalStatesList(), computationId, "response", "external");
    }

    private RequestMeters createRequestMeters(String computationId) {
        Tags tags = Tags.of(
                "request_type", PROCESS_BATCH,
                "computation_id", computationId
        );
        return new RequestMeters(
                Counter.builder(REQUEST_COUNT)
                        .description("Number of ProcessBatch requests received by the Java companion.")
                        .tags(tags)
                        .register(registry),
                DistributionSummary.builder(REQUEST_SIZE)
                        .description("Serialized size of ProcessBatch requests received by the Java companion.")
                        .baseUnit("bytes")
                        .tags(tags)
                        .register(registry),
                Timer.builder(REQUEST_DURATION)
                        .description("Time spent processing ProcessBatch requests in the Java companion.")
                        .tags(tags)
                        .register(registry),
                Counter.builder(JOB_RECREATION_COUNT)
                        .description(
                                "Number of job recreation attempts using JobInfo embedded in ProcessBatch requests."
                        )
                        .tags(tags)
                        .register(registry)
        );
    }

    private void recordStateSizes(
            List<TState> states,
            String computationId,
            String direction,
            String stateType
    ) {
        for (TState state : states) {
            StateMeterKey key = new StateMeterKey(computationId, direction, stateType, state.getName());
            stateSizeMeters.computeIfAbsent(key, this::createStateSizeMeter)
                    .record(state.getSerializedSize());
        }
    }

    private DistributionSummary createStateSizeMeter(StateMeterKey key) {
        return DistributionSummary.builder(STATE_SIZE)
                .description("Serialized size of states carried by ProcessBatch requests and responses.")
                .baseUnit("bytes")
                .tags(
                        "request_type", PROCESS_BATCH,
                        "computation_id", key.computationId(),
                        "direction", key.direction(),
                        "state_type", key.stateType(),
                        "state_name", key.stateName()
                )
                .register(registry);
    }

    private record RequestMeters(
            Counter requestCount,
            DistributionSummary requestSize,
            Timer requestDuration,
            Counter jobRecreationCount
    ) {
    }

    private record StateMeterKey(
            String computationId,
            String direction,
            String stateType,
            String stateName
    ) {
    }

    static final class RequestMeasurement {
        private final Timer.Sample sample;
        private final Timer timer;

        private RequestMeasurement(Timer.Sample sample, Timer timer) {
            this.sample = sample;
            this.timer = timer;
        }

        void stop() {
            sample.stop(timer);
        }
    }
}
