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
    static final String REQUEST_MESSAGE_COUNT = "yt.flow.companion.request.message_count";
    static final String REQUEST_TIMER_COUNT = "yt.flow.companion.request.timer_count";
    static final String REQUEST_VISIT_COUNT = "yt.flow.companion.request.visit_count";
    static final String STATE_ITEM_COUNT = "yt.flow.companion.state.item_count";
    static final String REQUEST_DURATION = "yt.flow.companion.request.duration";
    static final String STATE_SIZE = "yt.flow.companion.state.size";
    static final String JOB_RECREATION_COUNT = "yt.flow.companion.job.recreation.count";

    private static final String PROCESS_BATCH = "process_batch";

    private final MeterRegistry registry;
    private final Map<String, RequestMeters> requestMeters = new ConcurrentHashMap<>();
    private final Map<StateMeterKey, StateMeters> stateMeters = new ConcurrentHashMap<>();

    CompanionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    RequestMeasurement startProcessBatch(TReqProcessBatch request) {
        String computationId = request.getComputationId();
        RequestMeters meters = requestMeters.computeIfAbsent(computationId, this::createRequestMeters);

        meters.requestCount().increment();
        meters.requestSize().record(request.getSerializedSize());
        meters.messageCount().record(request.getMessagesCount());
        meters.timerCount().record(request.getTimersCount());
        meters.visitCount().record(request.getVisitsCount());
        recordStates(request.getInternalStatesList(), computationId, "request", "internal");
        recordStates(request.getExternalStatesList(), computationId, "request", "external");
        recordStates(request.getJoinedExternalStatesList(), computationId, "request", "joined_external");

        return new RequestMeasurement(Timer.start(registry), meters.requestDuration());
    }

    void recordJobRecreation(String computationId) {
        requestMeters.computeIfAbsent(computationId, this::createRequestMeters)
                .jobRecreationCount()
                .increment();
    }

    void recordResponseStates(String computationId, TResponseData response) {
        recordStates(response.getInternalStatesList(), computationId, "response", "internal");
        recordStates(response.getExternalStatesList(), computationId, "response", "external");
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
                DistributionSummary.builder(REQUEST_MESSAGE_COUNT)
                        .description("Number of input messages per ProcessBatch request, including retries.")
                        .tags(tags)
                        .register(registry),
                DistributionSummary.builder(REQUEST_TIMER_COUNT)
                        .description("Number of input timers per ProcessBatch request, including retries.")
                        .tags(tags)
                        .register(registry),
                DistributionSummary.builder(REQUEST_VISIT_COUNT)
                        .description("Number of input visits per ProcessBatch request, including retries.")
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

    private void recordStates(
            List<TState> states,
            String computationId,
            String direction,
            String stateType
    ) {
        for (TState state : states) {
            StateMeterKey key = new StateMeterKey(computationId, direction, stateType, state.getName());
            StateMeters meters = stateMeters.computeIfAbsent(key, this::createStateMeters);
            meters.size().record(state.getSerializedSize());
            meters.itemCount().record(state.getStateItemsCount());
        }
    }

    private StateMeters createStateMeters(StateMeterKey key) {
        Tags tags = Tags.of(
                "request_type", PROCESS_BATCH,
                "computation_id", key.computationId(),
                "direction", key.direction(),
                "state_type", key.stateType(),
                "state_name", key.stateName()
        );
        return new StateMeters(
                DistributionSummary.builder(STATE_SIZE)
                        .description("Serialized size of states carried by ProcessBatch requests and responses.")
                        .baseUnit("bytes")
                        .tags(tags)
                        .register(registry),
                DistributionSummary.builder(STATE_ITEM_COUNT)
                        .description("Number of entries, including resets, in each state carried by ProcessBatch.")
                        .tags(tags)
                        .register(registry)
        );
    }

    private record StateMeters(DistributionSummary size, DistributionSummary itemCount) {
    }

    private record RequestMeters(
            Counter requestCount,
            DistributionSummary requestSize,
            DistributionSummary messageCount,
            DistributionSummary timerCount,
            DistributionSummary visitCount,
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
