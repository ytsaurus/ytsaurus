package tech.ytsaurus.flow.service;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.rpc.TResponseData;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.testutils.ProtobufRequestBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompanionMetricsTest {

    @Test
    void recordsRequestItemCountsIncludingEmptyInputsAndRetries() {
        var registry = new SimpleMeterRegistry();
        var metrics = new CompanionMetrics(registry);
        var builder = new ProtobufRequestBuilder().setMessageCount(2).createProcessBatch().toBuilder();
        for (int i = 0; i < 3; i++) {
            builder.addTimersBuilder()
                    .setMessageId("timer-" + i)
                    .setSystemTimestamp(0)
                    .setStreamId(ByteString.copyFromUtf8("input"))
                    .setKey(ByteString.EMPTY)
                    .setTriggerTimestamp(0);
        }
        for (int i = 0; i < 4; i++) {
            builder.addVisitsBuilder()
                    .setMessageId("visit-" + i)
                    .setSystemTimestamp(0)
                    .setStreamId(ByteString.copyFromUtf8("input"))
                    .setKey(ByteString.EMPTY);
        }
        var request = builder.build();
        metrics.startProcessBatch(request).stop();
        metrics.startProcessBatch(request).stop();
        var emptyRequest = builder.clearMessages().clearTimers().clearVisits().build();
        metrics.startProcessBatch(emptyRequest).stop();

        assertRequestCount(registry, CompanionMetrics.REQUEST_MESSAGE_COUNT, 2);
        assertRequestCount(registry, CompanionMetrics.REQUEST_TIMER_COUNT, 3);
        assertRequestCount(registry, CompanionMetrics.REQUEST_VISIT_COUNT, 4);
        var size = registry.get(CompanionMetrics.REQUEST_SIZE).summary();
        assertEquals(3, size.count());
        assertEquals(2.0 * request.getSerializedSize() + emptyRequest.getSerializedSize(), size.totalAmount());
    }

    @Test
    void recordsStateSizesAndItemCountsForEveryWireSection() {
        var registry = new SimpleMeterRegistry();
        var metrics = new CompanionMetrics(registry);
        var internal = state("state", 2);
        var otherInternal = state("other-state", 4);
        var external = state("state", 3);
        var joined = state("state", 0);
        var modifiedInternal = state("state", 1);
        var modifiedExternal = state("state", 0);
        var request = new ProtobufRequestBuilder().setMessageCount(5).createProcessBatch().toBuilder()
                .addInternalStates(internal)
                .addInternalStates(otherInternal)
                .addExternalStates(external)
                .addJoinedExternalStates(joined)
                .build();
        metrics.startProcessBatch(request).stop();
        metrics.recordResponseStates(ProtobufRequestBuilder.COMPUTATION_ID, TResponseData.newBuilder()
                .addInternalStates(modifiedInternal)
                .addExternalStates(modifiedExternal)
                .build());

        // Absent states must not add zero samples to previously registered meters.
        metrics.startProcessBatch(new ProtobufRequestBuilder().setMessageCount(0).createProcessBatch()).stop();
        metrics.recordResponseStates(ProtobufRequestBuilder.COMPUTATION_ID, TResponseData.getDefaultInstance());

        assertStateMetrics(registry, "request", "internal", internal, 2);
        assertStateMetrics(registry, "request", "internal", otherInternal, 4);
        assertStateMetrics(registry, "request", "external", external, 3);
        assertStateMetrics(registry, "request", "joined_external", joined, 0);
        assertStateMetrics(registry, "response", "internal", modifiedInternal, 1);
        assertStateMetrics(registry, "response", "external", modifiedExternal, 0);
    }

    private static TState state(String name, int count) {
        var builder = TState.newBuilder().setName(name);
        for (int i = 0; i < count; i++) {
            // Reset entries, including the only modified internal entry, count too.
            builder.addStateItemsBuilder()
                    .setKey(ByteString.copyFromUtf8("key-" + i))
                    .setReset(i % 2 == 0);
        }
        return builder.build();
    }

    private static void assertRequestCount(SimpleMeterRegistry registry, String name, int maximum) {
        var summary = registry.get(name)
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .summary();
        assertEquals(3, summary.count());
        assertEquals(maximum, summary.max());
        assertEquals(2.0 * maximum, summary.totalAmount());
    }

    private static void assertStateMetrics(
            SimpleMeterRegistry registry, String direction, String type, TState state, int count
    ) {
        for (String name : new String[]{CompanionMetrics.STATE_SIZE, CompanionMetrics.STATE_ITEM_COUNT}) {
            var summary = registry.get(name)
                    .tag("request_type", "process_batch")
                    .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                    .tag("direction", direction)
                    .tag("state_type", type)
                    .tag("state_name", state.getName())
                    .summary();
            double expected = name.equals(CompanionMetrics.STATE_SIZE) ? state.getSerializedSize() : count;
            assertEquals(1, summary.count());
            assertEquals(expected, summary.totalAmount());
            assertEquals(expected, summary.max());
        }
    }
}
