package tech.ytsaurus.flow.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.rpc.TResponseData;
import tech.ytsaurus.flow.rpc.TState;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompanionMetricsTest {

    @Test
    void recordsResponseStateSizesByTypeAndName() {
        var registry = new SimpleMeterRegistry();
        var metrics = new CompanionMetrics(registry);
        var internalState = TState.newBuilder().setName("internal-state").build();
        var externalState = TState.newBuilder().setName("external-state").build();
        var response = TResponseData.newBuilder()
                .addInternalStates(internalState)
                .addExternalStates(externalState)
                .build();

        metrics.recordResponseStates("computation", response);

        var internalStateSize = registry.get(CompanionMetrics.STATE_SIZE)
                .tag("request_type", "process_batch")
                .tag("computation_id", "computation")
                .tag("direction", "response")
                .tag("state_type", "internal")
                .tag("state_name", "internal-state")
                .summary();
        assertEquals(1, internalStateSize.count());
        assertEquals(internalState.getSerializedSize(), internalStateSize.totalAmount());

        var externalStateSize = registry.get(CompanionMetrics.STATE_SIZE)
                .tag("request_type", "process_batch")
                .tag("computation_id", "computation")
                .tag("direction", "response")
                .tag("state_type", "external")
                .tag("state_name", "external-state")
                .summary();
        assertEquals(1, externalStateSize.count());
        assertEquals(externalState.getSerializedSize(), externalStateSize.totalAmount());
    }
}
