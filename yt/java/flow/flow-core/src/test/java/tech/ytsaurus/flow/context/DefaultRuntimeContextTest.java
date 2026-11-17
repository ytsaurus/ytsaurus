package tech.ytsaurus.flow.context;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.flow.state.StateBackend;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class DefaultRuntimeContextTest {

    private static class NoopResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    @Test
    public void builderDefaultsToAnEmptyContext() {
        var context = DefaultRuntimeContext.builder()
                .setStateBackend(mock(StateBackend.class))
                .build();

        assertEquals(0L, context.getEpochInputEventWatermark());
        assertNull(context.getEpochEventWatermark("input"));
        assertNull(context.getEpochSystemWatermark("input"));
        assertTrue(context.getComputationParameters().isEmpty());
        assertTrue(context.getComputationDynamicParameters().isEmpty());
        assertNull(context.getStreamSpecs().getStream("input"));
        assertThrows(IllegalArgumentException.class, () -> context.createMessageBuilder("input"));
        assertThrows(IllegalStateException.class, () -> context.getResource("view"));
    }

    @Test
    public void builderPassesEveryValue() {
        var streamSpecs = new StreamSpecs(StreamIdsMapping.builder().build(), List.of());
        var resource = new NoopResource();
        var context = DefaultRuntimeContext.builder()
                .setStateBackend(mock(StateBackend.class))
                .setStreamSpecs(streamSpecs)
                .setWatermarks(Map.of("input", 100L))
                .setSystemWatermarks(Map.of("input", 110L))
                .setMinWatermark(100L)
                .setComputationParameters(Map.of("limit", YTree.integerNode(7)))
                .setComputationDynamicParameters(Map.of("rate", YTree.integerNode(3)))
                .setResources(Map.of("view", resource))
                .build();

        assertSame(streamSpecs, context.getStreamSpecs());
        assertEquals(100L, context.getEpochEventWatermark("input"));
        assertEquals(110L, context.getEpochSystemWatermark("input"));
        assertEquals(100L, context.getEpochInputEventWatermark());
        assertEquals(7, context.getComputationParameters().get("limit").intValue());
        assertEquals(3, context.getComputationDynamicParameters().get("rate").intValue());
        assertSame(resource, context.getResource("view"));
    }

    @Test
    public void builderRequiresAStateBackend() {
        var error = assertThrows(NullPointerException.class, () -> DefaultRuntimeContext.builder().build());
        assertTrue(error.getMessage().contains("stateBackend"));
    }
}
