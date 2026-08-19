package tech.ytsaurus.flow.testutils;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.testutils.StateSeeder.CapturedSeed;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StateSeeder} in isolation: seeding a value / clear through a real accessor and
 * capturing the raw state, without a request builder or the harness.
 */
class StateSeederTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("count", TiType.int64())
            .build();

    private static Payload key() {
        return new PayloadBuilder(KEY_SCHEMA).set("k", "k1").finish();
    }

    @Test
    void capturesInternalSetAsSerializedBytes() {
        // raw(...) uses identity serialization, so the captured bytes are exactly what was set.
        var seed = StateSeeder.capture(
                StateDescriptors.raw("word-state-raw"), key(), acc -> acc.set(new byte[]{1, 2, 3}));

        assertEquals(CapturedSeed.Kind.INTERNAL, seed.kind());
        var state = assertInstanceOf(InternalState.class, seed.state());
        assertFalse(state.isReset());
        assertArrayEquals(new byte[]{1, 2, 3}, state.getValue());
    }

    @Test
    void capturesExternalSetAsPayload() {
        var value = new PayloadBuilder(STATE_SCHEMA).set("count", 7L).finish();

        var seed = StateSeeder.capture(
                StateDescriptors.external("/state"), key(), acc -> acc.set(value));

        assertEquals(CapturedSeed.Kind.EXTERNAL, seed.kind());
        var state = assertInstanceOf(ExternalState.class, seed.state());
        assertFalse(state.isReset());
        assertEquals(7L, state.getValue().get("count", Long.class));
    }

    @Test
    void capturesInternalClearAsReset() {
        var seed = StateSeeder.capture(
                StateDescriptors.raw("word-state-raw"), key(), StateAccessor::clear);

        assertEquals(CapturedSeed.Kind.INTERNAL, seed.kind());
        assertTrue(seed.state().isReset());
    }

    @Test
    void capturesExternalClearAsReset() {
        var seed = StateSeeder.capture(
                StateDescriptors.external("/state"), key(), StateAccessor::clear);

        assertEquals(CapturedSeed.Kind.EXTERNAL, seed.kind());
        assertTrue(seed.state().isReset());
    }

    @Test
    void serializesThroughTheDescriptorCodec() {
        // A yson descriptor must produce non-identity bytes: proves capture() runs the real codec
        // rather than storing the value raw.
        var seed = StateSeeder.capture(
                StateDescriptors.yson("count-state", Long.class), key(), acc -> acc.set(42L));

        assertEquals(CapturedSeed.Kind.INTERNAL, seed.kind());
        var state = assertInstanceOf(InternalState.class, seed.state());
        assertFalse(state.isReset());
        assertTrue(state.getValue().length > 0);
    }
}
