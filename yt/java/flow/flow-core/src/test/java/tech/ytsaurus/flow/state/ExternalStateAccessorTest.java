package tech.ytsaurus.flow.state;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ExternalStateAccessor} over a schema-less {@link StatesHolder}: only
 * {@code getOrDefault()} requires a schema and throws when it is missing.
 */
class ExternalStateAccessorTest {

    private static final String STATE_NAME = "/state";
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("count", TiType.int64())
            .build();

    private static Payload key() {
        return new PayloadBuilder(KEY_SCHEMA).set("k", "k1").finish();
    }

    private static Payload value(long count) {
        return new PayloadBuilder(STATE_SCHEMA).set("count", count).finish();
    }

    private static ExternalStateAccessor accessor(StatesHolder<ExternalState> holder) {
        return new ExternalStateAccessor(key(), holder);
    }

    @Test
    void getReturnsEmptyOnSchemalessHolderWhenAbsent() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, null);
        var acc = accessor(holder);
        assertTrue(acc.get().isEmpty());
    }

    @Test
    void protoFormatHolderIsRejected() {
        // A proto-format holder stores null in the row payload slot, so a
        // row accessor over it would silently read every state as absent.
        var holder = new StatesHolder<ExternalState>(
                STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, "Some.Type");
        var exception = assertThrows(IllegalStateException.class, () -> accessor(holder));
        assertTrue(exception.getMessage().contains("proto wire format"));

        var readOnlyException = assertThrows(
                IllegalStateException.class,
                () -> new ReadOnlyExternalStateAccessor(key(), holder));
        assertTrue(readOnlyException.getMessage().contains("proto wire format"));
    }

    @Test
    void setThenGetRoundTripsThroughWireBytes() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, STATE_SCHEMA);
        var acc = accessor(holder);
        acc.set(value(7L));
        assertEquals(7L, acc.get().orElseThrow().get("count", Long.class));
    }

    @Test
    void repeatedGetDecodesOnce() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, STATE_SCHEMA);
        var acc = accessor(holder);
        acc.set(value(7L));
        assertSame(acc.get().orElseThrow(), acc.get().orElseThrow());
    }

    @Test
    void setThrowsOnSchemalessHolder() {
        // Values are stored as wire bytes encoded against the state schema; without one there is
        // nothing to encode with, and such a state could not be sent back to the worker anyway.
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, null);
        var acc = accessor(holder);
        assertThrows(UnsupportedOperationException.class, () -> acc.set(value(7L)));
    }

    @Test
    void clearWorksOnSchemalessHolder() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, null);
        var acc = accessor(holder);
        acc.clear();
        assertTrue(acc.get().isEmpty());
    }

    @Test
    void getOrDefaultThrowsWhenSchemalessAndAbsent() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, null);
        var acc = accessor(holder);
        assertThrows(UnsupportedOperationException.class, acc::getOrDefault);
    }

    @Test
    void getOrDefaultReturnsEmptyPayloadWhenSchemaPresentAndAbsent() {
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, STATE_SCHEMA);
        var acc = accessor(holder);
        Payload p = acc.getOrDefault();
        assertNotNull(p);
    }
}
