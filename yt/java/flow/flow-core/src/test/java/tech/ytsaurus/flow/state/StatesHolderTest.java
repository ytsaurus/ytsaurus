package tech.ytsaurus.flow.state;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link StatesHolder} modified-state tracking: only states changed through
 * {@link StatesHolder#set} (accessor writes) are reported by
 * {@link StatesHolder#getModifiedStates}, while states populated from the request through
 * {@link StatesHolder#load} are not, so that unmodified states are not sent back.
 */
class StatesHolderTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addKey("k", TiType.string())
            .build();

    private static UnversionedRow key(String value) {
        return new PayloadBuilder(KEY_SCHEMA).set("k", value).finish().getRow();
    }

    private static InternalState state(String value) {
        return new InternalState(value.getBytes());
    }

    @Test
    @DisplayName("load() does not mark a state as modified")
    void loadDoesNotMarkModified() {
        var holder = new StatesHolder<InternalState>("s", KEY_SCHEMA, null);

        holder.load(key("a"), state("1"));
        holder.load(key("b"), state("2"));

        assertEquals(2, holder.getStates().size());
        assertTrue(holder.getModifiedStates().isEmpty());
    }

    @Test
    @DisplayName("set() marks a state as modified")
    void setMarksModified() {
        var holder = new StatesHolder<InternalState>("s", KEY_SCHEMA, null);

        holder.set(key("a"), state("1"));

        assertEquals(1, holder.getModifiedStates().size());
        assertTrue(holder.getModifiedStates().containsKey(key("a")));
    }

    @Test
    @DisplayName("only states modified after load are reported as modified")
    void onlyModifiedAfterLoadAreReported() {
        var holder = new StatesHolder<InternalState>("s", KEY_SCHEMA, null);

        holder.load(key("a"), state("1"));
        holder.load(key("b"), state("2"));
        // Accessor changes only key "a".
        holder.set(key("a"), state("11"));

        assertEquals(2, holder.getStates().size());
        var modifiedStates = holder.getModifiedStates();
        assertEquals(1, modifiedStates.size());
        assertTrue(modifiedStates.containsKey(key("a")));
        assertArrayEqualsState("11", holder.get(key("a")));
    }

    private static void assertArrayEqualsState(String expected, InternalState actual) {
        assertEquals(expected, new String(actual.getValue()));
    }
}
