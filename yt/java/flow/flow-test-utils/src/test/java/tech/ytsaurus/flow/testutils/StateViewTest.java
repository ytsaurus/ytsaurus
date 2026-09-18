package tech.ytsaurus.flow.testutils;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link StateView}: its accessors read without changing the viewed holders.
 */
class StateViewTest {
    private static final String STATE = "raw-state";

    private final TableSchema keySchema = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();

    @Test
    @DisplayName("getOrDefault() of a missing key does not add it to the view")
    void getOrDefaultDoesNotAddKeys() {
        var view = view();
        var key = new PayloadBuilder(keySchema).set("k", "k1").finish();

        var accessor = view.get(StateDescriptors.raw(STATE), key);
        assertEquals(1, accessor.getOrDefault(new byte[]{1}).length);
        assertNull(accessor.get());

        assertEquals(0, view.internalSize(STATE));
    }

    @Test
    @DisplayName("the view rejects writes")
    void rejectsWrites() {
        var key = new PayloadBuilder(keySchema).set("k", "k1").finish();
        var accessor = view().get(StateDescriptors.raw(STATE), key);

        assertThrows(UnsupportedOperationException.class, () -> accessor.set(new byte[]{1}));
        assertThrows(UnsupportedOperationException.class, accessor::clear);
    }

    private StateView view() {
        Map<String, StatesHolder> internal = new LinkedHashMap<>();
        internal.put(STATE, new StatesHolder(STATE, keySchema));
        return new StateView(Map.of(), internal, Map.of());
    }
}
