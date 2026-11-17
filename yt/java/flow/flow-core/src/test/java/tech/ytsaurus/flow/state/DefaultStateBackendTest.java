package tech.ytsaurus.flow.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultStateBackendTest {

    @Test
    public void builderDefaultsDeclareNoStates() {
        var backend = DefaultStateBackend.builder().build();

        assertThrows(IllegalArgumentException.class, () -> backend.getOrCreateInternalStateHolder("counter"));
        assertThrows(IllegalArgumentException.class, () -> backend.getExternalStateHolder("external"));
        assertThrows(IllegalArgumentException.class, () -> backend.getJoinedExternalStateHolder("joined"));
    }

    @Test
    public void builderDefaultInternalStatesAcceptNewHolders() {
        var backend = DefaultStateBackend.builder()
                .setInternalStateNames(Set.of("counter"))
                .build();

        var holder = backend.getOrCreateInternalStateHolder("counter");

        assertEquals("counter", holder.getName());
        assertSame(holder, backend.getOrCreateInternalStateHolder("counter"));
    }

    @Test
    public void builderPassesEveryStateKind() {
        Map<String, StatesHolder> internal = new HashMap<>();
        var external = new StatesHolder("external", null);
        var joined = new StatesHolder("joined", null);
        var backend = DefaultStateBackend.builder()
                .setInternalStateNames(Set.of("counter"))
                .setExternalStateNames(Set.of("external"))
                .setJoinedExternalStateNames(Set.of("joined"))
                .setInternalStates(internal)
                .setExternalStates(Map.of("external", external))
                .setJoinedExternalStates(Map.of("joined", joined))
                .build();

        var created = backend.getOrCreateInternalStateHolder("counter");

        assertSame(created, internal.get("counter"));
        assertSame(external, backend.getExternalStateHolder("external"));
        assertSame(joined, backend.getJoinedExternalStateHolder("joined"));
    }
}
