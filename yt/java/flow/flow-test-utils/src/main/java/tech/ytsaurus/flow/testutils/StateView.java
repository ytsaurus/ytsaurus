package tech.ytsaurus.flow.testutils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Read-only query view over one snapshot of computation state. A {@link TestDoProcessResponse}
 * exposes two: the full states ({@link TestDoProcessResponse#allStates()}) and only the states the
 * computation modified ({@link TestDoProcessResponse#modifiedStates()}).
 */
public final class StateView {
    private final Map<String, StatesHolder> externalHolders;
    private final Map<String, StatesHolder> internalHolders;
    private final SnapshotStateBackend backend;

    StateView(
            Map<String, StatesHolder> externalHolders,
            Map<String, StatesHolder> internalHolders,
            Map<String, TableSchema> requestExternalSchemas
    ) {
        this.externalHolders = externalHolders;
        this.internalHolders = internalHolders;
        // Backend gets shallow copies so the empty holders it creates for unknown state names stay
        // out of the metadata maps. Sharing the holder instances is safe because the accessors
        // handed out are read-only views, which never add entries. Those fabricated holders take
        // the schema the request declared, so a state missing from this view — one the computation
        // never modified, say — still reads with a default value, as it does in the computation.
        this.backend = new SnapshotStateBackend(
                new LinkedHashMap<>(internalHolders), new LinkedHashMap<>(externalHolders),
                requestExternalSchemas, null);
    }

    /**
     * Returns a read-only accessor for {@code descriptor} at {@code key}.
     *
     * @param <T> state value type.
     */
    public <T> StateAccessor<T> get(StateDescriptor<T> descriptor, Payload key) {
        return backend.accessor(descriptor, key).readOnly();
    }

    /**
     * Returns the external state names in this view.
     */
    public Set<String> externalNames() {
        return Collections.unmodifiableSet(externalHolders.keySet());
    }

    /**
     * Returns the internal state names in this view.
     */
    public Set<String> internalNames() {
        return Collections.unmodifiableSet(internalHolders.keySet());
    }

    /**
     * Returns the keys present for the given external state name (empty if unknown).
     */
    public Set<UnversionedRow> externalKeys(String stateName) {
        return keysOf(externalHolders, stateName);
    }

    /**
     * Returns the keys present for the given internal state name (empty if unknown).
     */
    public Set<UnversionedRow> internalKeys(String stateName) {
        return keysOf(internalHolders, stateName);
    }

    /**
     * Returns the number of external state entries for the given state name (0 if unknown).
     */
    public int externalSize(String stateName) {
        return externalKeys(stateName).size();
    }

    /**
     * Returns the number of internal state entries for the given state name (0 if unknown).
     */
    public int internalSize(String stateName) {
        return internalKeys(stateName).size();
    }

    private static Set<UnversionedRow> keysOf(Map<String, StatesHolder> holders, String stateName) {
        StatesHolder holder = holders.get(stateName);
        return holder == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(holder.getStates().keySet());
    }
}
