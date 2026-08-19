package tech.ytsaurus.flow.testutils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Reconstructs the two {@link StateView}s for a {@link TestDoProcessResponse} from the raw response
 * and request states: {@code all} (request states with the computation's changes overlaid) and
 * {@code modified} (only what the computation changed).
 */
record StateViews(StateView all, StateView modified) {

    static StateViews from(
            Map<String, StatesHolder<ExternalState>> responseExternal,
            Map<String, StatesHolder<InternalState>> responseInternal,
            Map<String, Map<Payload, ExternalState>> loadedExternal,
            Map<String, Map<Payload, InternalState>> loadedInternal,
            Map<String, TableSchema> externalStateSchemas
    ) {
        var modifiedExternal = buildModified(responseExternal);
        var modifiedInternal = buildModified(responseInternal);
        var allExternal = buildAll(loadedExternal, modifiedExternal);
        var allInternal = buildAll(loadedInternal, modifiedInternal);

        var allView = new StateView(
                toExternalHolders(allExternal, externalStateSchemas),
                toInternalHolders(allInternal),
                externalStateSchemas);
        var modifiedView = new StateView(
                toExternalHolders(modifiedExternal, externalStateSchemas),
                toInternalHolders(modifiedInternal),
                externalStateSchemas);
        return new StateViews(allView, modifiedView);
    }

    /**
     * Snapshots the modified states into a per-name map keyed by {@link UnversionedRow}.
     */
    private static <T extends State<?>> Map<String, Map<UnversionedRow, T>> buildModified(
            Map<String, StatesHolder<T>> responseHolders
    ) {
        var result = new LinkedHashMap<String, Map<UnversionedRow, T>>(responseHolders.size());
        for (var entry : responseHolders.entrySet()) {
            result.put(entry.getKey(), new LinkedHashMap<>(entry.getValue().getStates()));
        }
        return result;
    }

    /**
     * Merges the request's loaded states with the modified states (modified entries win), keyed by
     * {@link UnversionedRow}.
     */
    private static <T extends State<?>> Map<String, Map<UnversionedRow, T>> buildAll(
            Map<String, Map<Payload, T>> loaded,
            Map<String, Map<UnversionedRow, T>> modified
    ) {
        var names = new LinkedHashSet<String>();
        names.addAll(loaded.keySet());
        names.addAll(modified.keySet());

        var result = new LinkedHashMap<String, Map<UnversionedRow, T>>(names.size());
        for (var name : names) {
            var merged = new LinkedHashMap<UnversionedRow, T>();
            var loadedForName = loaded.get(name);
            if (loadedForName != null) {
                for (var loadedEntry : loadedForName.entrySet()) {
                    merged.put(loadedEntry.getKey().getRow(), loadedEntry.getValue());
                }
            }
            var modifiedForName = modified.get(name);
            if (modifiedForName != null) {
                merged.putAll(modifiedForName);
            }
            result.put(name, merged);
        }
        return result;
    }

    private static Map<String, StatesHolder<ExternalState>> toExternalHolders(
            Map<String, Map<UnversionedRow, ExternalState>> states,
            Map<String, TableSchema> externalStateSchemas
    ) {
        var result = new LinkedHashMap<String, StatesHolder<ExternalState>>(states.size());
        for (var entry : states.entrySet()) {
            var name = entry.getKey();
            var holder = new StatesHolder<ExternalState>(
                    name, null, externalSchema(name, externalStateSchemas, entry.getValue()));
            entry.getValue().forEach(holder::load);
            result.put(name, holder);
        }
        return result;
    }

    private static Map<String, StatesHolder<InternalState>> toInternalHolders(
            Map<String, Map<UnversionedRow, InternalState>> states
    ) {
        var result = new LinkedHashMap<String, StatesHolder<InternalState>>(states.size());
        for (var entry : states.entrySet()) {
            var holder = new StatesHolder<InternalState>(entry.getKey(), null, null);
            entry.getValue().forEach(holder::load);
            result.put(entry.getKey(), holder);
        }
        return result;
    }

    /**
     * Resolves the schema for an external state: the user-provided schema wins, otherwise it is
     * derived from the first stored non-reset payload.
     */
    private static @Nullable TableSchema externalSchema(
            String name,
            Map<String, TableSchema> externalStateSchemas,
            Map<UnversionedRow, ExternalState> states
    ) {
        TableSchema schema = externalStateSchemas.get(name);
        if (schema != null) {
            return schema;
        }
        for (var value : states.values()) {
            if (!value.isReset() && value.getValue() != null) {
                return value.getValue().getSchema();
            }
        }
        return null;
    }
}
