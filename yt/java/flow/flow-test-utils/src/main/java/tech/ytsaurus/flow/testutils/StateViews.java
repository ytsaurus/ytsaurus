package tech.ytsaurus.flow.testutils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Reconstructs the two {@link StateView}s for a {@link TestDoProcessResponse} from the raw response
 * and request states: {@code all} (request states with the computation's changes overlaid) and
 * {@code modified} (only what the computation changed).
 */
record StateViews(StateView all, StateView modified) {

    static StateViews from(
            Map<String, StatesHolder> responseExternal,
            Map<String, StatesHolder> responseInternal,
            Map<String, Map<Payload, State>> loadedExternal,
            Map<String, Map<Payload, State>> loadedInternal,
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
    private static Map<String, Map<UnversionedRow, State>> buildModified(
            Map<String, StatesHolder> responseHolders
    ) {
        var result = new LinkedHashMap<String, Map<UnversionedRow, State>>(responseHolders.size());
        for (var entry : responseHolders.entrySet()) {
            result.put(entry.getKey(), new LinkedHashMap<>(entry.getValue().getStates()));
        }
        return result;
    }

    /**
     * Merges the request's loaded states with the modified states (modified entries win), keyed by
     * {@link UnversionedRow}.
     */
    private static Map<String, Map<UnversionedRow, State>> buildAll(
            Map<String, Map<Payload, State>> loaded,
            Map<String, Map<UnversionedRow, State>> modified
    ) {
        var names = new LinkedHashSet<String>();
        names.addAll(loaded.keySet());
        names.addAll(modified.keySet());

        var result = new LinkedHashMap<String, Map<UnversionedRow, State>>(names.size());
        for (var name : names) {
            var merged = new LinkedHashMap<UnversionedRow, State>();
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

    private static Map<String, StatesHolder> toExternalHolders(
            Map<String, Map<UnversionedRow, State>> states,
            Map<String, TableSchema> externalStateSchemas
    ) {
        var result = new LinkedHashMap<String, StatesHolder>(states.size());
        for (var entry : states.entrySet()) {
            var name = entry.getKey();
            var holder = new StatesHolder(name, null, externalStateSchemas.get(name));
            entry.getValue().forEach(holder::load);
            result.put(name, holder);
        }
        return result;
    }

    private static Map<String, StatesHolder> toInternalHolders(
            Map<String, Map<UnversionedRow, State>> states
    ) {
        var result = new LinkedHashMap<String, StatesHolder>(states.size());
        for (var entry : states.entrySet()) {
            var holder = new StatesHolder(entry.getKey(), null, null);
            entry.getValue().forEach(holder::load);
            result.put(entry.getKey(), holder);
        }
        return result;
    }
}
