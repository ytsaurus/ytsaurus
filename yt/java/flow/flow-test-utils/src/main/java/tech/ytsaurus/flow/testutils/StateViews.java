package tech.ytsaurus.flow.testutils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Reconstructs the two {@link StateView}s for a {@link TestDoProcessResponse}: {@code all} (the
 * states the request carried, with the computation's changes overlaid) and {@code modified} (only
 * what the computation changed). Both sides are read back off the wire, so a view never shows a
 * state the request could not carry.
 */
record StateViews(StateView all, StateView modified) {

    static StateViews from(
            Map<String, StatesHolder> responseExternal,
            Map<String, StatesHolder> responseInternal,
            Map<String, StatesHolder> requestExternal,
            Map<String, StatesHolder> requestInternal
    ) {
        var requestExternalSchemas = externalSchemasOf(requestExternal);
        var allView = new StateView(
                overlay(requestExternal, responseExternal),
                overlay(requestInternal, responseInternal),
                requestExternalSchemas);
        var modifiedView = new StateView(responseExternal, responseInternal, requestExternalSchemas);
        return new StateViews(allView, modifiedView);
    }

    /**
     * The schema the request declared per external state. The modified view holds only what the
     * computation wrote, so it needs these to describe the states it does not hold.
     */
    private static Map<String, TableSchema> externalSchemasOf(Map<String, StatesHolder> holders) {
        var result = new LinkedHashMap<String, TableSchema>(holders.size());
        for (var entry : holders.entrySet()) {
            var schema = entry.getValue().getStateSchema();
            if (schema != null) {
                result.put(entry.getKey(), schema);
            }
        }
        return result;
    }

    /**
     * Merges the request's states with the ones the computation modified, the modified entry
     * winning for a key present in both.
     */
    private static Map<String, StatesHolder> overlay(
            Map<String, StatesHolder> request,
            Map<String, StatesHolder> modified
    ) {
        var names = new LinkedHashSet<>(request.keySet());
        names.addAll(modified.keySet());

        var result = new LinkedHashMap<String, StatesHolder>(names.size());
        for (var name : names) {
            var requestHolder = request.get(name);
            var modifiedHolder = modified.get(name);
            // Both sides describe the state identically. The request describes what it carried;
            // a state the computation created itself is described only by the response.
            var described = requestHolder != null ? requestHolder : modifiedHolder;
            var merged = new StatesHolder(
                    name,
                    null,
                    described.getStateSchema(),
                    described.getFormat(),
                    described.getProtoType());
            if (requestHolder != null) {
                requestHolder.getStates().forEach(merged::load);
            }
            if (modifiedHolder != null) {
                modifiedHolder.getStates().forEach(merged::load);
            }
            result.put(name, merged);
        }
        return result;
    }
}
