package tech.ytsaurus.flow.internal.computation;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.computation.TransformResult;

/**
 * A root collector for all child {@link OutputCollector} instances.
 */
public class RootCollector {

    /**
     * List of accumulated results grouped by parent message IDs.
     */
    private final List<TransformResult> results = new ArrayList<>();

    /**
     * Sets a single parent message ID. Avoids the varargs array and {@code List.of(String[])}
     * allocations of the {@link #setParentIds(String...)} overload on the per-message hot path.
     *
     * @param parentId A single message ID to track as parent.
     * @return A new {@link OutputCollector} instance that will associate added outputs with this parent.
     */
    public OutputCollector setParentIds(String parentId) {
        return new DefaultOutputCollector(this, new TransformResult(List.of(parentId)));
    }

    /**
     * Sets parent messages IDs.
     *
     * @param parentIds Array of message IDs to track as parents.
     * @return A new {@link OutputCollector} instance that will associate added outputs with these parent.
     */
    public OutputCollector setParentIds(String... parentIds) {
        return setParentIds(List.of(parentIds));
    }

    /**
     * Sets parent messages and timers IDs.
     *
     * @param parentIds List of message ids to track as parents.
     * @return A new {@link OutputCollector} instance that will associate added outputs with these parent.
     */
    public OutputCollector setParentIds(List<String> parentIds) {
        return new DefaultOutputCollector(this, new TransformResult(parentIds));
    }

    /**
     * Registers a {@link TransformResult} so it is returned from {@link #collectResults()}.
     * Called lazily by {@link DefaultOutputCollector} on the first add to keep filtered-out
     * (empty) results out of the result list.
     */
    void register(TransformResult result) {
        results.add(result);
    }

    /**
     * Collects and returns all accumulated output messages from all child collectors.
     *
     * @return A list of {@link TransformResult} objects containing the collected output messages and their parent
     * message IDs.
     */
    public List<TransformResult> collectResults() {
        return results;
    }
}
