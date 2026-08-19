package tech.ytsaurus.flow.request;

import java.util.List;
import java.util.Map;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * ResponseContext class contains all request processing results which must be sent back to worker.
 */
public class ResponseContext implements YTreeConvertible {

    private final GUID jobId;
    private final GUID requestId;
    private final List<TransformResult> transformResults;
    private final Map<String, StatesHolder<InternalState>> internalStates;
    private final Map<String, StatesHolder<ExternalState>> externalStates;

    public ResponseContext(
            GUID jobId,
            GUID requestId,
            List<TransformResult> transformResults,
            Map<String, StatesHolder<InternalState>> internalStates,
            Map<String, StatesHolder<ExternalState>> externalStates
    ) {
        this.jobId = jobId;
        this.requestId = requestId;
        this.transformResults = transformResults;
        this.internalStates = internalStates;
        this.externalStates = externalStates;
    }

    public List<TransformResult> getTransformResults() {
        return transformResults;
    }

    public Map<String, StatesHolder<InternalState>> getInternalStates() {
        return internalStates;
    }

    public Map<String, StatesHolder<ExternalState>> getExternalStates() {
        return externalStates;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        var builder = YTree.builder().beginMap();
        builder.key("job_id").value(jobId.toString())
                .key("request_id").value(requestId.toString());
        builder.key("transform_results").beginList();
        for (var result : transformResults) {
            builder.value(result.toYTree());
        }
        builder.endList();
        builder.key("internal_states").beginMap();
        for (var entry : internalStates.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue().toYTree());
        }
        builder.endMap();
        builder.key("external_states").beginMap();
        for (var entry : externalStates.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue().toYTree());
        }
        builder.endMap();
        return builder.endMap().build();
    }
}
