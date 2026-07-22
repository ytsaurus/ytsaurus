package tech.ytsaurus.flow.job;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

public class Job implements YTreeConvertible {
    // Common parameters, duplicated with Request.
    private final GUID jobId;
    private final String computationId;
    // Static part of the job.
    private final StreamSpecs streamSpecs;
    private final YTreeNode staticSpec;
    private final Map<String, YTreeNode> staticParameters;
    private final @Nullable TableSchema groupBySchema;
    // States names.
    private final Set<String> internalStatesNames;
    private final Set<String> externalStatesNames;
    private final Set<String> externalStateJoinersNames;
    // Dynamic part of the job. Might be updated.
    private YTreeNode dynamicSpec;
    private Map<String, YTreeNode> dynamicParameters;

    public Job(
            GUID jobId,
            String computationId,
            StreamSpecs streamSpecs,
            YTreeNode staticSpec,
            YTreeNode dynamicSpec,
            @Nullable TableSchema groupBySchema
    ) {
        this.jobId = jobId;
        this.computationId = computationId;
        this.streamSpecs = streamSpecs;
        this.staticSpec = staticSpec;
        this.dynamicSpec = dynamicSpec;
        this.staticParameters = extractParameters(staticSpec);
        this.dynamicParameters = extractParameters(dynamicSpec);
        this.groupBySchema = groupBySchema;
        this.internalStatesNames = extractInternalStates(staticParameters);
        this.externalStatesNames = extractExternalStates(staticSpec);
        this.externalStateJoinersNames = extractExternalStateJoiners(staticSpec);
    }

    static Map<String, YTreeNode> extractParameters(YTreeNode computationSpec) {
        return Optional.ofNullable(computationSpec.asMap().get("parameters"))
                .map(YTreeNode::asMap)
                .orElseGet(Collections::emptyMap);
    }

    static Set<String> extractInternalStates(Map<String, YTreeNode> parameters) {
        YTreeNode stateNamesParameter = parameters.get("internal_states");
        if (stateNamesParameter != null) {
            return stateNamesParameter
                    .asList()
                    .stream()
                    .map(YTreeNode::stringValue)
                    .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    /**
     * Extracts declared external state names from the computation spec.
     *
     * <p>External states are declared via the {@code external_state_managers} field at the top
     * level of the computation spec. Each key is the external state name (a path-like string
     * starting with {@code "/"}), and the value is the manager configuration, e.g.:
     * <pre>{@code
     *   external_state_managers = {
     *       "/state" = {
     *           parameters = { path = "//placeholder"; };
     *       };
     *   };
     * }</pre>
     */
    static Set<String> extractExternalStates(YTreeNode computationSpec) {
        YTreeNode managers = computationSpec.asMap().get("external_state_managers");
        if (managers != null) {
            return new HashSet<>(managers.asMap().keySet());
        }
        return Collections.emptySet();
    }

    /**
     * Extracts declared read-only external state joiner names from the computation spec.
     *
     * <p>Joiners are declared via the {@code external_state_joiners} field at the top level of the
     * computation spec, mirroring {@code external_state_managers}. A joiner reads another
     * computation's state table without owning it.
     */
    static Set<String> extractExternalStateJoiners(YTreeNode computationSpec) {
        YTreeNode joiners = computationSpec.asMap().get("external_state_joiners");
        if (joiners != null) {
            return new HashSet<>(joiners.asMap().keySet());
        }
        return Collections.emptySet();
    }

    public GUID getJobId() {
        return jobId;
    }

    public String getComputationId() {
        return computationId;
    }

    public StreamSpecs getStreamSpecs() {
        return streamSpecs;
    }

    public YTreeNode getStaticSpec() {
        return staticSpec;
    }

    public YTreeNode getDynamicSpec() {
        return dynamicSpec;
    }

    public void setDynamicSpec(YTreeNode dynamicSpec) {
        this.dynamicSpec = dynamicSpec;
        this.dynamicParameters = extractParameters(dynamicSpec);
    }

    public Map<String, YTreeNode> getStaticParameters() {
        return staticParameters;
    }

    public Map<String, YTreeNode> getDynamicParameters() {
        return dynamicParameters;
    }

    public @Nullable TableSchema getGroupBySchema() {
        return groupBySchema;
    }

    public Set<String> getInternalStatesNames() {
        return internalStatesNames;
    }

    public Set<String> getExternalStatesNames() {
        return externalStatesNames;
    }

    public Set<String> getExternalStateJoinersNames() {
        return externalStateJoinersNames;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        return YTree.builder()
                .beginMap()
                .key("job_id").value(jobId.toString())
                .key("computation_id").value(computationId)
                .key("stream_specs").value(streamSpecs.toYTree())
                .key("static_spec").value(staticSpec)
                .key("dynamic_spec").value(dynamicSpec)
                .key("static_parameters").value(staticParameters)
                .key("dynamic_parameters").value(dynamicParameters)
                .endMap()
                .build();
    }
}
