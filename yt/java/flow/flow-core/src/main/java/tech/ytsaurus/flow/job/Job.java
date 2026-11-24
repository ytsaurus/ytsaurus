package tech.ytsaurus.flow.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
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
    // Input streams of the static spec; null if the spec does not list them.
    private final @Nullable List<String> inputStreamIds;
    // Exact direct and transitive companion resource instances required by the job.
    private final List<CompanionResourceInstanceReference> companionResources;
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
        this(jobId, computationId, streamSpecs, staticSpec, dynamicSpec, groupBySchema, List.of());
    }

    public Job(
            GUID jobId,
            String computationId,
            StreamSpecs streamSpecs,
            YTreeNode staticSpec,
            YTreeNode dynamicSpec,
            @Nullable TableSchema groupBySchema,
            List<CompanionResourceInstanceReference> companionResources
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
        this.inputStreamIds = extractInputStreamIds(staticSpec);
        this.companionResources = List.copyOf(companionResources);
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

    private static @Nullable List<String> extractInputStreamIds(YTreeNode computationSpec) {
        YTreeNode streamIds = computationSpec.asMap().get("input_stream_ids");
        if (streamIds == null) {
            return null;
        }
        return streamIds.asList().stream().map(YTreeNode::stringValue).toList();
    }

    /**
     * Minimum event watermark across the input streams of the static spec.
     *
     * <p>As in the C++ runtime, an input stream without a watermark counts as {@code 0}. A
     * computation without input streams gets {@link Long#MAX_VALUE}: the C++ value is
     * {@code 2^64 - 1}, which a signed {@code long} cannot hold, so watermarks of
     * {@code 2^63} and above saturate to {@link Long#MAX_VALUE} as well. A spec that does not
     * list {@code input_stream_ids} falls back to the minimum of all the given watermarks, or
     * {@code 0} if there are none.
     *
     * @param watermarks event watermarks of the request keyed by stream id
     * @return the minimum event watermark
     */
    public long getMinWatermark(Map<String, Long> watermarks) {
        if (inputStreamIds == null) {
            return watermarks.values().stream()
                    .mapToLong(Job::saturate)
                    .min()
                    .orElse(0L);
        }
        long minWatermark = Long.MAX_VALUE;
        for (var streamId : inputStreamIds) {
            minWatermark = Math.min(minWatermark, saturate(watermarks.getOrDefault(streamId, 0L)));
        }
        return minWatermark;
    }

    // A uint64 watermark above Long.MAX_VALUE arrives as a negative long.
    private static long saturate(long watermark) {
        return watermark < 0 ? Long.MAX_VALUE : watermark;
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

    /**
     * Exact direct and transitive companion resource instances required by the job.
     */
    public List<CompanionResourceInstanceReference> getCompanionResources() {
        return companionResources;
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
                .key("companion_resource_ids").value(companionResourceIds())
                .endMap()
                .build();
    }

    private List<String> companionResourceIds() {
        List<String> resourceIds = new ArrayList<>(companionResources.size());
        for (CompanionResourceInstanceReference reference : companionResources) {
            resourceIds.add(reference.resourceId());
        }
        return resourceIds;
    }
}
