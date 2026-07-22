package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;

public class PipelineSpecTestUtils {

    private PipelineSpecTestUtils() {
    }

    /**
     * Extracts stream information from a pipeline specification node.
     *
     * @param pipelineSpec the pipeline specification node containing stream data
     * @return a list of {@link StreamInfo} objects representing the extracted stream information
     */
    public static List<StreamInfo> extractStreamInfos(YTreeNode pipelineSpec) {
        var streamSpecs = pipelineSpec.asMap()
                .get("spec").asMap()
                .get("streams").asMap();
        return buildStreamInfos(streamSpecs);
    }

    /**
     * Builds stream info list from stream specs map.
     */
    public static List<StreamInfo> buildStreamInfos(Map<String, YTreeNode> streamSpecs) {
        long streamSpecId = 0;
        var streamInfos = new ArrayList<StreamInfo>();
        for (var streamSpec : streamSpecs.entrySet()) {
            var streamId = streamSpec.getKey();
            var schema = TableSchema.fromYTree(streamSpec.getValue().asMap().get("schema"));
            streamInfos.add(new StreamInfo(streamId, schema, streamSpecId++));
        }
        return streamInfos;
    }


    /**
     * Extracts group by schemas from a pipeline spec.
     *
     * @param pipelineSpec the pipeline spec node.
     * @return a map of computation IDs to their corresponding group by schemas.
     */
    public static Map<String, TableSchema> extractGroupBySchemas(YTreeNode pipelineSpec) {
        Map<String, YTreeNode> computationsSpec = pipelineSpec.asMap()
                .get("spec").asMap()
                .get("computations").asMap();
        Map<String, TableSchema> groupBySchemas = new HashMap<>();
        for (var spec : computationsSpec.entrySet()) {
            var computationId = spec.getKey();
            var groupBySchema = spec.getValue().asMap()
                    .get("group_by_schema");
            if (groupBySchema != null) {
                groupBySchemas.put(computationId, TableSchema.fromYTree(groupBySchema));
            }
        }
        return groupBySchemas;
    }
}
