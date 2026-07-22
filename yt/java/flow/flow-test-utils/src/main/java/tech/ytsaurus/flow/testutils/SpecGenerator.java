package tech.ytsaurus.flow.testutils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class SpecGenerator {
    public static final SchemaGenerator SCHEMA_GENERATOR = SchemaGenerator.builder().build();
    public static final TableSchema DEFAULT_GROUP_BY_SCHEMA = SCHEMA_GENERATOR.createStringSchema(1);

    private static final YTreeNode HASH_SCHEMA = YTree.builder().beginMap()
            .key("name").value("hash")
            .key("expression").value("farm_hash(hash_column)")
            .key("type").value("uint64")
            .key("required").value(true)
            .endMap()
            .build();
    private static final List<StreamInfo> INPUT_STREAMS_DEFAULTS = List.of(
            new StreamInfo("inputStreamId-0", SCHEMA_GENERATOR.createMixedSchema(20), 0L)
    );
    private static final List<StreamInfo> OUTPUT_STREAMS_DEFAULTS = List.of(
            new StreamInfo("outputStreamId-0", SCHEMA_GENERATOR.createMixedSchema(20), 1L)
    );
    private static final Map<String, String> PARAMETER_DEFAULTS = Map.of("wait_for_actions", "10s");
    private static final List<String> INTERNAL_STATE_DEFAULTS = List.of("internal-state-0");
    private static final Map<String, Map<String, String>> EXTERNAL_STATE_DEFAULTS = Map.of(
            "/external-state-0", Map.of("path", "//path/to/state")
    );
    private static final Map<String, String> DYNAMIC_PARAMETER_DEFAULTS = Map.of(
            "min_partition_count", "5",
            "max_partition_count", "10"
    );
    private final List<StreamInfo> inputStreams;
    private final List<StreamInfo> outputStreams;
    private final List<String> internalStates;
    private final Map<String, Map<String, String>> externalStates;
    private final Map<String, String> parameters;
    private final Map<String, String> dynamicParameters;
    private final TableSchema groupBySchema;

    SpecGenerator(Builder builder) {
        this.inputStreams = builder.inputStreamIds;
        this.outputStreams = builder.outputStreamIds;
        this.internalStates = builder.internalStates;
        this.externalStates = builder.externalStates;
        this.parameters = builder.parameters;
        this.dynamicParameters = builder.dynamicParameters;
        this.groupBySchema = builder.groupBySchema;
    }

    public static Builder builder() {
        return new Builder();
    }

    public YTreeNode spec() {
        var builder = YTree.builder().beginMap();
        builder.key("computation_class_name").value("NYT::NFlow::NCompanion::TTransformCompanionComputation");
        builder.key("group_by_schema")
                .beginList()
                .value(HASH_SCHEMA);
        groupBySchema.toYTree().listNode().forEach(builder::value);
        builder.endList();

        builder.key("input_stream_ids").beginList();
        inputStreams.forEach(streamInfo -> builder.value(streamInfo.getStreamId()));
        builder.endList();
        builder.key("output_stream_ids");
        builder.beginList();
        outputStreams.forEach(streamInfo -> builder.value(streamInfo.getStreamId()));
        builder.endList();
        builder.key("parameters").beginMap();
        parameters.forEach((paramName, paramVal) -> builder.key(paramName).value(paramVal));
        builder.key("internal_states").beginList();
        internalStates.forEach(builder::value);
        builder.endList();
        builder.endMap();
        // External states are declared at the top level of the computation spec via
        // `external_state_managers`. Each entry is keyed by the state name (a path-like string
        // starting with "/") and the value carries the manager configuration under `parameters`,
        // e.g. {"/state": {"parameters": {"path": "//placeholder"}}}.
        builder.key("external_state_managers").beginMap();
        for (Map.Entry<String, Map<String, String>> entry : externalStates.entrySet()) {
            String externalStateName = entry.getKey();
            builder.key(externalStateName).beginMap();
            builder.key("parameters").beginMap();
            Map<String, String> externalStateParameters = entry.getValue();
            externalStateParameters.forEach((paramName, paramVal) -> builder.key(paramName).value(paramVal));
            builder.endMap();
            builder.endMap();
        }
        builder.endMap();
        return builder.endMap().build();
    }

    public List<StreamInfo> getInputStreams() {
        return inputStreams;
    }

    public List<StreamInfo> getOutputStreams() {
        return outputStreams;
    }

    public List<StreamInfo> getStreams() {
        return Stream.concat(inputStreams.stream(), outputStreams.stream()).collect(Collectors.toList());
    }

    public YTreeNode dynamicSpec() {
        var builder = YTree.builder().beginMap();
        builder.key("parameters").beginMap();
        dynamicParameters.forEach((paramName, paramVal) -> builder.key(paramName).value(paramVal));
        builder.endMap();
        return builder.endMap().build();
    }


    public static class Builder {
        private List<StreamInfo> inputStreamIds = INPUT_STREAMS_DEFAULTS;
        private List<StreamInfo> outputStreamIds = OUTPUT_STREAMS_DEFAULTS;
        private List<String> internalStates = INTERNAL_STATE_DEFAULTS;
        private Map<String, Map<String, String>> externalStates = EXTERNAL_STATE_DEFAULTS;
        private Map<String, String> parameters = PARAMETER_DEFAULTS;
        private TableSchema groupBySchema = DEFAULT_GROUP_BY_SCHEMA;
        private Map<String, String> dynamicParameters = DYNAMIC_PARAMETER_DEFAULTS;

        public Builder setInputStreams(List<StreamInfo> inputStreamIds) {
            this.inputStreamIds = inputStreamIds;
            return this;
        }

        public Builder setInputStreamSchema(TableSchema schema) {
            if (inputStreamIds.size() == 1) {
                inputStreamIds.getFirst().schema = schema;
            } else {
                throw new IllegalStateException("Cannot set schema for multiple input streams");
            }
            return this;
        }

        public Builder setOutputStreams(List<StreamInfo> outputStreamIds) {
            this.outputStreamIds = outputStreamIds;
            return this;
        }

        public Builder setOutputStreamSchema(TableSchema schema) {
            if (outputStreamIds.size() == 1) {
                outputStreamIds.getFirst().schema = schema;
            } else {
                throw new IllegalStateException("Cannot set schema for multiple output streams");
            }
            return this;
        }

        public Builder setInternalStates(List<String> internalStates) {
            this.internalStates = internalStates;
            return this;
        }

        public Builder setExternalStates(Map<String, Map<String, String>> externalStates) {
            this.externalStates = externalStates;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder setGroupBySchema(TableSchema groupBySchema) {
            this.groupBySchema = groupBySchema;
            return this;
        }

        public Builder setDynamicParameters(Map<String, String> dynamicParameters) {
            this.dynamicParameters = dynamicParameters;
            return this;
        }

        public SpecGenerator build() {
            return new SpecGenerator(this);
        }
    }
}
