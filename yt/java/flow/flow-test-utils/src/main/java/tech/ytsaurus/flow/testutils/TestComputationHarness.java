package tech.ytsaurus.flow.testutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.io.CharStreams;
import com.hubspot.jinjava.Jinjava;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.request.mapper.JobProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.ResponseProtoMapper;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.service.CompanionRequestProcessor;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Test harness for executing Flow computations in a local, isolated environment.
 *
 * <p>Wraps a {@link CompanionRequestProcessor} and provides convenience methods
 * to invoke {@code doProcess} operations against a pipeline
 * specification without requiring a running Flow cluster. Use {@link #builder()}
 * to construct an instance.
 */
public class TestComputationHarness {

    private final CompanionRequestProcessor requestProcessor;
    private final YTreeNode pipelineSpec;
    private final PipelineContextSnapshot pipelineContextSnapshot;
    private final Map<String, TableSchema> externalStateSchemas;
    private final Map<String, TableSchema> groupBySchemas;
    private final ProtobufRequestConverter requestConverter;

    TestComputationHarness(
            CompanionRequestProcessor requestProcessor,
            YTreeNode pipelineSpec,
            PipelineContextSnapshot pipelineContextSnapshot,
            Map<String, TableSchema> externalStateSchemas,
            Map<String, TableSchema> groupBySchemas
    ) {
        this.requestProcessor = requestProcessor;
        this.pipelineSpec = pipelineSpec;
        this.pipelineContextSnapshot = pipelineContextSnapshot;
        this.externalStateSchemas = Collections.unmodifiableMap(externalStateSchemas);
        this.groupBySchemas = Collections.unmodifiableMap(groupBySchemas);
        // The converter uses the JVM-wide CodecRegistry singleton, which is consistent
        // with what the server-side mappers will use to decode.
        this.requestConverter = new ProtobufRequestConverter();
    }

    /**
     * Creates a new {@link Builder} for constructing a {@link TestComputationHarness}.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Executes a process-batch operation for the given request.
     *
     * <p>Converts the high-level {@link TestDoProcessRequest} into a protobuf request,
     * delegates to the underlying {@link CompanionRequestProcessor}, and maps the
     * result back into a {@link TestDoProcessResponse}.
     *
     * @param request the process request containing computation id, messages, timers, and states
     * @return the response with output messages, timers, and updated states
     * @throws RuntimeException if the underlying processor throws an exception
     */
    public TestDoProcessResponse doProcess(TestDoProcessRequest request) {
        var streamContext = pipelineContextSnapshot.getStreamContext();
        var protoRequest = requestConverter.createProcessBatch(
                request.getComputationId(),
                request.getMessages(),
                request.getTimers(),
                pipelineSpec,
                streamContext,
                request.getInternalStates(),
                request.getExternalStates(),
                request.getJoinedExternalStates(),
                externalStateSchemas,
                request.getWatermarks()
        );
        CompanionRequestProcessor.ProcessBatchResult processBatchResult;
        try {
            processBatchResult = requestProcessor.processBatch(protoRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Reconstruct Job from the proto request to get StreamSpecs and keySchema for deserialization.
        var jobMapper = new JobProtoMapper(streamContext);
        var job = jobMapper.fromProto(
                ProtoUtils.fromProto(protoRequest.getJobId()),
                protoRequest.getComputationId(),
                protoRequest.getJobInfo()
        );
        var responseMapper = new ResponseProtoMapper();
        var responseContext = responseMapper.fromProto(
                processBatchResult.getData(),
                job.getStreamSpecs(),
                job.getGroupBySchema(),
                ProtoUtils.fromProto(protoRequest.getJobId()),
                ProtoUtils.fromProto(protoRequest.getRequestId())
        );
        return new TestDoProcessResponse(
                responseContext,
                request.getExternalStates(),
                request.getInternalStates(),
                externalStateSchemas
        );
    }

    /**
     * Returns the immutable {@link PipelineContextSnapshot} associated with this harness.
     *
     * <p>The snapshot reflects the state of the underlying {@link PipelineContext} at the moment
     * {@link Builder#build()} was called, after any spec-derived streams were registered.
     *
     * @return the pipeline context snapshot
     */
    public PipelineContextSnapshot getPipelineContextSnapshot() {
        return pipelineContextSnapshot;
    }

    /**
     * Retrieves a {@link FlowStream} by stream id from the pipeline context snapshot.
     *
     * @param streamId the stream id.
     * @return the flow stream associated with the given id
     */
    public @Nullable FlowStream<?> getStream(String streamId) {
        return pipelineContextSnapshot.getStreamContext().getStream(streamId);
    }

    /**
     * Returns the group_by_schema {@link TableSchema} for the specified computation.
     *
     * @param computationId the computation identifier
     * @return the group_by_schema schema, or {@code null} if computation doesn't have a group_by_schema.
     */
    public @Nullable TableSchema getGroupBySchema(String computationId) {
        return groupBySchemas.get(computationId);
    }

    /**
     * Builder for constructing {@link TestComputationHarness} instances.
     *
     * <p>At minimum, {@link #setPipelineContext(PipelineContext)} and one of the
     * {@code setPipelineSpec} overloads must be called before {@link #build()}.
     */
    public static class Builder {
        private @Nullable PipelineContext pipelineContext;
        private @Nullable JobContext jobContext;
        private @Nullable String pipelineSpecStr;
        private @Nullable YTreeNode pipelineSpecYTree;
        private Map<String, TableSchema> externalStateSchemas = new HashMap<>();
        private @Nullable Map<String, Object> jinjaContext;

        /**
         * Sets the {@link PipelineContext} to use.
         *
         * @param pipelineContext the pipeline context (required)
         * @return this builder
         */
        public Builder setPipelineContext(PipelineContext pipelineContext) {
            this.pipelineContext = pipelineContext;
            return this;
        }

        /**
         * Sets a custom {@link JobContext}. If not set, a default context with a
         * 10-minute timeout is used.
         *
         * @param jobContext the job context
         * @return this builder
         */
        public Builder setJobContext(JobContext jobContext) {
            this.jobContext = jobContext;
            return this;
        }

        /**
         * Sets the pipeline specification from a pre-parsed {@link YTreeNode}.
         *
         * @param pipelineSpec the pipeline spec as a YSON tree node (required)
         * @return this builder
         */
        public Builder setPipelineSpec(YTreeNode pipelineSpec) {
            this.pipelineSpecYTree = pipelineSpec;
            this.pipelineSpecStr = null;
            return this;
        }

        /**
         * Sets the pipeline specification by deserializing the given YSON text string.
         *
         * @param pipelineSpec the pipeline spec as a YSON text string
         * @return this builder
         */
        public Builder setPipelineSpec(String pipelineSpec) {
            this.pipelineSpecStr = pipelineSpec;
            this.pipelineSpecYTree = null;
            return this;
        }

        /**
         * Sets the pipeline specification by deserializing from the given input stream.
         *
         * @param pipelineSpecInputStream an input stream containing the pipeline spec in YSON text format
         * @return this builder
         */
        public Builder setPipelineSpec(InputStream pipelineSpecInputStream) {
            return setPipelineSpec(new InputStreamReader(pipelineSpecInputStream, StandardCharsets.UTF_8));
        }

        /**
         * Sets the pipeline specification by reading from the given {@link Reader}.
         *
         * <p>The reader content is fully consumed and stored as a YSON text string.
         *
         * @param pipelineSpecReader a reader providing the pipeline spec in YSON text format
         * @return this builder
         * @throws UncheckedIOException if an I/O error occurs while reading
         */
        public Builder setPipelineSpec(Reader pipelineSpecReader) {
            try {
                this.pipelineSpecStr = CharStreams.toString(pipelineSpecReader);
                this.pipelineSpecYTree = null;
                return this;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Sets the Jinja template context used to render the pipeline specification.
         *
         * <p>Variables in this map are substituted into the pipeline spec string
         * via Jinja templating before YSON deserialization.
         *
         * @param context a map of variable names to their values
         * @return this builder
         */
        public Builder setJinjaContext(Map<String, Object> context) {
            this.jinjaContext = new HashMap<>(context);
            return this;
        }

        /**
         * Sets the Jinja template context by loading properties from the given input stream.
         *
         * @param contextInputStream an input stream containing key-value properties
         * @return this builder
         * @throws UncheckedIOException if an I/O error occurs while reading
         */
        public Builder setJinjaContext(InputStream contextInputStream) {
            return setJinjaContext(new InputStreamReader(contextInputStream, StandardCharsets.UTF_8));
        }

        /**
         * Sets the Jinja template context by loading properties from the given reader.
         *
         * <p>The reader content is parsed as a {@link Properties} file (key=value, or key: value format).
         *
         * @param contextReader a reader providing key-value properties
         * @return this builder
         * @throws UncheckedIOException if an I/O error occurs while reading
         */
        public Builder setJinjaContext(Reader contextReader) {
            try {
                var properties = new Properties();
                properties.load(contextReader);
                return setJinjaContext(properties);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Sets the Jinja template context from the given {@link Properties}.
         *
         * <p>Each property key-value pair is converted to a string-keyed map entry.
         *
         * @param contextProperties the properties to use as Jinja context variables
         * @return this builder
         */
        public Builder setJinjaContext(Properties contextProperties) {
            var context = new HashMap<String, Object>();
            contextProperties.forEach((key, value) -> context.put(key.toString(), value));
            this.jinjaContext = context;
            return this;
        }

        /**
         * Replaces all external state schemas with the provided map.
         *
         * @param externalStateSchemas a map from state name to its {@link TableSchema}
         * @return this builder
         */
        public Builder setExternalStateSchemas(Map<String, TableSchema> externalStateSchemas) {
            this.externalStateSchemas = new HashMap<>(externalStateSchemas);
            return this;
        }

        /**
         * Adds a single external state schema entry.
         *
         * @param name   the external state name
         * @param schema the table schema for the external state
         * @return this builder
         */
        public Builder addExternalStateSchema(String name, TableSchema schema) {
            externalStateSchemas.put(name, schema);
            return this;
        }

        /**
         * Builds and returns a new {@link TestComputationHarness}.
         *
         * <p>Extracts stream information and group_by_schema schemas from the pipeline spec,
         * registers streams in the pipeline context, and creates the underlying
         * {@link CompanionRequestProcessor}.
         *
         * @return a fully configured harness instance.
         * @throws IllegalStateException if the pipeline context or pipeline spec is not set.
         */
        public TestComputationHarness build() {
            if (pipelineContext == null) {
                throw new IllegalStateException("PipelineContext must be set");
            }
            if (pipelineSpecYTree == null && pipelineSpecStr == null) {
                throw new IllegalStateException("PipelineSpec must be set");
            }
            if (jobContext == null) {
                jobContext = new JobContext(Duration.ofMinutes(10));
            }

            YTreeNode pipelineSpec;
            if (pipelineSpecYTree != null) {
                pipelineSpec = pipelineSpecYTree;
            } else {
                String renderedSpecStr = jinjaContext != null
                        ? new Jinjava().render(pipelineSpecStr, jinjaContext)
                        : pipelineSpecStr;
                pipelineSpec = YTreeTextSerializer.deserialize(renderedSpecStr);
            }

            var streamInfos = PipelineSpecTestUtils.extractStreamInfos(pipelineSpec);
            for (var streamInfo : streamInfos) {
                var stream = FlowStreams.raw(streamInfo.getStreamId(), streamInfo.getSchema());
                pipelineContext.registerStreamIfAbsent(stream);
            }

            // Freeze the pipeline configuration: from now on the runtime sees an immutable
            // view, even if the caller mutates the original PipelineContext further.
            var snapshot = new PipelineContextSnapshot(pipelineContext);
            var requestProcessor = new CompanionRequestProcessor(snapshot, jobContext);
            var groupBySchemas = PipelineSpecTestUtils.extractGroupBySchemas(pipelineSpec);

            return new TestComputationHarness(
                    requestProcessor,
                    pipelineSpec,
                    snapshot,
                    externalStateSchemas,
                    groupBySchemas
            );
        }
    }

}
