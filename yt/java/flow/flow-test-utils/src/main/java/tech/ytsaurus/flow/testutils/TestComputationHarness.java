package tech.ytsaurus.flow.testutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.io.CharStreams;
import com.google.protobuf.ByteString;
import com.hubspot.jinjava.Jinjava;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.request.mapper.ExternalStateProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.InternalStateProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.JobProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.ResponseProtoMapper;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.EResourceCommand;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.flow.rpc.TCompanionResourceInstanceReference;
import tech.ytsaurus.flow.service.CompanionRequestProcessor;
import tech.ytsaurus.flow.service.ResourceStore;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Test harness for executing Flow computations in a local, isolated environment.
 *
 * <p>Wraps a {@link CompanionRequestProcessor} and provides convenience methods
 * to invoke {@code doProcess} operations against a pipeline
 * specification without requiring a running Flow cluster. Use {@link #builder()}
 * to construct an instance, and {@link #close()} — try-with-resources works — to
 * unload the companion resources it loaded.
 */
public class TestComputationHarness implements AutoCloseable {

    /**
     * Incarnation the harness loads its companion resources under; the harness never advances it.
     */
    private static final GUID RESOURCE_INCARNATION_ID = GUID.valueOf("1-1-1-1");

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
            Map<String, TableSchema> groupBySchemas,
            List<TCompanionResourceInstanceReference> companionResources
    ) {
        this.requestProcessor = requestProcessor;
        this.pipelineSpec = pipelineSpec;
        this.pipelineContextSnapshot = pipelineContextSnapshot;
        this.externalStateSchemas = Collections.unmodifiableMap(externalStateSchemas);
        this.groupBySchemas = Collections.unmodifiableMap(groupBySchemas);
        // The converter uses the JVM-wide CodecRegistry singleton, which is consistent
        // with what the server-side mappers will use to decode.
        this.requestConverter = new ProtobufRequestConverter().setCompanionResources(companionResources);
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
        request.seedStates(externalStateSchemas);
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
                request.getProtoStateTypes(),
                request.getWatermarks()
        );
        CompanionRequestProcessor.ProcessBatchResult processBatchResult;
        try {
            processBatchResult = requestProcessor.processBatch(protoRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        var jobId = ProtoUtils.fromProto(protoRequest.getJobId());
        var requestId = ProtoUtils.fromProto(protoRequest.getRequestId());
        // Reconstruct Job from the proto request to get StreamSpecs and keySchema for deserialization.
        var jobMapper = new JobProtoMapper(streamContext);
        var job = jobMapper.fromProto(jobId, protoRequest.getComputationId(), protoRequest.getJobInfo());
        var responseMapper = new ResponseProtoMapper();
        var responseContext = responseMapper.fromProto(
                processBatchResult.getData(),
                job.getStreamSpecs(),
                job.getGroupBySchema(),
                jobId,
                requestId
        );
        // Read the request states back off the wire with the mappers the worker's side uses, so
        // the response views show exactly what the computation was handed: a seed the request
        // cannot express is invisible to the assertions too.
        var keyCodec = CodecRegistry.getInstance().getKeyCodec();
        var keySchema = job.getGroupBySchema();
        var requestExternalStates = new ExternalStateProtoMapper(keySchema, keyCodec)
                .fromProto(protoRequest.getExternalStatesList(), jobId, requestId);
        var requestInternalStates = new InternalStateProtoMapper(keySchema, keyCodec)
                .fromProto(protoRequest.getInternalStatesList(), jobId, requestId);
        return new TestDoProcessResponse(responseContext, requestExternalStates, requestInternalStates);
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
     * Releases the companion resources this harness loaded, running every {@code unload} hook.
     *
     * <p>Idempotent, so a test may close the harness explicitly and still use try-with-resources.
     * A resource owning threads, pools or connections is only released here — nothing else in a
     * unit test unloads it.
     */
    @Override
    public void close() {
        requestProcessor.shutdown();
    }

    /**
     * Builder for constructing {@link TestComputationHarness} instances.
     *
     * <p>At minimum, {@link #setPipelineContext(PipelineContext)} and one of the
     * {@code setPipelineSpec} overloads must be called before {@link #build()}.
     */
    public static class Builder {
        /**
         * Distinguishes the synthetic resource class names of harnesses sharing one context.
         */
        private static final AtomicLong HARNESS_SEQUENCE = new AtomicLong();

        private @Nullable PipelineContext pipelineContext;
        private @Nullable JobContext jobContext;
        private @Nullable String pipelineSpecStr;
        private @Nullable YTreeNode pipelineSpecYTree;
        private Map<String, TableSchema> externalStateSchemas = new HashMap<>();
        private final Map<String, FlowResource> resources = new LinkedHashMap<>();
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
         * Adds a single external state schema entry. Required for every external state the
         * request seeds with a value: the schema describes those values on the wire, as the
         * worker's {@code TState} does.
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
         * Replaces all companion-hosted resources with the provided map.
         *
         * @param resources resources keyed by the alias the computation looks them up under
         * @return this builder
         * @see #addResource(String, FlowResource)
         */
        public Builder setResources(Map<String, FlowResource> resources) {
            this.resources.clear();
            this.resources.putAll(resources);
            return this;
        }

        /**
         * Adds a companion-hosted resource under the alias the computation looks it up under via
         * {@link tech.ytsaurus.flow.context.RuntimeContext#getResource(String)}.
         *
         * <p>The instance is loaded once by {@link #build()}, with empty parameters and no
         * dependencies, stays in place for every {@code doProcess} call, and is unloaded by
         * {@link TestComputationHarness#close()}; the harness drives no reconfigure or re-init.
         *
         * @param alias    the alias the computation resolves the resource by
         * @param resource the resource instance to serve
         * @return this builder
         */
        public Builder addResource(String alias, FlowResource resource) {
            resources.put(alias, resource);
            return this;
        }

        /**
         * Builds and returns a new {@link TestComputationHarness}.
         *
         * <p>Extracts stream information and group_by_schema schemas from the pipeline spec,
         * registers streams in the pipeline context, creates the underlying
         * {@link CompanionRequestProcessor}, and loads resources into a private store. Resource
         * factories in the caller's pipeline context are never modified.
         *
         * @return a fully configured harness instance.
         * @throws IllegalStateException if the pipeline context or pipeline spec is not set, or a
         *                               declared resource fails to load — the ones loaded before it
         *                               are unloaded first.
         */
        public TestComputationHarness build() {
            if (pipelineContext == null) {
                throw new IllegalStateException("PipelineContext must be set");
            }
            if (pipelineSpecYTree == null && pipelineSpecStr == null) {
                throw new IllegalStateException("PipelineSpec must be set");
            }
            if (jobContext == null) {
                jobContext = new JobContext();
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
            var snapshot = new PipelineContextSnapshot(pipelineContext);
            long harnessId = HARNESS_SEQUENCE.incrementAndGet();
            Map<String, Supplier<? extends FlowResource>> factories = new HashMap<>(snapshot.getResourceFactories());
            resources.forEach((alias, resource) -> {
                String className = resourceClassName(harnessId, alias);
                if (factories.putIfAbsent(className, () -> resource) != null) {
                    throw new IllegalArgumentException("Duplicate harness resource class: " + className);
                }
            });
            // Test-only factories belong to this store, never to the caller's PipelineContext.
            var store = new ResourceStore(factories);
            var requestProcessor = new CompanionRequestProcessor(snapshot, jobContext, store);
            var groupBySchemas = PipelineSpecTestUtils.extractGroupBySchemas(pipelineSpec);

            List<TCompanionResourceInstanceReference> references;
            try {
                references = loadResources(store, harnessId, resources.keySet());
            } catch (RuntimeException | Error error) {
                var failures = new FailureCollector();
                failures.add(error);
                failures.tryRun(store::shutdown);
                failures.throwUncheckedIfAny();
                throw error;
            }

            return new TestComputationHarness(
                    requestProcessor,
                    pipelineSpec,
                    snapshot,
                    externalStateSchemas,
                    groupBySchemas,
                    references
            );
        }

        /**
         * Drives one init command per declared resource through the real resource store and returns
         * the instance references every request must carry to reach them.
         */
        private static List<TCompanionResourceInstanceReference> loadResources(
                ResourceStore store,
                long harnessId,
                Set<String> aliases
        ) {
            var references = new ArrayList<TCompanionResourceInstanceReference>(aliases.size());
            for (String alias : aliases) {
                var outcome = store.execute(
                        alias, EResourceCommand.RC_INIT_VALUE, initArgument(harnessId, alias));
                if (outcome.status() != EResourceExecuteStatus.RES_OK) {
                    throw new IllegalStateException("Failed to load resource '%s': %s (%s)".formatted(
                            alias, outcome.errorMessage(), outcome.status()));
                }
                references.add(TCompanionResourceInstanceReference.newBuilder()
                        .setResourceId(alias)
                        .setIncarnationId(ProtoUtils.toProto(RESOURCE_INCARNATION_ID))
                        .setConfigurationGeneration(0)
                        .setAlias(alias)
                        .build());
            }
            return references;
        }

        /**
         * Builds the init command argument loading the harness resource registered for the alias.
         */
        private static ByteString initArgument(long harnessId, String alias) {
            return YsonUtils.protoFromYTree(YTree.builder().beginMap()
                    .key("spec").beginMap()
                    .key("parameters").beginMap()
                    .key(ResourceStore.COMPANION_RESOURCE_CLASS_KEY).value(resourceClassName(harnessId, alias))
                    .endMap()
                    .endMap()
                    .key("dynamic_spec").beginMap()
                    .key("parameters").beginMap().endMap()
                    .endMap()
                    .key("incarnation_id").value(RESOURCE_INCARNATION_ID.toString())
                    .key("incarnation_generation").value(1)
                    .key("configuration_generation").value(0)
                    .key("dependencies").beginList().endList()
                    .endMap().build());
        }

        private static String resourceClassName(long harnessId, String alias) {
            return "TestHarnessResource:" + harnessId + ":" + alias;
        }
    }

}
