package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.request.mapper.MessageProtoMapper;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.row.codec.PayloadCodec;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.rpc.TStream;
import tech.ytsaurus.flow.rpc.TWatermark;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * Converts domain objects (messages, states, timers) into protobuf request format for tests,
 * driven from real test data rather than the synthetic data produced by
 * {@link ProtobufRequestBuilder}.
 */
public class ProtobufRequestConverter {

    private TGuid jobId = ProtobufRequestBuilder.PROTO_JOB_ID;
    private CodecRegistry codecRegistry = CodecRegistry.getInstance();

    /**
     * Overrides the job id used for subsequent {@code create*} calls.
     *
     * @param jobId the new job identifier
     * @return this converter
     */
    public ProtobufRequestConverter setJobId(GUID jobId) {
        this.jobId = ProtoUtils.toProto(jobId);
        return this;
    }

    /**
     * Overrides the {@link CodecRegistry} used to (de)serialize message keys, timer keys,
     * internal-state keys/values, and external-state keys/values.
     * <p>
     * Test-only override; defaults to {@link CodecRegistry#getInstance()}, which is the
     * JVM-wide registry resolved via {@code ServiceLoader<CodecRegistryProvider>}. Production
     * code should rely on the singleton.
     *
     * @param codecRegistry the registry to use
     * @return this converter
     */
    public ProtobufRequestConverter setCodecRegistry(CodecRegistry codecRegistry) {
        this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry must not be null");
        return this;
    }

    /**
     * Builds a protobuf {@link TReqProcessBatch} request from the given test inputs.
     *
     * @param computationId        target computation id
     * @param messages             domain messages to process
     * @param timers               domain timers to process
     * @param pipelineSpec         YSON pipeline specification
     * @param streamsContext       registered streams of the pipeline
     * @param internalStates       internal-state contents keyed by state name and entry key
     * @param externalStates       external-state contents keyed by state name and entry key
     * @param joinedExternalStates read-only joined external-state contents keyed by state name and key
     * @param externalStateSchemas optional per-state schemas used when no values are supplied
     * @param watermarks           per-stream watermarks
     * @return the protobuf process-batch request
     */
    public TReqProcessBatch createProcessBatch(
            String computationId,
            List<ExtendedMessage> messages,
            List<Timer> timers,
            YTreeNode pipelineSpec,
            FlowStreamsContext streamsContext,
            Map<String, Map<Payload, InternalState>> internalStates,
            Map<String, Map<Payload, ExternalState>> externalStates,
            Map<String, Map<Payload, ExternalState>> joinedExternalStates,
            Map<String, TableSchema> externalStateSchemas,
            Map<String, Long> watermarks
    ) {
        var builder = TReqProcessBatch.newBuilder()
                .setRequestId(ProtoUtils.toProto(GUID.create()))
                .setJobId(jobId)
                .setComputationId(computationId);

        var pipelineContext = extractPipelineContext(computationId, pipelineSpec, streamsContext);
        builder.setJobInfo(pipelineContext.jobInfo());

        var protoMessages = convertExtendedMessagesToProto(
                messages,
                pipelineContext.streamSpecs(),
                codecRegistry.getKeyCodec()
        );
        builder.addAllMessages(protoMessages);
        builder.addAllInternalStates(convertInternalStatesToProto(
                internalStates,
                codecRegistry.getKeyCodec(),
                codecRegistry.getInternalStateValueCodec()
        ));
        builder.addAllExternalStates(convertExternalStatesToProto(
                externalStates,
                externalStateSchemas,
                codecRegistry.getKeyCodec(),
                codecRegistry.getPayloadCodec()
        ));
        builder.addAllJoinedExternalStates(convertExternalStatesToProto(
                joinedExternalStates,
                externalStateSchemas,
                codecRegistry.getKeyCodec(),
                codecRegistry.getPayloadCodec()
        ));
        builder.addAllTimers(convertTimersToProto(timers, codecRegistry.getKeyCodec()));

        for (var entry : watermarks.entrySet()) {
            builder.addWatermarks(
                    TWatermark.newBuilder()
                            .setStreamId(entry.getKey())
                            .setWatermark(entry.getValue())
                            .build()
            );
        }

        return builder.build();
    }

    // --- Pipeline context extraction ---

    private ComputationPipelineContext extractPipelineContext(
            String computationId,
            YTreeNode pipelineSpec,
            FlowStreamsContext streamsContext
    ) {
        YTreeNode computationStaticSpec = pipelineSpec.asMap()
                .get("spec").asMap()
                .get("computations").asMap()
                .get(computationId);
        YTreeNode computationDynamicSpec = resolveComputationDynamicSpec(pipelineSpec, computationId);

        Map<String, YTreeNode> streamSpecs = pipelineSpec.asMap()
                .get("spec").asMap()
                .get("streams").asMap();

        var streamInfos = PipelineSpecTestUtils.buildStreamInfos(streamSpecs);

        var jobInfo = buildJobInfo(computationStaticSpec, computationDynamicSpec, streamInfos);
        var localStreamSpecs = buildStreamSpecs(streamInfos, streamsContext, codecRegistry);

        return new ComputationPipelineContext(jobInfo, localStreamSpecs);
    }

    private record ComputationPipelineContext(
            TJobInfo jobInfo,
            StreamSpecs streamSpecs
    ) {
    }

    /**
     * Resolves the dynamic spec for the given computation, tolerating an absent
     * {@code dynamic_spec}, {@code dynamic_spec/computations}, or per-computation entry.
     * When any of these is missing, an empty computation block with an empty
     * {@code parameters} map is synthesized on the fly so the request can still be built.
     */
    private static YTreeNode resolveComputationDynamicSpec(YTreeNode pipelineSpec, String computationId) {
        YTreeNode dynamicSpecNode = pipelineSpec.asMap().get("dynamic_spec");
        if (dynamicSpecNode != null) {
            YTreeNode computationsNode = dynamicSpecNode.asMap().get("computations");
            if (computationsNode != null) {
                YTreeNode computationDynamicSpec = computationsNode.asMap().get(computationId);
                if (computationDynamicSpec != null) {
                    return computationDynamicSpec;
                }
            }
        }
        return emptyComputationDynamicSpec();
    }

    private static YTreeNode emptyComputationDynamicSpec() {
        return YTree.mapBuilder()
                .key("parameters")
                .beginMap()
                .endMap()
                .endMap()
                .build();
    }

    // --- JobInfo / Stream building ---

    static TJobInfo buildJobInfo(
            YTreeNode computationStaticSpec,
            YTreeNode computationDynamicSpec,
            List<StreamInfo> streamInfos
    ) {
        var jobInfoBuilder = TJobInfo.newBuilder()
                .setSpec(YsonUtils.protoFromYTree(computationStaticSpec))
                .setDynamicSpec(YsonUtils.protoFromYTree(computationDynamicSpec));
        jobInfoBuilder.addAllStreams(buildTStreams(streamInfos));
        return jobInfoBuilder.build();
    }

    static List<TStream> buildTStreams(List<StreamInfo> streamInfos) {
        var streams = new ArrayList<TStream>(streamInfos.size());
        for (var streamInfo : streamInfos) {
            streams.add(TStream.newBuilder()
                    .setStreamId(streamInfo.getStreamId())
                    .setStreamSpecId(streamInfo.getStreamSpecId())
                    .setSchema(YsonUtils.protoFromYTree(streamInfo.getSchema().toYTree()))
                    .build());
        }
        return streams;
    }

    private static StreamSpecs buildStreamSpecs(
            List<StreamInfo> streamInfos,
            FlowStreamsContext streamsContext,
            CodecRegistry codecRegistry
    ) {
        var mappingBuilder = StreamIdsMapping.builder();
        var flowStreams = new ArrayList<FlowStream<?>>(streamInfos.size());
        for (var streamInfo : streamInfos) {
            mappingBuilder.addMapping(streamInfo.getStreamId(), streamInfo.getStreamSpecId());
            var stream = streamsContext.getStream(streamInfo.getStreamId());
            if (stream == null) {
                // Fallback stream uses the JVM-wide CodecRegistry via FlowStreams.raw().
                stream = FlowStreams.raw(streamInfo.getStreamId(), streamInfo.getSchema());
            }
            flowStreams.add(stream);
        }
        return new StreamSpecs(mappingBuilder.build(), flowStreams);
    }

    // --- Message conversion ---

    private static List<TReqProcessBatch.TExtendedMessage> convertExtendedMessagesToProto(
            List<ExtendedMessage> messages,
            StreamSpecs streamSpecs,
            KeyCodec keyCodec
    ) {
        var messageMapper = new MessageProtoMapper(streamSpecs);
        var protoMessages = new ArrayList<TReqProcessBatch.TExtendedMessage>(messages.size());
        for (var message : messages) {
            var protoMessage = messageMapper.toProto(message);
            protoMessages.add(TReqProcessBatch.TExtendedMessage.newBuilder()
                    .setKey(keyCodec.encode(message.getKey().getRow()))
                    .setMessage(protoMessage)
                    .build());
        }
        return protoMessages;
    }

    // --- Timer conversion ---

    private static List<NYT.NFlow.NProto.Timer.TTimer> convertTimersToProto(
            List<Timer> timers,
            KeyCodec keyCodec
    ) {
        var protoTimers = new ArrayList<NYT.NFlow.NProto.Timer.TTimer>(timers.size());
        for (int i = 0; i < timers.size(); ++i) {
            var timer = timers.get(i);
            protoTimers.add(NYT.NFlow.NProto.Timer.TTimer.newBuilder()
                    .setKey(keyCodec.encode(timer.getKey().getRow()))
                    .setMessageId("<id>" + i)
                    .setStreamId(ByteString.copyFromUtf8(timer.getStreamId()))
                    .setSystemTimestamp(timer.getSystemTimestamp())
                    .setEventTimestamp(timer.getEventTimestamp())
                    .setTriggerTimestamp(timer.getTriggerTimestamp())
                    .build());
        }
        return protoTimers;
    }

    // --- State conversion ---

    /**
     * Converts internal states to proto format.
     * Iterates over each key in the state map and serializes the key/value pairs using the
     * supplied codecs so that custom {@link KeyCodec} / {@link InternalStateValueCodec}
     * registrations are honoured on the wire.
     */
    private static List<TState> convertInternalStatesToProto(
            Map<String, Map<Payload, InternalState>> internalStates,
            KeyCodec keyCodec,
            InternalStateValueCodec valueCodec
    ) {
        var result = new ArrayList<TState>(internalStates.size());
        for (var entry : internalStates.entrySet()) {
            var stateName = entry.getKey();
            var stateMap = entry.getValue();
            TState.Builder stateBuilder = TState.newBuilder().setName(stateName);

            var stateItems = new ArrayList<TStateItem>(stateMap.size());
            for (var stateEntry : stateMap.entrySet()) {
                TStateItem.Builder itemBuilder = TStateItem.newBuilder()
                        .setKey(keyCodec.encode(stateEntry.getKey().getRow()))
                        .setReset(stateEntry.getValue().isReset());
                if (!stateEntry.getValue().isReset() && stateEntry.getValue().getValue() != null) {
                    itemBuilder.setState(valueCodec.encode((byte[]) stateEntry.getValue().getValue()));
                }
                stateItems.add(itemBuilder.build());
            }
            stateBuilder.addAllStateItems(stateItems);
            result.add(stateBuilder.build());
        }
        return result;
    }

    /**
     * Converts external states to proto format.
     * Derives schema from the first non-reset state value, falling back to user-provided schemas.
     * Uses the supplied codecs so that custom {@link KeyCodec} / {@link PayloadCodec}
     * registrations are honoured on the wire.
     */
    private static List<TState> convertExternalStatesToProto(
            Map<String, Map<Payload, ExternalState>> externalStates,
            Map<String, TableSchema> externalStateSchemas,
            KeyCodec keyCodec,
            PayloadCodec payloadCodec
    ) {
        var result = new ArrayList<TState>();

        for (var entry : externalStates.entrySet()) {
            var stateName = entry.getKey();
            var stateValuesMap = entry.getValue();
            TState.Builder stateBuilder = TState.newBuilder().setName(stateName);

            // Resolve the schema: derive from the first non-reset state with a non-null
            // payload, otherwise fall back to the user-provided schemas.
            TableSchema valueSchema = firstNonResetSchema(stateValuesMap);
            if (valueSchema == null) {
                valueSchema = externalStateSchemas.get(stateName);
            }
            if (valueSchema != null) {
                stateBuilder.setSchema(YsonUtils.protoFromYTree(valueSchema.toYTree()));
            }

            // Bind the payload codec to the per-state schema once: the schema is the same for
            // all items inside a single TState message. Items without a stored value (resets or
            // null payloads) do not need a bound codec.
            ByteStringCodec<Payload> boundValueCodec =
                    valueSchema != null ? payloadCodec.codecFor(valueSchema) : null;

            var stateItems = new ArrayList<TStateItem>(stateValuesMap.size());
            for (var stateEntry : stateValuesMap.entrySet()) {
                TStateItem.Builder itemBuilder = TStateItem.newBuilder()
                        .setKey(keyCodec.encode(stateEntry.getKey().getRow()))
                        .setReset(stateEntry.getValue().isReset());
                if (!stateEntry.getValue().isReset() && stateEntry.getValue().getValue() != null) {
                    Objects.requireNonNull(
                            boundValueCodec,
                            "External state value codec was not bound (missing schema)"
                    );
                    itemBuilder.setState(boundValueCodec.encode(stateEntry.getValue().getValue()));
                }
                stateItems.add(itemBuilder.build());
            }

            stateBuilder.addAllStateItems(stateItems);
            result.add(stateBuilder.build());
        }

        // Add schema-only states (states with schema but no data).
        for (var entry : externalStateSchemas.entrySet()) {
            if (!externalStates.containsKey(entry.getKey())) {
                result.add(TState.newBuilder()
                        .setName(entry.getKey())
                        .setSchema(YsonUtils.protoFromYTree(entry.getValue().toYTree()))
                        .build());
            }
        }

        return result;
    }

    private static @Nullable TableSchema firstNonResetSchema(Map<Payload, ExternalState> stateValuesMap) {
        for (var stateValue : stateValuesMap.values()) {
            if (!stateValue.isReset() && stateValue.getValue() != null) {
                return stateValue.getValue().getSchema();
            }
        }
        return null;
    }
}
