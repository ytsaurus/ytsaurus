package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.rpc.TStream;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;

/**
 * Builder for creating protobuf requests with synthetic (random) test data.
 * <p>
 * Used by benchmarks and unit tests to generate {@link TReqProcessBatch},
 * and {@link TReqPutJob} requests with configurable
 * schemas, message counts, and state configurations.
 * <p>
 * For converting real domain objects to proto requests, see {@link ProtobufRequestConverter}.
 */
public class ProtobufRequestBuilder {
    public static final TGuid PROTO_REQUEST_ID = TGuid.newBuilder().setFirst(100).setSecond(200).build();
    public static final GUID REQUEST_ID = ProtoUtils.fromProto(PROTO_REQUEST_ID);

    public static final TGuid PROTO_JOB_ID = TGuid.newBuilder().setFirst(101).setSecond(202).build();
    public static final GUID JOB_ID = ProtoUtils.fromProto(PROTO_JOB_ID);

    public static final String COMPUTATION_ID = "computation-id";

    private final List<InternalStateInfo> internalStateInfos = new ArrayList<>();
    private final List<ExternalStateInfo> externalStateInfos = new ArrayList<>();
    private SchemaGenerator schemaGenerator = SchemaGenerator.builder().build();
    private @Nullable SpecGenerator specGenerator = null;
    private int messageCount = 100;
    private int keyStringsSize = 20;
    private int valueStringsSize = 30;
    private String computationId = COMPUTATION_ID;
    private boolean addJobInfo = true;
    private TGuid jobId = PROTO_JOB_ID;
    private CodecRegistry codecRegistry = CodecRegistry.getInstance();

    private TableSchema keySchema = schemaGenerator.createStringSchema(1);
    private TableSchema payloadSchema = schemaGenerator.createMixedSchema(20);

    // --- Request creation methods ---

    public TReqProcessBatch createProcessBatch() {
        var builder = TReqProcessBatch.newBuilder()
                .setRequestId(PROTO_REQUEST_ID)
                .setJobId(jobId)
                .setComputationId(computationId);

        if (addJobInfo) {
            builder.setJobInfo(createJobInfo());
        }

        var protoMessageList = TestDataUtils.createExtenedeMessageList(
                payloadSchema, keySchema, messageCount, keyStringsSize, valueStringsSize,
                codecRegistry.getKeyCodec(),
                codecRegistry.getPayloadCodec().codecFor(payloadSchema)
        );
        builder.addAllMessages(protoMessageList);
        builder.addAllInternalStates(createInternalStates(protoMessageList));
        builder.addAllExternalStates(createExternalStates(protoMessageList));

        return builder.build();
    }


    public TReqPutJob createPutJob() {
        return TReqPutJob.newBuilder()
                .setRequestId(PROTO_REQUEST_ID)
                .setJobId(jobId)
                .setComputationId(computationId)
                .setJobInfo(createJobInfo())
                .build();
    }

    // --- Internal helpers ---

    private SpecGenerator getEffectiveSpecGenerator() {
        if (specGenerator != null) {
            return specGenerator;
        }
        return SpecGenerator.builder()
                .setInputStreamSchema(payloadSchema)
                .setOutputStreamSchema(payloadSchema)
                .build();
    }

    private TJobInfo createJobInfo() {
        var effectiveSpecGenerator = getEffectiveSpecGenerator();
        var jobInfoBuilder = TJobInfo.newBuilder()
                .setSpec(YsonUtils.protoFromYTree(effectiveSpecGenerator.spec()))
                .setDynamicSpec(YsonUtils.protoFromYTree(effectiveSpecGenerator.dynamicSpec()));

        int streamSpecId = 0;
        var streams = new ArrayList<TStream>();
        for (var streamSpec : effectiveSpecGenerator.getStreams()) {
            streams.add(TStream.newBuilder()
                    .setStreamId(streamSpec.getStreamId())
                    .setStreamSpecId(streamSpecId++)
                    .setSchema(YsonUtils.protoFromYTree(streamSpec.getSchema().toYTree()))
                    .build());
        }
        jobInfoBuilder.addAllStreams(streams);
        return jobInfoBuilder.build();
    }

    private List<TState> createInternalStates(List<TReqProcessBatch.TExtendedMessage> protoMessageList) {
        var internalStates = new ArrayList<TState>();
        var valueCodec = codecRegistry.getInternalStateValueCodec();
        for (var stateInfo : internalStateInfos) {
            TState.Builder stateBuilder = TState.newBuilder().setName(stateInfo.name());
            var items = new ArrayList<TStateItem>(messageCount);
            for (int i = 0; i < messageCount; ++i) {
                byte[] randomBytes = RandomStrings
                        .nextAlphanumeric(stateInfo.payloadSize())
                        .getBytes();
                items.add(TStateItem.newBuilder()
                        .setKey(protoMessageList.get(i).getKey())
                        .setReset(false)
                        .setState(valueCodec.encode(randomBytes))
                        .build());
            }
            stateBuilder.addAllStateItems(items);
            internalStates.add(stateBuilder.build());
        }
        return internalStates;
    }

    private List<TState> createExternalStates(List<TReqProcessBatch.TExtendedMessage> protoMessageList) {
        var externalStates = new ArrayList<TState>();
        var payloadCodec = codecRegistry.getPayloadCodec();
        for (var stateInfo : externalStateInfos) {
            TState.Builder stateBuilder = TState.newBuilder()
                    .setName(stateInfo.name())
                    .setSchema(YsonUtils.protoFromYTree(stateInfo.payloadSchema().toYTree()));

            var boundPayloadCodec = payloadCodec.codecFor(stateInfo.payloadSchema());
            var items = new ArrayList<TStateItem>(messageCount);
            for (int i = 0; i < messageCount; ++i) {
                var row = TestDataUtils.createUnversionedRow(
                        stateInfo.payloadSchema(), stateInfo.payloadStringSize()
                );
                items.add(TStateItem.newBuilder()
                        .setKey(protoMessageList.get(i).getKey())
                        .setReset(false)
                        .setState(boundPayloadCodec.encode(new Payload(row, stateInfo.payloadSchema())))
                        .build());
            }
            stateBuilder.addAllStateItems(items);
            externalStates.add(stateBuilder.build());
        }
        return externalStates;
    }

    // --- Setters (fluent API) ---

    public ProtobufRequestBuilder setSpecGenerator(SpecGenerator specGenerator) {
        this.specGenerator = specGenerator;
        return this;
    }

    public ProtobufRequestBuilder setMessageCount(int messageCount) {
        this.messageCount = messageCount;
        return this;
    }

    public ProtobufRequestBuilder addInternalState(String name, int payloadSize) {
        internalStateInfos.add(new InternalStateInfo(name, payloadSize));
        return this;
    }

    public ProtobufRequestBuilder addExternalState(String name, int payloadStringSize, TableSchema payloadSchema) {
        externalStateInfos.add(new ExternalStateInfo(name, payloadStringSize, payloadSchema));
        return this;
    }

    public ProtobufRequestBuilder setKeySchema(TableSchema keySchema) {
        this.keySchema = keySchema;
        return this;
    }

    public ProtobufRequestBuilder setPayloadSchema(TableSchema payloadSchema) {
        this.payloadSchema = payloadSchema;
        return this;
    }

    public ProtobufRequestBuilder setKeyStringsSize(int keyStringsSize) {
        this.keyStringsSize = keyStringsSize;
        return this;
    }

    public ProtobufRequestBuilder setValueStringsSize(int valueStringsSize) {
        this.valueStringsSize = valueStringsSize;
        return this;
    }

    public ProtobufRequestBuilder setSchemaGenerator(SchemaGenerator schemaGenerator) {
        this.schemaGenerator = schemaGenerator;
        return this;
    }

    public ProtobufRequestBuilder setComputationId(String computationId) {
        this.computationId = computationId;
        return this;
    }

    public ProtobufRequestBuilder setAddJobInfo(boolean addJobInfo) {
        this.addJobInfo = addJobInfo;
        return this;
    }

    public ProtobufRequestBuilder setJobId(GUID jobId) {
        this.jobId = ProtoUtils.toProto(jobId);
        return this;
    }

    /**
     * Overrides the {@link CodecRegistry} used to encode message keys, message payloads,
     * internal-state values, and external-state values.
     *
     * <p>Test-only override; defaults to {@link CodecRegistry#getInstance()}, the JVM-wide
     * registry resolved via {@code ServiceLoader<CodecRegistryProvider>}. Production code
     * should rely on the singleton and avoid calling this method.
     *
     * @param codecRegistry the registry to use
     * @return this builder
     */
    public ProtobufRequestBuilder setCodecRegistry(CodecRegistry codecRegistry) {
        this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry must not be null");
        return this;
    }

    // --- Auxiliary records ---

    record InternalStateInfo(String name, int payloadSize) {
    }

    record ExternalStateInfo(String name, int payloadStringSize, TableSchema payloadSchema) {
    }
}
