package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TNewTimer;
import tech.ytsaurus.flow.rpc.TResponseData;
import tech.ytsaurus.flow.stream.StreamSpecs;

/**
 * Orchestrator that composes per-type mappers to convert between protobuf {@link TResponseData}
 * and a domain {@link ResponseContext}.
 */
public class ResponseProtoMapper {

    private final CodecRegistry codecs;

    /**
     * Creates a response mapper using the JVM-wide {@link CodecRegistry} singleton.
     */
    public ResponseProtoMapper() {
        this.codecs = CodecRegistry.getInstance();
    }

    /**
     * Maps a {@link ResponseContext} to a protobuf {@link TResponseData}.
     *
     * @param response    the response context
     * @param streamSpecs the stream specs for message serialization
     * @return the protobuf response data
     */
    public TResponseData toProto(ResponseContext response, StreamSpecs streamSpecs) {
        TResponseData.Builder builder = TResponseData.newBuilder();

        var messageMapper = new MessageProtoMapper(streamSpecs);
        var newTimerMapper = new NewTimerProtoMapper();

        // Groups.
        var protoGroups = new ArrayList<TResponseData.TGroup>(response.getTransformResults().size());
        for (TransformResult transformResult : response.getTransformResults()) {
            var protoParentIds = new ArrayList<ByteString>(transformResult.getParentIds().size());
            for (var parentId : transformResult.getParentIds()) {
                protoParentIds.add(ByteString.copyFromUtf8(parentId));
            }
            if (protoParentIds.isEmpty()) {
                throw new IllegalArgumentException("Parent ids are empty");
            }
            var protoMessages = new ArrayList<NYT.NFlow.NProto.Message.TMessage>(
                    transformResult.getMessages().size()
            );
            for (var message : transformResult.getMessages()) {
                protoMessages.add(messageMapper.toProto(message));
            }
            var protoTimers = new ArrayList<TNewTimer>(transformResult.getTimers().size());
            for (var timer : transformResult.getTimers()) {
                protoTimers.add(newTimerMapper.toProto(timer));
            }
            protoGroups.add(TResponseData.TGroup.newBuilder()
                    .addAllMessages(protoMessages)
                    .addAllDistribute(transformResult.getDistribute())
                    .addAllParentIds(protoParentIds)
                    .addAllTimers(protoTimers)
                    .build());
        }
        builder.addAllOutput(protoGroups);

        // Internal states.
        var internalStateMapper = new InternalStateProtoMapper(
                /*keySchema not needed for toProto*/ null,
                codecs.getKeyCodec(),
                codecs.getInternalStateValueCodec()
        );
        var protoStates = new ArrayList<tech.ytsaurus.flow.rpc.TState>();
        for (var namedState : response.getInternalStates().values()) {
            // Only states changed via accessors are sent back; skip holders with no modifications.
            if (namedState.hasModifiedStates()) {
                protoStates.add(internalStateMapper.toProto(namedState));
            }
        }
        builder.addAllInternalStates(protoStates);

        // External states.
        var externalStateMapper = new ExternalStateProtoMapper(
                /*keySchema not needed for toProto*/ null,
                codecs.getKeyCodec(),
                codecs.getPayloadCodec()
        );
        var externalProtoStates = new ArrayList<tech.ytsaurus.flow.rpc.TState>();
        for (var namedState : response.getExternalStates().values()) {
            // Only states changed via accessors are sent back; skip holders with no modifications.
            if (namedState.hasModifiedStates()) {
                externalProtoStates.add(externalStateMapper.toProto(namedState));
            }
        }
        builder.addAllExternalStates(externalProtoStates);

        return builder.build();
    }

    /**
     * Maps a protobuf {@link TResponseData} back to a {@link ResponseContext}.
     *
     * @param responseData the protobuf response data
     * @param streamSpecs  the stream specs for message deserialization
     * @param keySchema    the key schema for state deserialization
     * @param jobId        the job identifier
     * @param requestId    the request identifier
     * @return the response context
     */
    public ResponseContext fromProto(
            TResponseData responseData,
            StreamSpecs streamSpecs,
            TableSchema keySchema,
            GUID jobId,
            GUID requestId
    ) {
        var messageMapper = new MessageProtoMapper(streamSpecs);
        var newTimerMapper = new NewTimerProtoMapper();

        // Groups → TransformResults.
        var transformResults = new ArrayList<TransformResult>(responseData.getOutputCount());
        for (var protoGroup : responseData.getOutputList()) {
            var parentIds = new ArrayList<String>(protoGroup.getParentIdsCount());
            for (var protoParentId : protoGroup.getParentIdsList()) {
                parentIds.add(protoParentId.toStringUtf8());
            }
            var transformResult = new TransformResult(parentIds);
            var distribute = protoGroup.getDistributeList();
            int messageIndex = 0;
            for (var protoMessage : protoGroup.getMessagesList()) {
                // Empty distribute means "distribute all".
                boolean messageDistribute = distribute.isEmpty() || distribute.get(messageIndex);
                transformResult.addMessage(messageMapper.fromProto(protoMessage), messageDistribute);
                ++messageIndex;
            }
            for (var protoTimer : protoGroup.getTimersList()) {
                transformResult.addTimer(newTimerMapper.fromProto(protoTimer));
            }
            transformResults.add(transformResult);
        }

        // Internal states.
        var internalStateMapper = new InternalStateProtoMapper(
                keySchema, codecs.getKeyCodec(), codecs.getInternalStateValueCodec()
        );
        var internalStates = internalStateMapper.fromProto(responseData.getInternalStatesList());

        // External states.
        var externalStateMapper = new ExternalStateProtoMapper(
                keySchema, codecs.getKeyCodec(), codecs.getPayloadCodec()
        );
        var externalStates = externalStateMapper.fromProto(
                responseData.getExternalStatesList(), jobId, requestId
        );

        return new ResponseContext(jobId, requestId, transformResults, internalStates, externalStates);
    }
}
