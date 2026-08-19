package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.rpc.TStream;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Mapper that converts a list of protobuf {@link TStream} definitions into a {@link StreamSpecs}
 * by resolving each definition against the registered streams of a pipeline.
 */
public class StreamSpecsProtoMapper {

    private final FlowStreamsContext streamsContext;

    /**
     * Creates a mapper with the supplied stream context.
     *
     * @param streamsContext registered streams of the pipeline used to resolve stream IDs
     */
    public StreamSpecsProtoMapper(FlowStreamsContext streamsContext) {
        this.streamsContext = Objects.requireNonNull(streamsContext, "streamsContext must not be null");
    }

    /**
     * Converts a list of protobuf stream definitions to {@link StreamSpecs}.
     *
     * @param protoStreams the protobuf stream definitions
     * @return the stream specs
     */
    public StreamSpecs fromProto(List<TStream> protoStreams) {
        var mappingBuilder = StreamIdsMapping.builder();
        var flowStreams = new ArrayList<FlowStream<?>>(protoStreams.size());
        for (var protoStream : protoStreams) {
            YTreeNode streamSchema = YsonUtils.yTreeFromProto(protoStream.getSchema());
            mappingBuilder.addMapping(protoStream.getStreamId(), protoStream.getStreamSpecId());
            var stream = streamsContext.getStream(protoStream.getStreamId());
            if (stream == null) {
                stream = FlowStreams.raw(
                        protoStream.getStreamId(),
                        TableSchema.fromYTree(streamSchema)
                );
            }
            flowStreams.add(stream);
        }
        return new StreamSpecs(mappingBuilder.build(), flowStreams);
    }
}
