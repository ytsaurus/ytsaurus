package tech.ytsaurus.flow.internal.request.mapper;

import java.util.Objects;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Mapper for creating {@link Job} instances from protobuf requests.
 * <p>
 * Requires {@link FlowStreamsContext} for stream resolution.
 * </p>
 */
public class JobProtoMapper {

    private final FlowStreamsContext streamsContext;

    public JobProtoMapper(FlowStreamsContext streamsContext) {
        this.streamsContext = Objects.requireNonNull(streamsContext, "streamsContext must not be null");
    }

    /**
     * Creates a {@link Job} from a PutJob protobuf request.
     *
     * @param request the protobuf PutJob request
     * @return the job
     */
    public Job fromProto(TReqPutJob request) {
        GUID jobId = ProtoUtils.fromProto(request.getJobId());
        String computationId = request.getComputationId();
        return fromProto(jobId, computationId, request.getJobInfo());
    }

    /**
     * Creates a {@link Job} from protobuf job info.
     *
     * @param jobId         the job identifier
     * @param computationId the computation identifier
     * @param jobInfo       the protobuf job info
     * @return the job
     */
    public Job fromProto(GUID jobId, String computationId, TJobInfo jobInfo) {
        var streamSpecsMapper = new StreamSpecsProtoMapper(streamsContext);
        StreamSpecs streamSpecs = streamSpecsMapper.fromProto(jobInfo.getStreamsList());

        YTreeNode computationSpec = YsonUtils.yTreeFromProto(jobInfo.getSpec());
        YTreeNode computationDynamicSpec = YsonUtils.yTreeFromProto(jobInfo.getDynamicSpec());

        TableSchema groupBySchema = Payload.EMPTY_SCHEMA;
        YTreeNode groupByKey = computationSpec.asMap().get("group_by_schema");
        if (groupByKey != null) {
            groupBySchema = TableSchema.fromYTree(groupByKey);
        }

        return new Job(
                jobId,
                computationId,
                streamSpecs,
                computationSpec,
                computationDynamicSpec,
                groupBySchema
        );
    }
}
