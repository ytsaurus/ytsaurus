package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TJob;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class JobResult {
    private final GUID id;
    @Nullable
    private JobState state;
    @Nullable
    private YTreeNode error;
    @Nullable
    private Long stderrSize;

    public JobResult(TJob job) {
        this.id = RpcUtil.fromProto(job.getId());
        if (job.hasState()) {
            this.state = JobState.fromProto(job.getState());
        }
        if (job.hasError()) {
            this.error = YTreeBinarySerializer.deserialize(job.getError().newInput());
        }
        if (job.hasStderrSize()) {
            this.stderrSize = job.getStderrSize();
        }
    }

    public GUID getId() {
        return id;
    }

    public Optional<JobState> getState() {
        return Optional.ofNullable(state);
    }

    public Optional<YTreeNode> getError() {
        return Optional.ofNullable(error);
    }

    public Optional<Long> getStderrSize() {
        return Optional.ofNullable(stderrSize);
    }
}
