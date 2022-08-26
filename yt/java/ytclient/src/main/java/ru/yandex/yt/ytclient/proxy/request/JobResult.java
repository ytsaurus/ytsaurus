package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TJob;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
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
