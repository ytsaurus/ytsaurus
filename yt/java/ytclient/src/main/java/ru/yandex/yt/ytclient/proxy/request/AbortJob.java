package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAbortJob;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullFields
@NonNullApi
public class AbortJob extends RequestBase<AbortJob> implements HighLevelRequest<TReqAbortJob.Builder> {
    private final GUID jobId;
    private final @Nullable Long interruptTimeout;

    public AbortJob(GUID jobId) {
        this(jobId, null);
    }

    public AbortJob(GUID jobId, @Nullable Long interruptTimeout) {
        this.jobId = jobId;
        this.interruptTimeout = interruptTimeout;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortJob.Builder, ?> requestBuilder) {
        TReqAbortJob.Builder messageBuilder = requestBuilder.body();
        messageBuilder.setJobId(RpcUtil.toProto(jobId));
        if (interruptTimeout != null) {
            messageBuilder.setInterruptTimeout(interruptTimeout);
        }
    }

    @Override
    protected AbortJob self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
        sb.append("interruptTimeout: ").append(interruptTimeout).append(";");
    }
}
