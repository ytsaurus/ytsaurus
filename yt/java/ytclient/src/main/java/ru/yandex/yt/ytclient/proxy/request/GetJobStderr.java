package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGetJobStderr;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public class GetJobStderr
        extends RequestBase<GetJobStderr>
        implements HighLevelRequest<TReqGetJobStderr.Builder> {
    private final GUID operationId;
    private final GUID jobId;

    public GetJobStderr(GUID operationId, GUID jobId) {
        this.operationId = operationId;
        this.jobId = jobId;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetJobStderr.Builder, ?> requestBuilder) {
        requestBuilder.body()
                .setOperationId(RpcUtil.toProto(operationId))
                .setJobId(RpcUtil.toProto(jobId));
    }

    @Nonnull
    @Override
    protected GetJobStderr self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("OperationId: ").append(operationId).append("; ");
        sb.append("JobId: ").append(jobId).append("; ");
        super.writeArgumentsLogString(sb);
    }
}
