package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.EJobState;
import ru.yandex.yt.rpcproxy.TReqListJobs;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public class ListJobs
        extends RequestBase<ListJobs>
        implements HighLevelRequest<TReqListJobs.Builder> {
    private final GUID operationId;
    @Nullable
    private final JobState state;
    @Nullable
    private final Long limit;

    public ListJobs(GUID operationId, @Nullable JobState state, @Nullable Long limit) {
        this.operationId = operationId;
        this.state = state;
        this.limit = limit;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqListJobs.Builder, ?> requestBuilder) {
        requestBuilder.body().setOperationId(RpcUtil.toProto(operationId));
        if (state != null) {
            requestBuilder.body().setState(EJobState.forNumber(state.getProtoValue()));
        }
        if (limit != null) {
            requestBuilder.body().setLimit(limit);
        }
    }

    @Nonnull
    @Override
    protected ListJobs self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("OperationId: ").append(operationId).append("; ");
        if (state != null) {
            sb.append("State: ").append(state.getWireName()).append("; ");
        }
        if (limit != null) {
            sb.append("Limit: ").append(limit).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }
}
