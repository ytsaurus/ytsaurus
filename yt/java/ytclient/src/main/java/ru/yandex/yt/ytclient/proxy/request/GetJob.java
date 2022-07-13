package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGetJob;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;


@NonNullFields
@NonNullApi
public class GetJob extends OperationReq<GetJob> implements HighLevelRequest<TReqGetJob.Builder> {
    private final GUID jobId;

    public GetJob(GUID operationId, GUID jobId) {
        super(operationId, null);
        this.jobId = jobId;
    }

    GetJob(String alias, GUID jobId) {
        super(null, alias);
        this.jobId = jobId;
    }

    public static GetJob fromAlias(String alias, GUID jobId) {
        return new GetJob(alias, jobId);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetJob.Builder, ?> requestBuilder) {
        TReqGetJob.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        messageBuilder.setJobId(RpcUtil.toProto(jobId));
    }

    @Override
    protected GetJob self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
    }
}
