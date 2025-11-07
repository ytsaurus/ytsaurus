package tech.ytsaurus.client;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqGetInSyncReplicas;

@NonNullApi
@NonNullFields
class GetInSyncReplicasWrapper implements HighLevelRequest<TReqGetInSyncReplicas.Builder> {
    private final YtTimestamp timestamp;
    private final GetInSyncReplicas request;

    GetInSyncReplicasWrapper(YtTimestamp timestamp, GetInSyncReplicas request) {
        this.timestamp = timestamp;
        this.request = request;
    }

    @Override
    public String getArgumentsLogString() {
        return "Path: " + request.getPath() +
                "; Timestamp: " + timestamp + "; ";
    }

    @Override
    public void writeHeaderTo(TRequestHeader.Builder header) {
        request.writeHeaderTo(header);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, ?> builder) {
        builder.body().setPath(ByteString.copyFromUtf8(request.getPath()));
        builder.body().setTimestamp(timestamp.getValue());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));

        request.serializeRowsetTo(builder.attachments());
    }
}
