package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TReqAbortOperation;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class AbortOperation
        extends RequestBase<AbortOperation>
        implements HighLevelRequest<TReqAbortOperation.Builder> {
    private final GUID id;
    @Nullable
    private final String message;

    public AbortOperation(GUID id, @Nullable String message) {
        this.id = id;
        this.message = message;
    }

    public AbortOperation(GUID id) {
        this(id, null);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortOperation.Builder, ?> requestBuilder) {
        requestBuilder.body().setOperationId(RpcUtil.toProto(id));
        if (message != null) {
            requestBuilder.body().setAbortMessage(message);
        }
    }

    @Nonnull
    @Override
    protected AbortOperation self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Id: ").append(id).append("; ");
        if (message != null) {
            sb.append("Message: ").append(message).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

}
