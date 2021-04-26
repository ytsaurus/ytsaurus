package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGenerateTimestamps;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class GenerateTimestamps
        extends RequestBase<GenerateTimestamps>
        implements HighLevelRequest<TReqGenerateTimestamps.Builder> {
    private final int count;

    public GenerateTimestamps(int count) {
        this.count = count;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGenerateTimestamps.Builder, ?> builder) {
        builder.body().setCount(count);
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Count: ").append(count).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    protected GenerateTimestamps self() {
        return this;
    }
}
