package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TReqGetFileFromCache;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class GetFileFromCache
        extends TransactionalRequest<GetFileFromCache>
        implements HighLevelRequest<TReqGetFileFromCache.Builder> {
    private final YPath cachePath;
    private final String md5;
    @Nullable
    private MasterReadOptions masterReadOptions;

    public GetFileFromCache(YPath cachePath, String md5) {
        this.cachePath = cachePath;
        this.md5 = md5;
    }

    public YPath getCachePath() {
        return cachePath;
    }

    public String getMd5() {
        return md5;
    }

    public GetFileFromCache setMasterReadOptions(MasterReadOptions masterReadOptions) {
        this.masterReadOptions = masterReadOptions;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetFileFromCache.Builder, ?> builder) {
        builder.body()
                .setCachePath(cachePath.toString())
                .setMd5(md5);
        if (masterReadOptions != null) {
            builder.body().setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("CachePath: ").append(cachePath).append("; ");
        sb.append("Md5: ").append(md5).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Nonnull
    @Override
    protected GetFileFromCache self() {
        return this;
    }
}
