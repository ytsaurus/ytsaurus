package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqPutFileToCache;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class PutFileToCache
        extends MutateNode<PutFileToCache>
        implements HighLevelRequest<TReqPutFileToCache.Builder> {
    private final YPath filePath;
    private final YPath cachePath;
    private final String md5;
    @Nullable
    private Boolean preserveExpirationTimeout;
    @Nullable
    private MasterReadOptions masterReadOptions;

    public PutFileToCache(YPath filePath, YPath cachePath, String md5) {
        this.filePath = filePath;
        this.cachePath = cachePath;
        this.md5 = md5;
    }

    public YPath getFilePath() {
        return filePath;
    }

    public YPath getCachePath() {
        return cachePath;
    }

    public String getMd5() {
        return md5;
    }

    public PutFileToCache setMasterReadOptions(MasterReadOptions masterReadOptions) {
        this.masterReadOptions = masterReadOptions;
        return this;
    }

    public PutFileToCache setPreserveExpirationTimeout(Boolean preserveExpirationTimeout) {
        this.preserveExpirationTimeout = preserveExpirationTimeout;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPutFileToCache.Builder, ?> builder) {
        builder.body()
                .setPath(filePath.toString())
                .setCachePath(cachePath.toString())
                .setMd5(md5);
        if (masterReadOptions != null) {
            builder.body().setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.body().setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        if (preserveExpirationTimeout != null) {
            builder.body().setPreserveExpirationTimeout(preserveExpirationTimeout);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(filePath).append("; ");
        sb.append("CachePath: ").append(cachePath).append("; ");
        sb.append("Md5: ").append(md5).append("; ");
        if (preserveExpirationTimeout != null) {
            sb.append("PreserveExpirationTimeout: ").append(preserveExpirationTimeout).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Nonnull
    @Override
    protected PutFileToCache self() {
        return this;
    }
}
