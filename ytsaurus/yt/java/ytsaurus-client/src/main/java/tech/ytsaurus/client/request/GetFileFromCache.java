package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMasterReadOptions;
import tech.ytsaurus.rpcproxy.TReqGetFileFromCache;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

public class GetFileFromCache
        extends TransactionalRequest<GetFileFromCache.Builder, GetFileFromCache>
        implements HighLevelRequest<TReqGetFileFromCache.Builder> {
    private final YPath cachePath;
    private final String md5;
    @Nullable
    private final MasterReadOptions masterReadOptions;

    GetFileFromCache(Builder builder) {
        super(builder);
        this.cachePath = Objects.requireNonNull(builder.cachePath);
        this.md5 = Objects.requireNonNull(builder.md5);
        if (builder.masterReadOptions != null) {
            this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
        } else {
            this.masterReadOptions = null;
        }
    }

    public GetFileFromCache(YPath cachePath, String md5) {
        this(builder().setCachePath(cachePath).setMd5(md5));
    }

    public static Builder builder() {
        return new Builder();
    }

    public YPath getCachePath() {
        return cachePath;
    }

    public String getMd5() {
        return md5;
    }

    /**
     * Internal method: prepare request to send over network.
     */
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

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setCachePath(cachePath)
                .setMd5(md5)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        if (masterReadOptions != null) {
            builder.setMasterReadOptions(masterReadOptions);
        }
        return builder;
    }

    public static class Builder extends TransactionalRequest.Builder<Builder, GetFileFromCache> {
        @Nullable
        private YPath cachePath;
        @Nullable
        private String md5;
        @Nullable
        private MasterReadOptions masterReadOptions;

        public Builder setCachePath(YPath cachePath) {
            this.cachePath = cachePath;
            return self();
        }

        public Builder setMd5(String md5) {
            this.md5 = md5;
            return self();
        }

        public Builder setMasterReadOptions(MasterReadOptions masterReadOptions) {
            this.masterReadOptions = new MasterReadOptions(masterReadOptions);
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public GetFileFromCache build() {
            return new GetFileFromCache(this);
        }
    }
}
