package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMasterReadOptions;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqPutFileToCache;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

public class PutFileToCache
        extends MutateNode<PutFileToCache.Builder, PutFileToCache>
        implements HighLevelRequest<TReqPutFileToCache.Builder> {
    private final YPath filePath;
    private final YPath cachePath;
    private final String md5;
    @Nullable
    private final Boolean preserveExpirationTimeout;
    @Nullable
    private final MasterReadOptions masterReadOptions;

    PutFileToCache(Builder builder) {
        super(builder);
        if (builder.filePath == null) {
            throw new IllegalArgumentException("filePath should be not null");
        }
        if (builder.cachePath == null) {
            throw new IllegalArgumentException("cachePath should be not null");
        }
        if (builder.md5 == null) {
            throw new IllegalArgumentException("md5 should be not null");
        }
        this.filePath = builder.filePath;
        this.cachePath = builder.cachePath;
        this.md5 = builder.md5;
        this.preserveExpirationTimeout = builder.preserveExpirationTimeout;
        if (builder.masterReadOptions != null) {
            this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
        } else {
            this.masterReadOptions = null;
        }
    }

    public PutFileToCache(YPath filePath, YPath cachePath, String md5) {
        this(builder().setFilePath(filePath).setCachePath(cachePath).setMd5(md5));
    }

    public static Builder builder() {
        return new Builder();
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

    /**
     * Internal method: prepare request to send over network.
     */
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

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setFilePath(filePath)
                .setCachePath(cachePath)
                .setMd5(md5)
                .setPreserveExpirationTimeout(preserveExpirationTimeout)
                .setTransactionalOptions(transactionalOptions != null
                        ? new TransactionalOptions(transactionalOptions)
                        : null)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);

        if (masterReadOptions != null) {
            builder.setMasterReadOptions(new MasterReadOptions(masterReadOptions));
        }
        builder.setMutatingOptions(new MutatingOptions(mutatingOptions));

        return builder;
    }

    public static class Builder extends MutateNode.Builder<Builder, PutFileToCache> {
        @Nullable
        private YPath filePath;
        @Nullable
        private YPath cachePath;
        @Nullable
        private String md5;
        @Nullable
        private Boolean preserveExpirationTimeout;
        @Nullable
        private MasterReadOptions masterReadOptions;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.filePath = builder.filePath;
            this.cachePath = builder.cachePath;
            this.md5 = builder.md5;
            this.preserveExpirationTimeout = builder.preserveExpirationTimeout;
            if (builder.masterReadOptions != null) {
                this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
            }
        }

        public Builder setFilePath(YPath filePath) {
            this.filePath = filePath;
            return self();
        }

        public Builder setCachePath(YPath cachePath) {
            this.cachePath = cachePath;
            return self();
        }

        public Builder setMd5(String md5) {
            this.md5 = md5;
            return self();
        }

        public Builder setPreserveExpirationTimeout(Boolean preserveExpirationTimeout) {
            this.preserveExpirationTimeout = preserveExpirationTimeout;
            return self();
        }

        public Builder setMasterReadOptions(MasterReadOptions masterReadOptions) {
            this.masterReadOptions = masterReadOptions;
            return self();
        }

        public PutFileToCache build() {
            return new PutFileToCache(this);
        }

        @Override
        public Builder self() {
            return this;
        }
    }
}
