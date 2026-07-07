package tech.ytsaurus.client.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TReqStartShuffle;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class StartShuffle extends RequestBase<StartShuffle.Builder, StartShuffle>
        implements HighLevelRequest<TReqStartShuffle.Builder> {

    private final String account;

    private final int partitionCount;

    private final GUID parentTransactionId;

    private final Boolean usePushBasedShuffle;

    @Nullable
    private final String medium;

    @Nullable
    private final Integer replicationFactor;

    @Nullable
    private final TableSchema schema;

    @Nullable
    private final YTreeMapNode pushConfig;

    public StartShuffle(BuilderBase<?> builder) {
        super(builder);
        this.account = builder.account;
        this.partitionCount = builder.partitionCount;
        this.parentTransactionId = builder.parentTransactionId;
        this.medium = builder.medium;
        this.replicationFactor = builder.replicationFactor;
        this.schema = builder.schema;
        this.usePushBasedShuffle = builder.usePushBasedShuffle;
        this.pushConfig = builder.pushConfig;
    }

    public static StartShuffle.Builder builder() {
        return new StartShuffle.Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartShuffle.Builder, ?> requestBuilder) {
        TReqStartShuffle.Builder builder = requestBuilder.body();

        builder.setAccount(account)
                .setPartitionCount(partitionCount)
                .setParentTransactionId(RpcUtil.toProto(parentTransactionId))
                .setUsePushBasedShuffle(usePushBasedShuffle);

        if (medium != null) {
            builder.setMedium(medium);
        }

        if (replicationFactor != null) {
            builder.setReplicationFactor(replicationFactor);
        }

        if (schema != null) {
            builder.setSchema(ApiServiceUtil.serializeTableSchema(schema));
        }

        if (pushConfig != null) {
            builder.setPushConfig(ByteString.copyFrom(pushConfig.toBinary()));
        }
    }

    @Override
    public StartShuffle.Builder toBuilder() {
        return builder().setAccount(account)
                .setPartitionCount(partitionCount)
                .setParentTransactionId(parentTransactionId)
                .setMedium(medium)
                .setReplicationFactor(replicationFactor)
                .setSchema(schema)
                .setUsePushBasedShuffle(usePushBasedShuffle)
                .setPushConfig(pushConfig);
    }

    public static class Builder extends StartShuffle.BuilderBase<StartShuffle.Builder> {
        @Override
        protected StartShuffle.Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends StartShuffle.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, StartShuffle> {

        private String account;

        private int partitionCount;

        private GUID parentTransactionId;

        private Boolean usePushBasedShuffle = false;

        @Nullable
        private String medium = null;

        @Nullable
        private Integer replicationFactor = null;

        @Nullable
        private TableSchema schema = null;

        @Nullable
        private YTreeMapNode pushConfig = null;

        public TBuilder setAccount(String account) {
            this.account = account;
            return self();
        }

        public TBuilder setPartitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return self();
        }

        public TBuilder setParentTransactionId(GUID parentTransactionId) {
            this.parentTransactionId = parentTransactionId;
            return self();
        }

        public TBuilder setUsePushBasedShuffle(Boolean usePushBasedShuffle) {
            this.usePushBasedShuffle = usePushBasedShuffle;
            return self();
        }

        public TBuilder setMedium(@Nullable String medium) {
            this.medium = medium;
            return self();
        }

        public TBuilder setReplicationFactor(@Nullable Integer replicationFactor) {
            this.replicationFactor = replicationFactor;
            return self();
        }

        public TBuilder setSchema(@Nullable TableSchema schema) {
            this.schema = schema;
            return self();
        }

        public TBuilder setPushConfig(@Nullable YTreeMapNode pushConfig) {
            this.pushConfig = pushConfig;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            sb.append("account=").append(account).append(", ");
            sb.append("partitionCount=").append(partitionCount).append(", ");
            sb.append("parentTransactionId=").append(parentTransactionId).append(", ");
            sb.append("usePushBasedShuffle=").append(usePushBasedShuffle).append(", ");
            if (medium != null) {
                sb.append("medium=").append(medium).append(", ");
            }
            if (replicationFactor != null) {
                sb.append("replicationFactor=").append(replicationFactor).append(", ");
            }
            if (schema != null) {
                sb.append("schema=").append(schema).append(", ");
            }
            if (pushConfig != null) {
                sb.append("pushConfig=").append(pushConfig).append(", ");
            }
            super.writeArgumentsLogString(sb);
        }

        public StartShuffle build() {
            return new StartShuffle(this);
        }
    }
}
