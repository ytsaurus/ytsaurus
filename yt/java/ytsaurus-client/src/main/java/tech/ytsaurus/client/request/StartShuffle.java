package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqStartShuffle;

public class StartShuffle extends RequestBase<StartShuffle.Builder, StartShuffle>
        implements HighLevelRequest<TReqStartShuffle.Builder> {

    private final String account;

    private final int partitionCount;

    private final GUID parentTransactionId;

    @Nullable
    private final String medium;

    @Nullable
    private final Integer replicationFactor;

    public StartShuffle(BuilderBase<?> builder) {
        super(builder);
        this.account = builder.account;
        this.partitionCount = builder.partitionCount;
        this.parentTransactionId = builder.parentTransactionId;
        this.medium = builder.medium;
        this.replicationFactor = builder.replicationFactor;
    }

    public static StartShuffle.Builder builder() {
        return new StartShuffle.Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartShuffle.Builder, ?> requestBuilder) {
        TReqStartShuffle.Builder builder = requestBuilder.body();

        builder.setAccount(account)
                .setPartitionCount(partitionCount)
                .setParentTransactionId(RpcUtil.toProto(parentTransactionId));

        if (medium != null) {
            builder.setMedium(medium);
        }

        if (replicationFactor != null) {
            builder.setReplicationFactor(replicationFactor);
        }
    }

    @Override
    public StartShuffle.Builder toBuilder() {
        return builder().setAccount(account)
                .setPartitionCount(partitionCount)
                .setParentTransactionId(parentTransactionId)
                .setMedium(medium)
                .setReplicationFactor(replicationFactor);
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

        @Nullable
        private String medium = null;

        @Nullable
        private Integer replicationFactor = null;

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

        public TBuilder setMedium(@Nullable String medium) {
            this.medium = medium;
            return self();
        }

        public TBuilder setReplicationFactor(@Nullable Integer replicationFactor) {
            this.replicationFactor = replicationFactor;
            return self();
        }

        public StartShuffle build() {
            return new StartShuffle(this);
        }
    }
}
