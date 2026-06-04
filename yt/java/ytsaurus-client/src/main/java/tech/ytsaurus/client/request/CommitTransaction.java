package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqCommitTransaction;

/**
 * Request for committing transaction.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#commit_tx">
 * commit_tx documentation
 * </a>
 */
public class CommitTransaction extends RequestBase<CommitTransaction.Builder, CommitTransaction>
        implements HighLevelRequest<TReqCommitTransaction.Builder> {
    private final GUID transactionId;
    @Nullable
    private final PrerequisiteOptions prerequisiteOptions;

    CommitTransaction(Builder builder) {
        super(builder);
        transactionId = Objects.requireNonNull(builder.transactionId);
        prerequisiteOptions = builder.prerequisiteOptions;
    }

    public CommitTransaction(GUID transactionId) {
        this(builder().setTransactionId(transactionId));
    }

    public static Builder builder() {
        return new Builder();
    }

    public GUID getTransactionId() {
        return transactionId;
    }

    @Nullable
    public PrerequisiteOptions getPrerequisiteOptions() {
        return prerequisiteOptions;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCommitTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
        if (prerequisiteOptions != null) {
            prerequisiteOptions.writeTo(builder.body().getPrerequisiteOptionsBuilder());
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
        if (prerequisiteOptions != null) {
            sb.append("PrerequisiteOptions: {");
            prerequisiteOptions.writeArgumentsLogString(sb);
            sb.append("}; ");
        }
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTransactionId(transactionId)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, CommitTransaction> {
        @Nullable
        private GUID transactionId;
        @Nullable
        private PrerequisiteOptions prerequisiteOptions;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            transactionId = builder.transactionId;
            prerequisiteOptions = builder.prerequisiteOptions;
        }

        public Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
            this.prerequisiteOptions = prerequisiteOptions;
            return this;
        }

        public CommitTransaction build() {
            return new CommitTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
