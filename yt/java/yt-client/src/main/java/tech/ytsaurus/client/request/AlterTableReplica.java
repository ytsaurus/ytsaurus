package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAlterTableReplica;

@NonNullFields
@NonNullApi
public class AlterTableReplica
        extends RequestBase<AlterTableReplica.Builder, AlterTableReplica>
        implements HighLevelRequest<TReqAlterTableReplica.Builder> {
    private final GUID replicaId;
    @Nullable private final Boolean enabled;
    @Nullable private final TableReplicaMode mode;
    @Nullable private final Boolean preserveTimestamps;
    @Nullable private final Atomicity atomicity;

    AlterTableReplica(Builder builder) {
        super(builder);
        this.replicaId = Objects.requireNonNull(builder.replicaId);
        this.enabled = builder.enabled;
        this.mode = builder.mode;
        this.preserveTimestamps = builder.preserveTimestamps;
        this.atomicity = builder.atomicity;
    }

    public AlterTableReplica(GUID replicaId) {
        this(builder().setReplicaId(replicaId));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAlterTableReplica.Builder, ?> requestBuilder) {
        TReqAlterTableReplica.Builder builder = requestBuilder.body();
        builder.setReplicaId(RpcUtil.toProto(replicaId));
        if (enabled != null) {
            builder.setEnabled(enabled);
        }
        if (mode != null) {
            builder.setMode(mode.getProtoValue());
        }
        if (preserveTimestamps != null) {
            builder.setPreserveTimestamps(preserveTimestamps);
        }
        if (atomicity != null) {
            builder.setAtomicity(atomicity.getProtoValue());
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("ReplicaId: ").append(replicaId).append("; ");
        if (enabled != null) {
            sb.append("Enabled: ").append(enabled).append("; ");
        }
        if (mode != null) {
            sb.append("Mode: ").append(mode).append("; ");
        }
        if (preserveTimestamps != null) {
            sb.append("PreserveTimestamps: ").append(preserveTimestamps).append("; ");
        }
        if (atomicity != null) {
            sb.append("Atomicity: ").append(atomicity).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setReplicaId(replicaId)
                .setEnabled(enabled)
                .setMode(mode)
                .setPreserveTimestamps(preserveTimestamps)
                .setAtomicity(atomicity)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder, AlterTableReplica> {
        @Nullable GUID replicaId;
        @Nullable Boolean enabled;
        @Nullable TableReplicaMode mode;
        @Nullable Boolean preserveTimestamps;
        @Nullable Atomicity atomicity;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.replicaId = builder.replicaId;
            this.enabled = builder.enabled;
            this.mode = builder.mode;
            this.preserveTimestamps = builder.preserveTimestamps;
            this.atomicity = builder.atomicity;
        }

        public Builder setReplicaId(GUID replicaId) {
            this.replicaId = replicaId;
            return self();
        }

        public Builder setEnabled(@Nullable Boolean enabled) {
            this.enabled = enabled;
            return self();
        }

        public Builder setMode(@Nullable TableReplicaMode mode) {
            this.mode = mode;
            return self();
        }

        public Builder setPreserveTimestamps(@Nullable Boolean preserveTimestamps) {
            this.preserveTimestamps = preserveTimestamps;
            return self();
        }

        public Builder setAtomicity(@Nullable Atomicity atomicity) {
            this.atomicity = atomicity;
            return self();
        }

        public AlterTableReplica build() {
            return new AlterTableReplica(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
