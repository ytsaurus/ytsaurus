package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAlterTableReplica;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullFields
@NonNullApi
public class AlterTableReplica
        extends RequestBase<AlterTableReplica>
        implements HighLevelRequest<TReqAlterTableReplica.Builder> {
    final GUID replicaId;
    @Nullable Boolean enabled;
    @Nullable TableReplicaMode mode;
    @Nullable Boolean preserveTimestamps;
    @Nullable Atomicity atomicity;

    public AlterTableReplica(GUID replicaId) {
        this.replicaId = replicaId;
    }

    public AlterTableReplica setEnabled(Boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public AlterTableReplica setMode(TableReplicaMode mode) {
        this.mode = mode;
        return this;
    }

    public AlterTableReplica setPreserveTimestamps(Boolean preserveTimestamps) {
        this.preserveTimestamps = preserveTimestamps;
        return this;
    }

    public AlterTableReplica setAtomicity(Atomicity atomicity) {
        this.atomicity = atomicity;
        return this;
    }

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

    @Nonnull
    @Override
    protected AlterTableReplica self() {
        return this;
    }
}
