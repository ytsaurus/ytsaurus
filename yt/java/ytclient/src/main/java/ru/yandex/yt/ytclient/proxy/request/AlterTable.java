package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class AlterTable extends TableReq<AlterTable> implements HighLevelRequest<TReqAlterTable.Builder> {
    private YTreeNode schemaNode;
    private Boolean dynamic;
    private GUID upstreamReplicaId;
    private TransactionalOptions transactionalOptions;

    public AlterTable(YPath path) {
        super(path.justPath());
    }

    /**
     * @deprecated Use {@link #AlterTable(YPath path)} instead.
     */
    @Deprecated
    public AlterTable(String path) {
        super(path);
    }

    public AlterTable setSchema(TableSchema schema) {
        this.schemaNode = schema.toYTree();
        return this;
    }

    /**
     * Альтернативный способ задания схемы - по аналогии с {@link CreateNode}
     *
     * @param schema схема
     * @return текущий объект
     */
    public AlterTable setSchema(YTreeNode schema) {
        this.schemaNode = schema;
        return this;
    }

    public AlterTable setDynamic(boolean f) {
        this.dynamic = f;
        return this;
    }

    public AlterTable setUpstreamReplicaId(GUID guid) {
        this.upstreamReplicaId = guid;
        return this;
    }

    public AlterTable setTransactionalOptions(TransactionalOptions opt) {
        this.transactionalOptions = opt;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAlterTable.Builder, ?> requestBuilder) {
        TReqAlterTable.Builder builder = requestBuilder.body();

        super.writeTo(builder);

        if (schemaNode != null) {
            builder.setSchema(ByteString.copyFrom(schemaNode.toBinary()));
        }

        if (dynamic != null) {
            builder.setDynamic(dynamic);
        }

        if (upstreamReplicaId != null) {
            builder.setUpstreamReplicaId(RpcUtil.toProto(upstreamReplicaId));
        }

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.toProto());
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        if (schemaNode != null) {
            sb.append("Schema: ").append(schemaNode.toString()).append("; ");
        }
        if (dynamic != null) {
            sb.append("Dynamic: ").append(dynamic).append("; ");

        }
    }

    @Nonnull
    @Override
    protected AlterTable self() {
        return this;
    }
}
