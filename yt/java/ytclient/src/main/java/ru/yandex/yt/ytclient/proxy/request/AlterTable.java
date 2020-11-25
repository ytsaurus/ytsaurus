package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class AlterTable extends TableReq<AlterTable> {
    private TableSchema schema;
    private YTreeNode schemaNode;
    private Boolean dynamic;
    private GUID upstreamReplicaId;
    private TransactionalOptions transactionalOptions;

    public AlterTable(String path) {
        super(path);
    }

    public AlterTable setSchema(TableSchema schema) {
        this.schema = schema;
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

    public TReqAlterTable.Builder writeTo(TReqAlterTable.Builder builder) {
        super.writeTo(builder);

        if (schema != null) {
            builder.setSchema(ByteString.copyFrom(schema.toYTree().toBinary()));
        } else if (schemaNode != null) {
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

        return builder;
    }

    @Nonnull
    @Override
    protected AlterTable self() {
        return this;
    }
}
