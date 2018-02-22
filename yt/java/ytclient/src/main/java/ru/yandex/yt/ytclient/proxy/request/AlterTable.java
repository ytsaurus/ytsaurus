package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class AlterTable extends TableReq<AlterTable> {
    private TableSchema schema;
    private Optional<Boolean> dynamic;
    private GUID upstreamReplicaId;
    private TransactionalOptions transactionalOptions;

    public AlterTable(String path) {
        super(path);
    }

    public AlterTable setSchema(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    public AlterTable setDynamic(boolean f) {
        this.dynamic = Optional.of(f);
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
        }

        dynamic.ifPresent(x -> builder.setDynamic(x));

        if (upstreamReplicaId != null) {
            builder.setUpstreamReplicaId(RpcUtil.toProto(upstreamReplicaId));
        }

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.toProto());
        }

        return builder;
    }
}
