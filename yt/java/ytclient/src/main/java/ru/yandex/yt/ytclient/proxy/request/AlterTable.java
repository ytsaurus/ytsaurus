package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import com.google.protobuf.ByteString;

import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class AlterTable extends TableReq<AlterTable> {
    private TableSchema schema;
    private Optional<Boolean> dynamic;
    private YtGuid upstreamReplicaId;
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

    public AlterTable setUpstreamReplicaId(YtGuid guid) {
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
            builder.setUpstreamReplicaId(upstreamReplicaId.toProto());
        }

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.toProto());
        }

        return builder;
    }
}
