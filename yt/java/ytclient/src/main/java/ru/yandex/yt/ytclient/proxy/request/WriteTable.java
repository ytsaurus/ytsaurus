package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import com.google.protobuf.ByteString;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class WriteTable extends RequestBase<WriteTable> {
    private final String path;

    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;

    public WriteTable(String path) {
        this.path = path;
    }

    public WriteTable setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public WriteTable setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return this;
    }

    public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
        builder.setPath(path);
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            IoUtils.closeQuietly(baos);
            builder.setConfig(ByteString.copyFrom(data));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }
}
