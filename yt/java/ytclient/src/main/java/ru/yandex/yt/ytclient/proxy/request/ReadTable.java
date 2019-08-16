package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;

public class ReadTable<T> extends RequestBase<ReadTable<T>> {
    private final String path;
    private final WireRowDeserializer<T> deserializer;

    private boolean unordered = false;
    private boolean omitInaccessibleColumns = false;
    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;

    public ReadTable(String path, WireRowDeserializer<T> deserializer) {
        this.path = path;
        this.deserializer = deserializer;
    }

    public WireRowDeserializer<T> getDeserializer() {
        return this.deserializer;
    }

    public ReadTable<T> setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return this;
    }

    public ReadTable<T> setUnordered(boolean flag) {
        this.unordered = flag;
        return this;
    }

    public ReadTable<T> setOmitInaccessibleColumns(boolean flag) {
        this.omitInaccessibleColumns = flag;
        return this;
    }

    public ReadTable<T> setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public TReqReadTable.Builder writeTo(TReqReadTable.Builder builder) {
        builder.setUnordered(unordered);
        builder.setOmitInaccessibleColumns(omitInaccessibleColumns);
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
