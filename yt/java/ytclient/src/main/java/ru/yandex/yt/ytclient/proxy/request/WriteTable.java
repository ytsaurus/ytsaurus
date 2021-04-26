package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WriteTable<T> extends RequestBase<WriteTable<T>> {
    private final String path;
    private final WireRowSerializer<T> serializer;

    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;

    private long windowSize = 16000000L;
    private long packetSize = windowSize / 2;

    public WriteTable(String path, WireRowSerializer<T> serializer) {
        this.path = path;
        this.serializer = serializer;
    }

    public WriteTable(String path, YTreeSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public WireRowSerializer<T> getSerializer() {
        return this.serializer;
    }

    public WriteTable<T> setWindowSize(long windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public WriteTable<T> setPacketSize(long packetSize) {
        this.packetSize = packetSize;
        return this;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public WriteTable<T> setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public WriteTable<T> setTransactionalOptions(TransactionalOptions to) {
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
        } else {
            // TODO: remove this HACK
            builder.setConfig(ByteString.copyFrom("{}", UTF_8));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Nonnull
    @Override
    protected WriteTable<T> self() {
        return this;
    }
}
