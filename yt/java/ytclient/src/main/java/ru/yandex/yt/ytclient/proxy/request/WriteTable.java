package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import com.google.protobuf.ByteString;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WriteTable extends RequestBase<WriteTable> {
    private final String path;

    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;

    private long windowSize = 16000000L;
    private long packetSize = windowSize/2;

    public WriteTable(String path) {
        this.path = path;
    }

    public WriteTable setWindowSize(long windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public WriteTable setPacketSize(long packetSize) {
        this.packetSize = packetSize;
        return this;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
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
}
