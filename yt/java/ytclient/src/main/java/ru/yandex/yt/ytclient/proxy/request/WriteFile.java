package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class WriteFile extends RequestBase<WriteFile> {
    private final String path;

    private YTreeNode config = null;

    private Boolean computeMd5 = null;

    private TransactionalOptions transactionalOptions = null;
    private PrerequisiteOptions prerequisiteOptions = null;

    private long windowSize = 16000000L;
    private long packetSize = windowSize / 2;


    public WriteFile(String path) {
        this.path = path;
    }

    public WriteFile setWindowSize(long windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public WriteFile setPacketSize(long packetSize) {
        this.packetSize = packetSize;
        return this;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public WriteFile setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public WriteFile setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return this;
    }

    public WriteFile setComputeMd5(boolean flag) {
        this.computeMd5 = flag;
        return this;
    }

    public WriteFile setPrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        return this;
    }

    public TReqWriteFile.Builder writeTo(TReqWriteFile.Builder builder) {
        builder.setPath(path);

        if (computeMd5 != null) {
            builder.setComputeMd5(computeMd5);
        }
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
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Nonnull
    @Override
    protected WriteFile self() {
        return this;
    }
}
