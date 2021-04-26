package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class ReadFile extends RequestBase<ReadFile> {
    private final String path;

    private Long offset = null;
    private Long length = null;
    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;
    private SuppressableAccessTrackingOptions suppressableAccessTrackingOptions = null;

    public ReadFile(String path) {
        this.path = path;
    }

    public ReadFile setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return this;
    }

    public ReadFile setSuppressableAccessTrackingOptions(SuppressableAccessTrackingOptions s) {
        this.suppressableAccessTrackingOptions = s;
        return this;
    }

    public ReadFile setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public ReadFile setLength(long length) {
        this.length = length;
        return this;
    }

    public ReadFile setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public TReqReadFile.Builder writeTo(TReqReadFile.Builder builder) {
        builder.setPath(path);
        if (offset != null) {
            builder.setOffset(offset);
        }
        if (length != null) {
            builder.setLength(length);
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
        if (suppressableAccessTrackingOptions != null) {
            builder.setSuppressableAccessTrackingOptions(
                    suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder())
            );
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Nonnull
    @Override
    protected ReadFile self() {
        return this;
    }
}
