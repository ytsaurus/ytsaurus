package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.rpcproxy.ERowsetFormat;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;
import ru.yandex.yt.ytclient.object.YTreeDeserializer;

public class ReadTable<T> extends RequestBase<ReadTable<T>> {
    private final YPath path;
    private final String stringPath;
    private final WireRowDeserializer<T> deserializer;

    private boolean unordered = false;
    private boolean omitInaccessibleColumns = false;
    private YTreeNode config = null;
    private ERowsetFormat desiredRowsetFormat = ERowsetFormat.RF_YT_WIRE;

    private TransactionalOptions transactionalOptions = null;

    public ReadTable(YPath path, WireRowDeserializer<T> deserializer) {
        this.path = path;
        this.stringPath = null;
        this.deserializer = deserializer;
    }

    public ReadTable(YPath path, YTreeObjectSerializer<T> serializer) {
        this.path = path;
        this.stringPath = null;
        this.deserializer = MappedRowsetDeserializer.forClass(serializer);
    }

    public ReadTable(YPath path, YTreeSerializer<T> serializer) {
        this.path = path;
        this.stringPath = null;
        if (serializer instanceof YTreeObjectSerializer) {
            this.deserializer = MappedRowsetDeserializer.forClass((YTreeObjectSerializer<T>) serializer);
        } else {
            this.deserializer = new YTreeDeserializer<>(serializer);
        }
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, WireRowDeserializer<T> deserializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, WireRowDeserializer<T> deserializer) {
        this.stringPath = path;
        this.path = null;
        this.deserializer = deserializer;
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, YTreeObjectSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeObjectSerializer<T> serializer) {
        this.stringPath = path;
        this.path = null;
        this.deserializer = MappedRowsetDeserializer.forClass(serializer);
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path,  YTreeSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeSerializer<T> serializer) {
        this.stringPath = path;
        this.path = null;
        if (serializer instanceof YTreeObjectSerializer) {
            this.deserializer = MappedRowsetDeserializer.forClass((YTreeObjectSerializer<T>) serializer);
        } else {
            this.deserializer = new YTreeDeserializer<>(serializer);
        }
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

    public ReadTable<T> setDesiredRowsetFormat(ERowsetFormat desiredRowsetFormat) {
        this.desiredRowsetFormat = desiredRowsetFormat;
        return this;
    }

    private String getPath() {
        return path != null ? path.toString() : stringPath;
    }

    public TReqReadTable.Builder writeTo(TReqReadTable.Builder builder) {
        builder.setUnordered(unordered);
        builder.setOmitInaccessibleColumns(omitInaccessibleColumns);
        builder.setPath(getPath());
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
        builder.setDesiredRowsetFormat(desiredRowsetFormat);
        return builder;
    }

    @Nonnull
    @Override
    protected ReadTable<T> self() {
        return this;
    }
}
