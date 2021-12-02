package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WriteTable<T> extends RequestBase<WriteTable<T>> {
    private YPath path;
    private final String stringPath;
    private final WireRowSerializer<T> serializer;

    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;

    private long windowSize = 16000000L;
    private long packetSize = windowSize / 2;

    private boolean needRetries = false;
    private int maxWritesInFlight = 1;
    private int chunkSize = 524288000;

    public WriteTable(WriteTable<T> other) {
        super(other);
        this.path = other.path;
        this.stringPath = other.stringPath;
        this.serializer = other.serializer;
        this.config = other.config;
        if (other.transactionalOptions != null) {
            this.transactionalOptions = new TransactionalOptions(other.transactionalOptions);
        }
        this.windowSize = other.windowSize;
        this.packetSize = other.packetSize;
        this.needRetries = other.needRetries;
        this.maxWritesInFlight = other.maxWritesInFlight;
        this.chunkSize = other.chunkSize;
    }

    public WriteTable(YPath path, WireRowSerializer<T> serializer) {
        this.path = path;
        this.stringPath = null;
        this.serializer = serializer;
    }

    public WriteTable(YPath path, YTreeSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    /**
     * @deprecated Use {@link #WriteTable(YPath path, WireRowSerializer<T> serializer)} instead.
     */
    @Deprecated
    public WriteTable(String path, WireRowSerializer<T> serializer) {
        this.stringPath = path;
        this.path = null;
        this.serializer = serializer;
    }

    /**
     * @deprecated Use {@link #WriteTable(YPath path, YTreeSerializer<T> serializer)} instead.
     */
    @Deprecated
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

    /**
     * If you need a writer with retries, set needRetries=true.
     * RetryPolicy should be set in RpcOptions
     * @param needRetries
     * @return self
     */
    public WriteTable<T> setNeedRetries(boolean needRetries) {
        this.needRetries = needRetries;
        return this;
    }

    /**
     * If a rows ordering doesn't matter, you can set maxWritesInFlight more than 1.
     * This will make writing faster.
     * @param maxWritesInFlight
     * @return self
     */
    public WriteTable<T> setMaxWritesInFlight(int maxWritesInFlight) {
        this.maxWritesInFlight = maxWritesInFlight;
        return this;
    }

    /**
     * Allows to regular a chunk size in the output table.
     * @param chunkSize
     * @return self
     */
    public WriteTable<T> setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public WriteTable<T> setPath(YPath path) {
        this.path = path;
        return this;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    /**
     * @see #setNeedRetries(boolean)
     */
    public boolean getNeedRetries() {
        return needRetries;
    }

    /**
     * @see #setMaxWritesInFlight(int)
     */
    public int getMaxWritesInFlight() {
        return maxWritesInFlight;
    }

    /**
     * @see #setChunkSize(int)
     */
    public int getChunkSize() {
        return chunkSize;
    }

    public WriteTable<T> setConfig(YTreeNode config) {
        this.config = config;
        return this;
    }

    public WriteTable<T> setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return this;
    }

    public Optional<GUID> getTransactionId() {
        if (this.transactionalOptions == null) {
            return Optional.empty();
        }
        return this.transactionalOptions.getTransactionId();
    }

    public String getPath() {
        return path != null ? path.toString() : stringPath;
    }

    public YPath getYPath() {
        return path;
    }

    public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
        builder.setPath(getPath());
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
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
