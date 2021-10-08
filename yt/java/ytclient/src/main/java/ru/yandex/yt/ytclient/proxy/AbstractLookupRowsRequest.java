package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.tables.TableSchema;

@NonNullApi
public abstract class AbstractLookupRowsRequest<R extends AbstractLookupRowsRequest<R>> extends RequestBase<R> {
    private final String path;
    private final TableSchema schema;
    private final List<String> lookupColumns = new ArrayList<>();
    @Nullable private YtTimestamp timestamp;
    @Nullable private YtTimestamp retentionTimestamp;

    // NB. Java default of keepMissingRows is different from YT default for historical reasons,
    // now we have to keep backward compatibility.
    private boolean keepMissingRows = false;

    public AbstractLookupRowsRequest(String path, TableSchema schema) {
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
        if (!schema.isLookupSchema()) {
            throw new IllegalArgumentException("LookupRowsRequest requires a lookup schema");
        }
    }

    public String getPath() {
        return path;
    }

    public boolean getKeepMissingRows() {
        return keepMissingRows;
    }

    @SuppressWarnings("unchecked")
    public R setKeepMissingRows(boolean keepMissingRows) {
        this.keepMissingRows = keepMissingRows;
        return (R) this;
    }

    @SuppressWarnings("unchecked")
    public R setTimestamp(YtTimestamp timestamp) {
        this.timestamp = timestamp;
        return (R) this;
    }

    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    @SuppressWarnings("unchecked")
    public R setRetentionTimestamp(YtTimestamp retentionTimestamp) {
        this.retentionTimestamp = retentionTimestamp;
        return (R) this;
    }

    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<String> getLookupColumns() {
        return Collections.unmodifiableList(lookupColumns);
    }

    @SuppressWarnings("unchecked")
    public R addLookupColumn(String name) {
        lookupColumns.add(name);
        return (R) this;
    }

    @SuppressWarnings("unchecked")
    public R addLookupColumns(List<String> names) {
        lookupColumns.addAll(names);
        return (R) this;
    }

    public R addLookupColumns(String... names) {
        return addLookupColumns(Arrays.asList(names));
    }

    public abstract void serializeRowsetTo(List<byte[]> attachments);

    HighLevelRequest<TReqLookupRows.Builder> asLookupRowsWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqLookupRows.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return AbstractLookupRowsRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                AbstractLookupRowsRequest.this.writeHeaderTo(header);
            }

            @Override
            public void writeTo(RpcClientRequestBuilder<TReqLookupRows.Builder, ?> builder) {
                builder.body().setPath(getPath());
                builder.body().addAllColumns(getLookupColumns());
                builder.body().setKeepMissingRows(getKeepMissingRows());
                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                if (getRetentionTimestamp().isPresent()) {
                    builder.body().setRetentionTimestamp(getRetentionTimestamp().get().getValue());
                }
                builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(getSchema()));
                serializeRowsetTo(builder.attachments());
            }
        };
    }

    HighLevelRequest<TReqVersionedLookupRows.Builder> asVersionedLookupRowsWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqVersionedLookupRows.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return AbstractLookupRowsRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                AbstractLookupRowsRequest.this.writeHeaderTo(header);
            }

            @Override
            public void writeTo(RpcClientRequestBuilder<TReqVersionedLookupRows.Builder, ?> builder) {
                builder.body().setPath(getPath());
                builder.body().addAllColumns(getLookupColumns());
                builder.body().setKeepMissingRows(getKeepMissingRows());
                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(getSchema()));
                serializeRowsetTo(builder.attachments());
            }
        };
    }
}
