package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.ytclient.SerializationResolver;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;

/**
 * Base class for modify rows requests.
 * <p>
 * @see LookupRowsRequest
 * @see MappedLookupRowsRequest
 */
@NonNullApi
public abstract class AbstractLookupRowsRequest<
        TBuilder extends AbstractLookupRowsRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractLookupRowsRequest<TBuilder, TRequest>> extends RequestBase<TBuilder, TRequest> {
    protected final String path;
    protected final TableSchema schema;
    protected final List<String> lookupColumns;
    @Nullable
    protected final YtTimestamp timestamp;
    @Nullable
    protected final YtTimestamp retentionTimestamp;

    // NB. Java default of keepMissingRows is different from YT default for historical reasons,
    // now we have to keep backward compatibility.
    protected boolean keepMissingRows;

    AbstractLookupRowsRequest(Builder<?, ?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.schema = Objects.requireNonNull(builder.schema);
        if (!this.schema.isLookupSchema()) {
            throw new IllegalArgumentException("LookupRowsRequest requires a lookup schema");
        }
        this.lookupColumns = new ArrayList<>(Objects.requireNonNull(builder.lookupColumns));
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.keepMissingRows = builder.keepMissingRows;
    }

    /**
     * Get path of the table to be modified.
     */
    public String getPath() {
        return path;
    }

    public boolean getKeepMissingRows() {
        return keepMissingRows;
    }

    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
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

    public abstract void convertValues(SerializationResolver serializationResolver);

    public abstract void serializeRowsetTo(List<byte[]> attachments);

    public HighLevelRequest<TReqLookupRows.Builder> asLookupRowsWritable() {
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

            /**
             * Internal method: prepare request to send over network.
             */
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

    public HighLevelRequest<TReqVersionedLookupRows.Builder> asVersionedLookupRowsWritable() {
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

            /**
             * Internal method: prepare request to send over network.
             */
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

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends AbstractLookupRowsRequest<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
        @Nullable
        private String path;
        @Nullable
        private TableSchema schema;
        private final List<String> lookupColumns = new ArrayList<>();
        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        // NB. Java default of keepMissingRows is different from YT default for historical reasons,
        // now we have to keep backward compatibility.
        private boolean keepMissingRows = false;

        /**
         * Construct empty builder.
         */
        public Builder() {
        }

        /**
         * Set path of a table.
         */
        public TBuilder setPath(String path) {
            this.path = path;
            return self();
        }

        public TBuilder setSchema(TableSchema schema) {
            this.schema = schema;
            return self();
        }

        public TBuilder setKeepMissingRows(boolean keepMissingRows) {
            this.keepMissingRows = keepMissingRows;
            return self();
        }

        public TBuilder setTimestamp(@Nullable YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        public TBuilder setRetentionTimestamp(@Nullable YtTimestamp retentionTimestamp) {
            this.retentionTimestamp = retentionTimestamp;
            return self();
        }

        public TBuilder addLookupColumn(String name) {
            lookupColumns.add(name);
            return self();
        }

        public TBuilder addLookupColumns(List<String> names) {
            lookupColumns.addAll(names);
            return self();
        }

        public TBuilder addLookupColumns(String... names) {
            return addLookupColumns(Arrays.asList(names));
        }

        public String getPath() {
            return Objects.requireNonNull(path);
        }

        public boolean getKeepMissingRows() {
            return keepMissingRows;
        }

        public Optional<YtTimestamp> getTimestamp() {
            return Optional.ofNullable(timestamp);
        }

        public Optional<YtTimestamp> getRetentionTimestamp() {
            return Optional.ofNullable(retentionTimestamp);
        }

        public TableSchema getSchema() {
            return Objects.requireNonNull(schema);
        }

        public List<String> getLookupColumns() {
            return Collections.unmodifiableList(lookupColumns);
        }
    }
}
