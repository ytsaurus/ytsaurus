package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqLookupRows;
import tech.ytsaurus.rpcproxy.TReqVersionedLookupRows;

/**
 * Base class for lookup rows requests.
 * <p>
 * Users use one of the inheritors of this class.
 * <p>
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#lookup_rows">
 * lookup_rows documentation
 * </a>
 */
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

    protected AbstractLookupRowsRequest(Builder<?, ?> builder) {
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
     * Get path parameter.
     *
     * @see Builder#setPath(String)
     */
    public String getPath() {
        return path;
    }

    /**
     * Get schema parameter.
     *
     * @see Builder#setSchema(TableSchema)
     */
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * Get keep-missing-rows parameter.
     *
     * @see Builder#setKeepMissingRows(boolean)
     */
    public boolean getKeepMissingRows() {
        return keepMissingRows;
    }

    /**
     * Get timestamp parameter.
     *
     * @see Builder#setTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    /**
     * Get retention-timestamp parameter.
     *
     * @see Builder#setRetentionTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    /**
     * Get lookup-columns parameter.
     *
     * @see Builder#addLookupColumn(String)
     */
    public List<String> getLookupColumns() {
        return Collections.unmodifiableList(lookupColumns);
    }

    /**
     * Internal method.
     */
    public abstract void convertValues(SerializationResolver serializationResolver);

    /**
     * Internal method.
     */
    public abstract void serializeRowsetTo(List<byte[]> attachments);

    /**
     * Internal method: prepare request to send over network.
     */
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

    /**
     * Internal method: prepare request to send over network.
     */
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

    /**
     * Base class for builders of LookupRows requests.
     */
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
         * <b>Required.</b> Set path of a table to lookup.
         */
        public TBuilder setPath(String path) {
            this.path = path;
            return self();
        }

        /**
         * <b>Required.</b> Set schema of key columns used to lookup rows.
         * <p>
         * It must be "lookup schema". Such schema contains only key columns of a table.
         * Schema is used to properly serialize and send to server keys to lookup.
         * <p>
         *
         * @see TableSchema#toLookup()
         */
        public TBuilder setSchema(TableSchema schema) {
            this.schema = schema;
            return self();
        }

        /**
         * Whether to keep rows that were not found.
         * <p>
         * When this parameter is set to false (default) YT returns only rows that were found in table.
         * <p>
         * If this parameter is set to true then the size of the returned rowset is equal to
         * the number of keys in request. The order of resulting rows corresponds to the order of requested keys
         * and rows for missing keys are `null`.
         */
        public TBuilder setKeepMissingRows(boolean keepMissingRows) {
            this.keepMissingRows = keepMissingRows;
            return self();
        }

        /**
         * Set version of a table to be used for lookup request.
         */
        public TBuilder setTimestamp(@Nullable YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        /**
         * Set lower boundary for value timestamps to be returned.
         * I.e. values that were written before this timestamp are ignored and not returned.
         */
        public TBuilder setRetentionTimestamp(@Nullable YtTimestamp retentionTimestamp) {
            this.retentionTimestamp = retentionTimestamp;
            return self();
        }

        /**
         * Add column name to be returned
         * <p>
         * By default, YT returns all columns of the table.
         * If some columns are unnecessary user can specify only required ones using addLookupColumn(s) method.
         */
        public TBuilder addLookupColumn(String name) {
            lookupColumns.add(name);
            return self();
        }

        /**
         * Add column names to be returned.
         *
         * @see #addLookupColumn
         */
        public TBuilder addLookupColumns(List<String> names) {
            lookupColumns.addAll(names);
            return self();
        }

        /**
         * Add column names to be returned.
         *
         * @see #addLookupColumn
         */
        public TBuilder addLookupColumns(String... names) {
            return addLookupColumns(Arrays.asList(names));
        }

        /**
         * Get value of path parameter.
         *
         * @see #setPath
         */
        public String getPath() {
            return Objects.requireNonNull(path);
        }

        /**
         * Get value of keep-missing-rows parameter.
         *
         * @see #setKeepMissingRows(boolean)
         */
        public boolean getKeepMissingRows() {
            return keepMissingRows;
        }

        /**
         * Get value of timestamp parameter.
         *
         * @see #setTimestamp parameter
         */
        public Optional<YtTimestamp> getTimestamp() {
            return Optional.ofNullable(timestamp);
        }

        /**
         * Get value of retention-timestamp parameter.
         *
         * @see #setRetentionTimestamp
         */
        public Optional<YtTimestamp> getRetentionTimestamp() {
            return Optional.ofNullable(retentionTimestamp);
        }

        /**
         * Get value of schema parameter
         *
         * @see #setSchema
         */
        public TableSchema getSchema() {
            return Objects.requireNonNull(schema);
        }

        /**
         * Get value of lookup-columns parameter
         *
         * @see #addLookupColumns(String...)
         */
        public List<String> getLookupColumns() {
            return Collections.unmodifiableList(lookupColumns);
        }
    }
}
