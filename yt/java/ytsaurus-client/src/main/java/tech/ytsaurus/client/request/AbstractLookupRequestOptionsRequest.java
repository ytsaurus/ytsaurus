package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.TableSchema;

public abstract class AbstractLookupRequestOptionsRequest<
        TBuilder extends AbstractLookupRequestOptionsRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractLookupRequestOptionsRequest<TBuilder, TRequest>> extends AbstractTabletReadRequest<TBuilder, TRequest> {

    protected final String path;
    protected final TableSchema schema;
    protected final List<String> lookupColumns;

    // NB. Java default of keepMissingRows is different from YT default for historical reasons,
    // now we have to keep backward compatibility.
    protected boolean keepMissingRows;

    protected AbstractLookupRequestOptionsRequest(Builder<?, ?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.schema = Objects.requireNonNull(builder.schema);
        if (!this.schema.isLookupSchema()) {
            throw new IllegalArgumentException("LookupRowsRequest requires a lookup schema");
        }
        this.lookupColumns = new ArrayList<>(Objects.requireNonNull(builder.lookupColumns));
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
    public abstract void serializeRowsetTo(List<byte[]> attachments);

    /**
     * Base class for builders of LookupRows & MultiLookup requests.
     */
    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends AbstractLookupRequestOptionsRequest<?, TRequest>>
            extends AbstractTabletReadRequest.Builder<TBuilder, TRequest> {
        @Nullable
        private String path;
        @Nullable
        private TableSchema schema;
        private final List<String> lookupColumns = new ArrayList<>();

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
