package tech.ytsaurus.client.request;

import java.util.List;

import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.core.tables.TableSchema;

public class MultiLookupSubrequest
        extends AbstractLookupRequestOptionsRequest<MultiLookupSubrequest.Builder, MultiLookupSubrequest> {

    protected MultiLookupSubrequest(BuilderBase<?> builder) {
        super(builder);
    }

    public MultiLookupSubrequest(String path, TableSchema schema) {
        this(builder().setPath(path).setSchema(schema));
    }

    public static Builder builder() {
        return new Builder();
    }
    
    @Override
    public Builder toBuilder() {
        return builder()
                .setFilters(filters)
                .setUnconvertedFilters(unconvertedFilters)
                .setPath(path)
                .setSchema(schema)
                .addLookupColumns(lookupColumns)
                .setTimestamp(timestamp)
                .setRetentionTimestamp(retentionTimestamp)
                .setKeepMissingRows(keepMissingRows)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }


    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public MultiLookupSubrequest build() {
            return new MultiLookupSubrequest(this);
        }
    }
    
    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractLookupRequestOptionsRequest.Builder<TBuilder, MultiLookupSubrequest> {

        /**
         * Construct empty builder.
         */
        public BuilderBase() {
        }

    }
}
