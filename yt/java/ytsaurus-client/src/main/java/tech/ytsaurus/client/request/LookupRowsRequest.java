package tech.ytsaurus.client.request;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

public class LookupRowsRequest extends AbstractLookupRowsRequest<LookupRowsRequest.Builder, LookupRowsRequest> {

    public LookupRowsRequest(BuilderBase<?> builder) {
        super(builder);
    }

    public LookupRowsRequest(String path, TableSchema schema) {
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
        public LookupRowsRequest build() {
            return new LookupRowsRequest(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractLookupRowsRequest.Builder<TBuilder, LookupRowsRequest> {

        public TBuilder addFilter(List<?> filter) {
            unconvertedFilters.add(Objects.requireNonNull(filter));
            return self();
        }

        public TBuilder addFilter(Object... filterValues) {
            return addFilter(Arrays.asList(filterValues));
        }

        public TBuilder addFilters(Iterable<? extends List<?>> filters) {
            for (List<?> filter : filters) {
                addFilter(filter);
            }
            return self();
        }

        TBuilder setUnconvertedFilters(List<List<?>> unconvertedFilters) {
            this.unconvertedFilters.clear();
            this.unconvertedFilters.addAll(unconvertedFilters);
            return self();
        }

        TBuilder setFilters(List<UnversionedRow> filters) {
            this.filters.clear();
            this.filters.addAll(filters);
            return self();
        }
    }
}
