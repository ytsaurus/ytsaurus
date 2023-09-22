package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.core.tables.TableSchema;

public class LookupRowsRequest extends AbstractLookupRowsRequest<LookupRowsRequest.Builder, LookupRowsRequest> {
    private final List<UnversionedRow> filters;
    private final List<List<?>> unconvertedFilters;

    public LookupRowsRequest(BuilderBase<?> builder) {
        super(builder);
        this.unconvertedFilters = new ArrayList<>(builder.unconvertedFilters);
        this.filters = new ArrayList<>(builder.filters);
    }

    public LookupRowsRequest(String path, TableSchema schema) {
        this(builder().setPath(path).setSchema(schema));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void convertValues(SerializationResolver serializationResolver) {
        this.filters.addAll(this.unconvertedFilters.stream().map(
                filter -> convertFilterToRow(filter, serializationResolver)).collect(Collectors.toList()));
        this.unconvertedFilters.clear();
    }

    private UnversionedRow convertFilterToRow(List<?> filter, SerializationResolver serializationResolver) {
        if (filter.size() != schema.getColumns().size()) {
            throw new IllegalArgumentException("Number of filter columns must match the number key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(schema.getColumns().size());
        ApiServiceUtil.convertKeyColumns(row, schema, filter, serializationResolver);
        return new UnversionedRow(row);
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(filters, new UnversionedRowSerializer(getSchema()));
        writer.finish();
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
        private final List<List<?>> unconvertedFilters = new ArrayList<>();
        private final List<UnversionedRow> filters = new ArrayList<>();

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
