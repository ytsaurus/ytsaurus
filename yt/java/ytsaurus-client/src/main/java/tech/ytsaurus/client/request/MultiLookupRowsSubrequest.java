package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;

public class MultiLookupRowsSubrequest
        extends AbstractLookupRequest<MultiLookupRowsSubrequest.Builder, MultiLookupRowsSubrequest> {

    protected MultiLookupRowsSubrequest(BuilderBase<?> builder) {
        super(builder);
    }

    public MultiLookupRowsSubrequest(String path, TableSchema schema) {
        this(builder().setPath(path).setSchema(schema));
    }

    /**
     * Internal method: prepare request to send over network.
     */
    public HighLevelRequest<TReqMultiLookup.Builder> asMultiLookupRowsSubrequestWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqMultiLookup.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return MultiLookupRowsSubrequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                MultiLookupRowsSubrequest.this.writeHeaderTo(header);
            }

            /**
             * Internal method: prepare request to send over network.
             */
            @Override
            public void writeTo(RpcClientRequestBuilder<TReqMultiLookup.Builder, ?> builder) {
                List<byte[]> subAttachments = new ArrayList<>();
                serializeRowsetTo(subAttachments);
                builder.attachments().addAll(subAttachments);

                var rowset = ApiServiceUtil.makeRowsetDescriptor(getSchema());

                builder.body().addSubrequests(
                        TReqMultiLookup.TSubrequest.newBuilder()
                                .setPath(ByteString.copyFromUtf8(path))
                                .addAllColumns(lookupColumns)
                                .setKeepMissingRows(keepMissingRows)
                                .setRowsetDescriptor(rowset)
                                .setAttachmentCount(subAttachments.size())
                                .build()
                );
            }
        };
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
        public MultiLookupRowsSubrequest build() {
            return new MultiLookupRowsSubrequest(this);
        }
    }

    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractLookupRequest.Builder<TBuilder, MultiLookupRowsSubrequest> {

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
