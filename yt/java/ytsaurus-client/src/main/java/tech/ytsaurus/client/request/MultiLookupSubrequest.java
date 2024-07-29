package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;

public class MultiLookupSubrequest
        extends AbstractLookupRequest<MultiLookupSubrequest.Builder, MultiLookupSubrequest>
{

    protected MultiLookupSubrequest(BuilderBase<?> builder) {
        super(builder);
    }

    public MultiLookupSubrequest(String path, TableSchema schema) {
        this(builder().setPath(path).setSchema(schema));
    }

    /**
     * Internal method: prepare request to send over network.
     */
    public HighLevelRequest<TReqMultiLookup.Builder> asMultiLookupSubrequestWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqMultiLookup.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return MultiLookupSubrequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                MultiLookupSubrequest.this.writeHeaderTo(header);
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
                                .setPath(path)
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
        public MultiLookupSubrequest build() {
            return new MultiLookupSubrequest(this);
        }
    }
    
    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractLookupRequest.Builder<TBuilder, MultiLookupSubrequest> {

        /**
         * Construct empty builder.
         */
        public BuilderBase() {
        }

    }
}
