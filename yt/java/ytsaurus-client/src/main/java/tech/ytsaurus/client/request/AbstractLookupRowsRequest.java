package tech.ytsaurus.client.request;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqLookupRows;
import tech.ytsaurus.rpcproxy.TReqVersionedLookupRows;

/**
 * Base class for lookup rows requests.
 * <p>
 * Users use one of the inheritors of this class.
 * <p>
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#lookup_rows">
 * lookup_rows documentation
 * </a>
 */
public abstract class AbstractLookupRowsRequest<
        TBuilder extends AbstractLookupRowsRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractLookupRowsRequest<TBuilder, TRequest>> extends AbstractLookupRequestOptionsRequest<TBuilder, TRequest> {

    protected AbstractLookupRowsRequest(Builder<?, ?> builder) {
        super(builder);
    }


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
                if (getReplicaConsistency().isPresent()) {
                    builder.body().setReplicaConsistency(getReplicaConsistency().get().getProtoValue());
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
            extends AbstractLookupRequestOptionsRequest.Builder<TBuilder, TRequest> {

        /**
         * Construct empty builder.
         */
        public Builder() {
        }

    }
}
