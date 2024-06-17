package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAlterQuery;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * Immutable alter query request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#alterQuery(AlterQuery)
 */
public class AlterQuery extends QueryTrackerReq<AlterQuery.Builder, AlterQuery>
        implements HighLevelRequest<TReqAlterQuery.Builder> {
    private final GUID queryId;
    @Nullable
    private final YTreeMapNode annotations;
    @Nullable
    private final List<String> accessControlObjects;

    AlterQuery(Builder builder) {
        super(builder);
        this.queryId = Objects.requireNonNull(builder.queryId);
        this.annotations = builder.annotations;
        this.accessControlObjects = builder.accessControlObjects;
    }

    /**
     * Construct empty builder for alter query request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder()
                .setQueryId(queryId)
                .setAnnotations(annotations)
                .setAccessControlObjects(accessControlObjects)
                .setQueryTrackerStage(queryTrackerStage)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAlterQuery.Builder, ?> requestBuilder) {
        TReqAlterQuery.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setQueryId(RpcUtil.toProto(queryId));
        if (annotations != null) {
            builder.setAnnotations(ByteString.copyFrom(annotations.toBinary()));
        }
        if (accessControlObjects != null) {
            builder.setAccessControlObjects(
                    TReqAlterQuery.TAccessControlObjects.newBuilder()
                            .addAllItems(accessControlObjects)
                            .build()
            );
        }
    }

    /**
     * Builder for {@link AlterQuery}
     */
    public static class Builder extends QueryTrackerReq.Builder<AlterQuery.Builder, AlterQuery> {
        @Nullable
        private GUID queryId;
        @Nullable
        private YTreeMapNode annotations;
        @Nullable
        private List<String> accessControlObjects;

        private Builder() {
        }

        /**
         * Set query id.
         *
         * @return self
         */
        public Builder setQueryId(GUID queryId) {
            this.queryId = queryId;
            return self();
        }


        /**
         * Set new annotations for the query.
         *
         * @return self
         */
        public Builder setAnnotations(@Nullable YTreeMapNode annotations) {
            this.annotations = annotations;
            return self();
        }

        /**
         * Set new access control objects for the query.
         *
         * @return self
         */
        public Builder setAccessControlObjects(@Nullable List<String> accessControlObjects) {
            this.accessControlObjects = accessControlObjects;
            return self();
        }

        /**
         * Construct {@link AlterQuery} instance.
         */
        public AlterQuery build() {
            return new AlterQuery(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
