package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TLegacyAttributeKeys;
import tech.ytsaurus.rpcproxy.TMasterReadOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqGetNode;
import tech.ytsaurus.rpcproxy.TSuppressableAccessTrackingOptions;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

public class GetNode extends GetLikeReq<GetNode.Builder, GetNode> implements HighLevelRequest<TReqGetNode.Builder> {
    public GetNode(BuilderBase<?> builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetNode.Builder, ?> builder) {
        builder.body().setPath(path.toString());
        if (attributes != null) {
            // TODO(max42): switch to modern "attributes" field.
            builder.body().setLegacyAttributes(attributes.writeTo(TLegacyAttributeKeys.newBuilder()));
        }
        if (maxSize != null) {
            builder.body().setMaxSize(maxSize);
        }
        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.body().setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (masterReadOptions != null) {
            builder.body().setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
        if (suppressableAccessTrackingOptions != null) {
            builder.body().setSuppressableAccessTrackingOptions(
                    suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder())
            );
        }
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
    }

    @Override
    public Builder toBuilder() {
        return new Builder()
                .setPath(path)
                .setAttributes(attributes)
                .setMaxSize(maxSize)
                .setMasterReadOptions(masterReadOptions != null
                        ? new MasterReadOptions(masterReadOptions)
                        : null)
                .setSuppressableAccessTrackingOptions(suppressableAccessTrackingOptions != null
                        ? new SuppressableAccessTrackingOptions(suppressableAccessTrackingOptions)
                        : null)
                .setTransactionalOptions(transactionalOptions != null
                        ? new TransactionalOptions(transactionalOptions)
                        : null)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
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
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends GetLikeReq.Builder<TBuilder, GetNode> {
        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
        }

        @Override
        public GetNode build() {
            return new GetNode(this);
        }
    }
}
