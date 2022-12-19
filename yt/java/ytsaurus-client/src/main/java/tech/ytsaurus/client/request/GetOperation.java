package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TMasterReadOptions;
import tech.ytsaurus.rpcproxy.TReqGetOperation;

public class GetOperation
        extends OperationReq<GetOperation.Builder, GetOperation>
        implements HighLevelRequest<TReqGetOperation.Builder> {
    private final List<String> attributes;

    @Nullable
    private final MasterReadOptions masterReadOptions;
    private final boolean includeRuntime;

    public GetOperation(BuilderBase<?> builder) {
        super(builder);
        this.attributes = new ArrayList<>(builder.attributes);
        if (builder.masterReadOptions != null) {
            this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
        } else {
            this.masterReadOptions = null;
        }
        this.includeRuntime = builder.includeRuntime;
    }

    public GetOperation(GUID guid) {
        this(builder().setOperationId(guid));
    }

    public GetOperation(String alias) {
        this(builder().setOperationAlias(alias));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetOperation.Builder, ?> requestBuilder) {
        TReqGetOperation.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        messageBuilder
                // TODO(max42): switch to modern "attributes" field.
                .addAllLegacyAttributes(attributes)
                .setIncludeRuntime(includeRuntime);

        if (masterReadOptions != null) {
            messageBuilder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        if (!attributes.isEmpty()) {
            sb.append("Attributes: ").append(attributes).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setAttributes(new ArrayList<>(attributes))
                .includeRuntime(includeRuntime)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        if (masterReadOptions != null) {
            builder.setMasterReadOptions(new MasterReadOptions(masterReadOptions));
        }
        return builder;
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends OperationReq.Builder<TBuilder, GetOperation> {
        private List<String> attributes = new ArrayList<>();

        @Nullable
        private MasterReadOptions masterReadOptions;
        private boolean includeRuntime;

        protected BuilderBase() {
        }

        BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.attributes = new ArrayList<>(builder.attributes);
            if (builder.masterReadOptions != null) {
                this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
            }
            this.includeRuntime = builder.includeRuntime;
        }

        public TBuilder setMasterReadOptions(MasterReadOptions masterReadOptions) {
            this.masterReadOptions = masterReadOptions;
            return self();
        }

        public TBuilder includeRuntime(boolean includeRuntime) {
            this.includeRuntime = includeRuntime;
            return self();
        }

        public TBuilder addAttribute(String attribute) {
            this.attributes.add(attribute);
            return self();
        }

        public TBuilder setAttributes(Collection<String> attributes) {
            this.attributes.clear();
            this.attributes.addAll(attributes);
            return self();
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            if (!attributes.isEmpty()) {
                sb.append("Attributes: ").append(attributes).append("; ");
            }
            super.writeArgumentsLogString(sb);
        }

        @Override
        public GetOperation build() {
            return new GetOperation(this);
        }
    }
}
