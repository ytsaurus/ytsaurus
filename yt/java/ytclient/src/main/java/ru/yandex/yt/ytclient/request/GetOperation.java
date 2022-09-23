package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TReqGetOperation;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.MasterReadOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public class GetOperation
          extends RequestBase<GetOperation.Builder>
          implements HighLevelRequest<TReqGetOperation.Builder> {
    private final GUID guid;
    private final List<String> attributes;

    @Nullable
    private final MasterReadOptions masterReadOptions;
    private final boolean includeRuntime;

    GetOperation(BuilderBase<?> builder) {
        super(builder);
        this.guid = Objects.requireNonNull(builder.guid);
        this.attributes = new ArrayList<>(builder.attributes);
        if (builder.masterReadOptions != null) {
            this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
        } else {
            this.masterReadOptions = null;
        }
        this.includeRuntime = builder.includeRuntime;
    }

    public GetOperation(GUID guid) {
        this(builder().setId(guid));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetOperation.Builder, ?> requestBuilder) {
        TReqGetOperation.Builder builder = requestBuilder.body()
                .setOperationId(RpcUtil.toProto(guid))
                // TODO(max42): switch to modern "attributes" field.
                .addAllLegacyAttributes(attributes)
                .setIncludeRuntime(includeRuntime);

        if (masterReadOptions != null) {
            builder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Id: ").append(guid).append("; ");
        if (!attributes.isEmpty()) {
            sb.append("Attributes: ").append(attributes).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Override
    public Builder toBuilder() {
        Builder builder =  builder()
                .setId(guid)
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

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>>
            extends ru.yandex.yt.ytclient.proxy.request.RequestBase<T>
            implements HighLevelRequest<TReqGetOperation.Builder> {
        @Nullable
        private GUID guid;
        private List<String> attributes = new ArrayList<>();

        @Nullable
        private MasterReadOptions masterReadOptions;
        private boolean includeRuntime;

        protected BuilderBase() {
        }

        BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.guid = builder.guid;
            this.attributes = new ArrayList<>(builder.attributes);
            if (builder.masterReadOptions != null) {
                this.masterReadOptions = new MasterReadOptions(builder.masterReadOptions);
            }
            this.includeRuntime = builder.includeRuntime;
        }

        public T setId(GUID id) {
            this.guid = id;
            return self();
        }

        public T setMasterReadOptions(MasterReadOptions masterReadOptions) {
            this.masterReadOptions = masterReadOptions;
            return self();
        }

        public T includeRuntime(boolean includeRuntime) {
            this.includeRuntime = includeRuntime;
            return self();
        }

        public T addAttribute(String attribute) {
            this.attributes.add(attribute);
            return self();
        }

        public T setAttributes(Collection<String> attributes) {
            this.attributes.clear();
            this.attributes.addAll(attributes);
            return self();
        }

        public GetOperation build() {
            return new GetOperation(this);
        }

        @Override
        public void writeTo(RpcClientRequestBuilder<TReqGetOperation.Builder, ?> requestBuilder) {
            TReqGetOperation.Builder builder = requestBuilder.body()
                    .setOperationId(RpcUtil.toProto(Objects.requireNonNull(guid)))
                    // TODO(max42): switch to modern "attributes" field.
                    .addAllLegacyAttributes(attributes)
                    .setIncludeRuntime(includeRuntime);

            if (masterReadOptions != null) {
                builder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
            }
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            sb.append("Id: ").append(guid).append("; ");
            if (!attributes.isEmpty()) {
                sb.append("Attributes: ").append(attributes).append("; ");
            }
            super.writeArgumentsLogString(sb);
        }
    }
}
