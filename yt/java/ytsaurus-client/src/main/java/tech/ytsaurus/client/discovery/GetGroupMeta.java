package tech.ytsaurus.client.discovery;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.TReqGetGroupMeta;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.request.RequestBase;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;

public class GetGroupMeta extends RequestBase<GetGroupMeta.Builder, GetGroupMeta>
        implements HighLevelRequest<TReqGetGroupMeta.Builder> {
    private final String groupId;

    GetGroupMeta(Builder builder) {
        super(builder);
        this.groupId = Objects.requireNonNull(builder.groupId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetGroupMeta.Builder, ?> requestBuilder) {
        TReqGetGroupMeta.Builder builder = requestBuilder.body();
        builder.setGroupId(groupId);
    }

    public static class Builder extends RequestBase.Builder<GetGroupMeta.Builder, GetGroupMeta> {
        @Nullable
        private String groupId;

        private Builder() {
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return self();
        }

        @Override
        public GetGroupMeta build() {
            return new GetGroupMeta(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
