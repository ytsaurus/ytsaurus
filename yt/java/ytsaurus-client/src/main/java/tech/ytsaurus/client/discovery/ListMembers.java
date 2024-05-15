package tech.ytsaurus.client.discovery;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.TReqListMembers;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.request.RequestBase;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;

public class ListMembers extends RequestBase<ListMembers.Builder, ListMembers>
        implements HighLevelRequest<TReqListMembers.Builder> {
    private final String groupId;
    private final ListMembersOptions options;

    ListMembers(Builder builder) {
        super(builder);
        this.groupId = Objects.requireNonNull(builder.groupId);
        this.options = Objects.requireNonNull(builder.options);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setGroupId(groupId)
                .setOptions(options)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqListMembers.Builder, ?> requestBuilder) {
        TReqListMembers.Builder builder = requestBuilder.body();
        builder.setGroupId(groupId);
        builder.setOptions(options.toProto());
    }

    public static class Builder extends RequestBase.Builder<ListMembers.Builder, ListMembers> {
        @Nullable
        private String groupId;
        private ListMembersOptions options = new ListMembersOptions();

        private Builder() {
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return self();
        }

        public Builder setOptions(ListMembersOptions options) {
            this.options = options;
            return self();
        }

        @Override
        public ListMembers build() {
            return new ListMembers(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
