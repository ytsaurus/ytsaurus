package tech.ytsaurus.client.discovery;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.TReqListGroups;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.request.RequestBase;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;

public class ListGroups extends RequestBase<ListGroups.Builder, ListGroups>
        implements HighLevelRequest<TReqListGroups.Builder> {
    private final String prefix;
    private final ListGroupsOptions options;

    ListGroups(Builder builder) {
        super(builder);
        this.prefix = Objects.requireNonNull(builder.prefix);
        this.options = Objects.requireNonNull(builder.options);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPrefix(prefix)
                .setOptions(options)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqListGroups.Builder, ?> requestBuilder) {
        TReqListGroups.Builder builder = requestBuilder.body();
        builder.setPrefix(prefix);
        builder.setOptions(options.toProto());
    }

    public static class Builder extends RequestBase.Builder<ListGroups.Builder, ListGroups> {
        @Nullable
        private String prefix;
        private ListGroupsOptions options = new ListGroupsOptions();

        private Builder() {
        }

        public Builder setPrefix(String prefix) {
            this.prefix = prefix;
            return self();
        }

        public Builder setOptions(ListGroupsOptions options) {
            this.options = options;
            return self();
        }

        @Override
        public ListGroups build() {
            return new ListGroups(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
