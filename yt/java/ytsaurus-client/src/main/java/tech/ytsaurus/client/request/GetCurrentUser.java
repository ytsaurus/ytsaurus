package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqGetCurrentUser;

public class GetCurrentUser extends RequestBase<GetCurrentUser.Builder, GetCurrentUser>
        implements HighLevelRequest<TReqGetCurrentUser.Builder> {

    public GetCurrentUser() {
        this(builder());
    }

    public GetCurrentUser(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetCurrentUser.Builder, ?> requestBuilder) {
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

    public static class Builder extends RequestBase.Builder<Builder, GetCurrentUser> {
        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
        }

        @Override
        public GetCurrentUser build() {
            return new GetCurrentUser(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
