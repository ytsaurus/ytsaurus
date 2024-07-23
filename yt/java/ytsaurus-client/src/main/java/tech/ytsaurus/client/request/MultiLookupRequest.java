package tech.ytsaurus.client.request;

public class MultiLookupRequest extends AbstractMultiLookupRequest<MultiLookupRequest.Builder, MultiLookupRequest> {

    public MultiLookupRequest(BuilderBase<?> builder) {
        super(builder);
    }

    public MultiLookupRequest() {
        super(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTimestamp(timestamp)
                .setRetentionTimestamp(retentionTimestamp)
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
        public MultiLookupRequest build() {
            return new MultiLookupRequest(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractMultiLookupRequest.Builder<TBuilder, MultiLookupRequest> {
    }
}
