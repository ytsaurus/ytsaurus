package tech.ytsaurus.client.discovery;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.TReqHeartbeat;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.request.RequestBase;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;

public class Heartbeat extends RequestBase<Heartbeat.Builder, Heartbeat>
        implements HighLevelRequest<TReqHeartbeat.Builder> {
    private final String groupId;

    private final MemberInfo memberInfo;

    private final long leaseTimeout;

    Heartbeat(Builder builder) {
        super(builder);
        this.groupId = Objects.requireNonNull(builder.groupId);
        this.memberInfo = Objects.requireNonNull(builder.memberInfo);
        this.leaseTimeout = builder.leaseTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setGroupId(groupId)
                .setMemberInfo(memberInfo)
                .setLeaseTimeout(leaseTimeout)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqHeartbeat.Builder, ?> requestBuilder) {
        TReqHeartbeat.Builder builder = requestBuilder.body();
        builder.setGroupId(groupId);
        builder.setMemberInfo(memberInfo.toProto());
        builder.setLeaseTimeout(leaseTimeout);
    }

    public static class Builder extends RequestBase.Builder<Heartbeat.Builder, Heartbeat> {
        @Nullable
        private String groupId;

        @Nullable
        private MemberInfo memberInfo;

        private long leaseTimeout;

        private Builder() {
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return self();
        }

        public Builder setMemberInfo(MemberInfo memberInfo) {
            this.memberInfo = memberInfo;
            return self();
        }

        public Builder setLeaseTimeout(long leaseTimeout) {
            this.leaseTimeout = leaseTimeout;
            return self();
        }

        @Override
        public Heartbeat build() {
            return new Heartbeat(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}

