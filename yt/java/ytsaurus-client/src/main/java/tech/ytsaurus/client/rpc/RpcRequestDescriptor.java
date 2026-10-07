package tech.ytsaurus.client.rpc;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;

/**
 * Descriptor of an RPC request used for traffic accounting.
 */
public final class RpcRequestDescriptor {
    private final String service;
    private final String method;
    private final GUID requestId;
    private final GUID originalRequestId;
    private final boolean isStream;
    private final @Nullable String rpcProxyAddress;

    private RpcRequestDescriptor(Builder builder) {
        this.service = builder.service;
        this.method = builder.method;
        this.requestId = builder.requestId;
        this.originalRequestId = builder.originalRequestId == null
                ? builder.requestId
                : builder.originalRequestId;
        this.isStream = builder.isStream;
        this.rpcProxyAddress = builder.rpcProxyAddress;
    }

    /**
     * Service name of the RPC method.
     */
    public String getService() {
        return service;
    }

    /**
     * Method name of the RPC request.
     */
    public String getMethod() {
        return method;
    }

    /**
     * Request id of the RPC request.
     */
    public GUID getRequestId() {
        return requestId;
    }

    /**
     * Id of the original logical request. It is preserved across retry and failover attempts.
     */
    public GUID getOriginalRequestId() {
        return originalRequestId;
    }

    /**
     * True if this accounting event belongs to streaming payload; false for regular RPC request.
     */
    public boolean isStream() {
        return isStream;
    }

    /**
     * Address of the RPC proxy handling this request.
     */
    @Nullable
    public String getRpcProxyAddress() {
        return rpcProxyAddress;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String service;
        private String method;
        private GUID requestId;
        private GUID originalRequestId;
        private boolean isStream;
        private @Nullable String rpcProxyAddress;

        /**
         * Set service name.
         */
        public Builder setService(String service) {
            this.service = service;
            return this;
        }

        /**
         * Set method name.
         */
        public Builder setMethod(String method) {
            this.method = method;
            return this;
        }

        /**
         * Set stringified request id (GUID).
         */
        public Builder setRequestId(GUID requestId) {
            this.requestId = requestId;
            return this;
        }

        /**
         * Set id of the original logical request.
         */
        public Builder setOriginalRequestId(GUID originalRequestId) {
            this.originalRequestId = originalRequestId;
            return this;
        }

        /**
         * Mark events produced by streaming traffic.
         */
        public Builder setIsStream(boolean isStream) {
            this.isStream = isStream;
            return this;
        }

        /**
         * Set address of the RPC proxy handling this request.
         */
        public Builder setRpcProxyAddress(@Nullable String rpcProxyAddress) {
            this.rpcProxyAddress = rpcProxyAddress;
            return this;
        }

        public RpcRequestDescriptor build() {
            return new RpcRequestDescriptor(this);
        }
    }
}
