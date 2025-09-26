package tech.ytsaurus.client.rpc;

import tech.ytsaurus.core.GUID;

/**
 * Descriptor of an RPC request used for traffic accounting.
 */
public final class RpcRequestDescriptor {
    private final String service;
    private final String method;
    private final GUID requestId;
    private final boolean isStream;

    private RpcRequestDescriptor(Builder builder) {
        this.service = builder.service;
        this.method = builder.method;
        this.requestId = builder.requestId;
        this.isStream = builder.isStream;
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
     * True if this accounting event belongs to streaming payload; false for regular RPC request.
     */
    public boolean isStream() {
        return isStream;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String service;
        private String method;
        private GUID requestId;
        private boolean isStream;

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
         * Mark events produced by streaming traffic.
         */
        public Builder setIsStream(boolean isStream) {
            this.isStream = isStream;
            return this;
        }

        public RpcRequestDescriptor build() {
            return new RpcRequestDescriptor(this);
        }
    }
}


