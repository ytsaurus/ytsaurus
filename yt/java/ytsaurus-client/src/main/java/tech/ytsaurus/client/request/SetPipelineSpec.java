package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqSetPipelineSpec;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable Flow set pipeline spec request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#setPipelineSpec(SetPipelineSpec)
 */
public class SetPipelineSpec
        extends RequestBase<SetPipelineSpec.Builder, SetPipelineSpec>
        implements HighLevelRequest<TReqSetPipelineSpec.Builder> {

    private final YPath pipelinePath;
    @Nullable
    private final Long expectedVersion;
    private final YTreeNode spec;
    private final boolean force;

    public SetPipelineSpec(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
        this.expectedVersion = builder.expectedVersion;
        this.spec = Objects.requireNonNull(builder.spec);
        this.force = builder.force;
    }

    /**
     * Construct empty builder for set pipeline spec request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSetPipelineSpec.Builder, ?> builder) {
        builder.body()
                .setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()))
                .setForce(force);
        ByteString.Output specOutput = ByteString.newOutput();
        YTreeBinarySerializer.serialize(spec, specOutput);
        builder.body().setSpec(specOutput.toByteString());
        if (expectedVersion != null) {
            builder.body().setExpectedVersion(expectedVersion);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        if (expectedVersion != null) {
            sb.append("expectedVersion: ").append(expectedVersion).append(";");
        }
        sb.append("force: ").append(force).append(";");
        sb.append("spec: ").append(spec).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder()
                .setPipelinePath(pipelinePath)
                .setExpectedVersion(expectedVersion)
                .setSpec(spec)
                .setForce(force)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }


    public static class Builder extends RequestBase.Builder<Builder, SetPipelineSpec> {
        @Nullable
        private YPath pipelinePath;
        @Nullable
        private Long expectedVersion;
        @Nullable
        private YTreeNode spec;
        private boolean force;

        Builder() {
        }

        /**
         * Sets expected pipeline version.
         * The request will fail if the expectedVersion is provided and does not equal the received version.
         * <p>
         *
         * @param expectedVersion Expected pipeline version.
         * @return self
         */
        public Builder setExpectedVersion(@Nullable Long expectedVersion) {
            this.expectedVersion = expectedVersion;
            return self();
        }

        /**
         * Sets Flow pipeline spec.
         *
         * @param spec Pipeline spec.
         * @return self
         */
        public Builder setSpec(YTreeNode spec) {
            this.spec = spec;
            return self();
        }

        /**
         * Sets force flag.
         * <p>
         *
         * @param force Flag.
         * @return self
         */
        public Builder setForce(boolean force) {
            this.force = force;
            return self();
        }

        /**
         * Sets the path to the Flow pipeline to be operated on.
         * <p>
         *
         * @param pipelinePath Path for the pipeline.
         * @return self
         */
        public Builder setPipelinePath(YPath pipelinePath) {
            this.pipelinePath = pipelinePath;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Construct {@link SetPipelineSpec} instance.
         */
        @Override
        public SetPipelineSpec build() {
            return new SetPipelineSpec(this);
        }
    }

}
