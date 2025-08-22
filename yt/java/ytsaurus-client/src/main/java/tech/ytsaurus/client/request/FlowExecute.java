package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqFlowExecute;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable Flow execute request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#flowExecute(FlowExecute)
 */
public class FlowExecute
        extends RequestBase<FlowExecute.Builder, FlowExecute>
        implements HighLevelRequest<TReqFlowExecute.Builder> {
    private final YPath pipelinePath;
    @Nullable
    private final String command;
    @Nullable
    private final YTreeNode argument;

    public FlowExecute(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
        this.command = builder.command;
        this.argument = builder.argument;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqFlowExecute.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
        if (command != null) {
            builder.body().setCommand(command);
        }
        if (argument != null) {
            ByteString.Output argumentOutput = ByteString.newOutput();
            YTreeBinarySerializer.serialize(argument, argumentOutput);
            builder.body().setArgument(argumentOutput.toByteString());
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        if (command != null) {
            sb.append("command: ").append(command).append(";");
        }
        if (argument != null) {
            sb.append("argument: ").append(argument).append(";");
        }
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct empty builder for Flow execute request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder()
                .setPipelinePath(pipelinePath)
                .setCommand(command)
                .setArgument(argument)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, FlowExecute> {
        @Nullable
        private YPath pipelinePath;
        @Nullable
        private String command;
        @Nullable
        private YTreeNode argument;

        Builder() {
        }

        /**
         * Sets the path to the Flow pipeline to be operated on.
         *
         * @param pipelinePath Path for the pipeline.
         * @return self
         */
        public Builder setPipelinePath(YPath pipelinePath) {
            this.pipelinePath = pipelinePath;
            return self();
        }

        /**
         * Sets the command to be executed.
         *
         * @param command Command to execute.
         * @return self
         */
        public Builder setCommand(@Nullable String command) {
            this.command = command;
            return self();
        }

        /**
         * Sets the argument for the command.
         *
         * @param argument Argument for the command.
         * @return self
         */
        public Builder setArgument(@Nullable YTreeNode argument) {
            this.argument = argument;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Construct {@link FlowExecute} instance.
         */
        @Override
        public FlowExecute build() {
            return new FlowExecute(this);
        }
    }
}
