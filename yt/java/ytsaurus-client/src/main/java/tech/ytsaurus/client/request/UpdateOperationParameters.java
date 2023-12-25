package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqUpdateOperationParameters;
import tech.ytsaurus.yson.YsonTextWriter;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

public class UpdateOperationParameters
        extends OperationReq<UpdateOperationParameters.Builder, UpdateOperationParameters>
        implements HighLevelRequest<TReqUpdateOperationParameters.Builder> {
    @Nullable
    private final List<String> owners;
    @Nullable
    private final String pool;
    @Nullable
    private final Map<String, SchedulingOptions> schedulingOptionsPerPoolTree;
    @Nullable
    private final Double weight;
    @Nullable
    private final YTreeNode annotations;

    public UpdateOperationParameters(BuilderBase<?> builder) {
        super(builder);
        if (builder.owners != null) {
            this.owners = new ArrayList<>(builder.owners);
        } else {
            this.owners = null;
        }
        this.pool = builder.pool;
        if (builder.schedulingOptionsPerPoolTree != null) {
            this.schedulingOptionsPerPoolTree = new HashMap<>(builder.schedulingOptionsPerPoolTree);
        } else {
            this.schedulingOptionsPerPoolTree = null;
        }
        this.weight = builder.weight;
        this.annotations = builder.annotations;
    }

    public UpdateOperationParameters(GUID guid) {
        this(builder().setOperationId(guid));
    }

    UpdateOperationParameters(String alias) {
        this(builder().setOperationAlias(alias));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UpdateOperationParameters fromAlias(String alias) {
        return new UpdateOperationParameters(alias);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUpdateOperationParameters.Builder, ?> requestBuilder) {
        TReqUpdateOperationParameters.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        YTreeNode parameters = toTreeParametersOnly(YTree.mapBuilder()).buildMap();
        messageBuilder.setParameters(ByteString.copyFrom(parameters.toBinary()));
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        builder = super.toTree(builder);
        builder = toTreeParametersOnly(builder.key("parameters").beginMap()).endMap();
        return builder;
    }

    YTreeBuilder toTreeParametersOnly(YTreeBuilder builder) {
        if (owners != null) {
            YTreeBuilder lb = YTree.listBuilder();
            for (String owner : owners) {
                lb.value(owner);
            }
            builder.key("owners").value(lb.buildList());
        }
        if (pool != null) {
            builder.key("pool").value(pool);
        }
        if (schedulingOptionsPerPoolTree != null) {
            YTreeBuilder mb = YTree.mapBuilder();
            for (Map.Entry<String, SchedulingOptions> entry : schedulingOptionsPerPoolTree.entrySet()) {
                mb.key(entry.getKey()).value(entry.getValue().toYTreeNode());
            }
            builder.key("scheduling_options_per_pool_tree").value(mb.buildMap());
        }
        if (weight != null) {
            builder.key("weight").value(weight);
        }
        if (annotations != null) {
            builder.key("annotations").value(annotations);
        }
        return builder;
    }

    public YTreeMapNode toTreeParametersOnly() {
        return toTreeParametersOnly(YTree.mapBuilder()).buildMap();
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        YTreeMapNode parameters = toTreeParametersOnly(YTree.mapBuilder()).buildMap();
        try (YsonTextWriter writer = new YsonTextWriter(sb)) {
            sb.append("Parameters: ");
            YTreeNodeUtils.walk(parameters, writer, false, true);
            sb.append("; ");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UpdateOperationParameters(");
        writeArgumentsLogString(sb);
        sb.append(")");
        return sb.toString();
    }

    public Builder toBuilder() {
        Builder builder = builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);

        if (owners != null) {
            builder.setOwners(owners);
        }
        if (schedulingOptionsPerPoolTree != null) {
            builder.setSchedulingOptionsPerPoolTree(schedulingOptionsPerPoolTree);
        }
        if (weight != null) {
            builder.setWeight(weight);
        }
        if (pool != null) {
            builder.setPool(pool);
        }
        if (annotations != null) {
            builder.setAnnotations(annotations);
        }
        return builder;
    }

    public static class SchedulingOptions {
        @Nullable
        private Double weight;
        @Nullable
        private ResourceLimits resourceLimits;

        public SchedulingOptions() {
        }

        public SchedulingOptions(SchedulingOptions other) {
            this.weight = other.weight;
            if (other.resourceLimits != null) {
                this.resourceLimits = new ResourceLimits(other.resourceLimits);
            } else {
                this.resourceLimits = null;
            }
        }

        public SchedulingOptions(@Nullable Double weight, @Nullable ResourceLimits resourceLimits) {
            this.weight = weight;
            this.resourceLimits = resourceLimits;
        }

        public SchedulingOptions setWeight(double weight) {
            this.weight = weight;
            return this;
        }

        public SchedulingOptions setResourceLimits(ResourceLimits resourceLimits) {
            this.resourceLimits = resourceLimits;
            return this;
        }

        private YTreeNode toYTreeNode() {
            YTreeBuilder b = YTree.mapBuilder();
            if (weight != null) {
                b.key("weight").value(weight);
            }
            if (resourceLimits != null) {
                b.key("resource_limits").value(resourceLimits.toYTreeNode());
            }
            return b.buildMap();
        }
    }

    public static class ResourceLimits {
        @Nullable
        private Long userSlots;
        @Nullable
        private Double cpu;
        @Nullable
        private Long network;
        @Nullable
        private Long memory;

        public ResourceLimits() {
        }

        public ResourceLimits(ResourceLimits other) {
            this.userSlots = other.userSlots;
            this.cpu = other.cpu;
            this.network = other.network;
            this.memory = other.memory;
        }

        public ResourceLimits(@Nullable Long userSlots, @Nullable Double cpu, @Nullable Long network,
                              @Nullable Long memory) {
            this.userSlots = userSlots;
            this.cpu = cpu;
            this.network = network;
            this.memory = memory;
        }

        public ResourceLimits setUserSlots(long userSlots) {
            this.userSlots = userSlots;
            return this;
        }

        public ResourceLimits setCpu(double cpu) {
            this.cpu = cpu;
            return this;
        }

        public ResourceLimits setNetwork(long network) {
            this.network = network;
            return this;
        }

        public ResourceLimits setMemory(long memory) {
            this.memory = memory;
            return this;
        }

        private YTreeNode toYTreeNode() {
            YTreeBuilder b = YTree.mapBuilder();
            if (userSlots != null) {
                b.key("user_slots").value(userSlots);
            }
            if (cpu != null) {
                b.key("cpu").value(cpu);
            }
            if (network != null) {
                b.key("network").value(network);
            }
            if (memory != null) {
                b.key("memory").value(memory);
            }
            return b.buildMap();
        }
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends OperationReq.Builder<TBuilder, UpdateOperationParameters> {
        @Nullable
        private List<String> owners;
        @Nullable
        private String pool;
        @Nullable
        private Map<String, SchedulingOptions> schedulingOptionsPerPoolTree;
        @Nullable
        private Double weight;
        @Nullable
        private YTreeNode annotations;

        public TBuilder setOwners(List<String> owners) {
            this.owners = owners;
            return self();
        }

        public TBuilder setOwners(String... owners) {
            return setOwners(Arrays.asList(owners));
        }

        public TBuilder setPool(String pool) {
            this.pool = pool;
            return self();
        }

        public TBuilder setSchedulingOptionsPerPoolTree(
                Map<String, SchedulingOptions> schedulingOptionsPerPoolTree) {
            this.schedulingOptionsPerPoolTree = new HashMap<>();
            for (Map.Entry<String, SchedulingOptions> entry : schedulingOptionsPerPoolTree.entrySet()) {
                this.schedulingOptionsPerPoolTree.put(entry.getKey(), new SchedulingOptions(entry.getValue()));
            }
            return self();
        }

        public TBuilder addSchedulingOptions(String pool, SchedulingOptions schedulingOptions) {
            if (schedulingOptionsPerPoolTree == null) {
                schedulingOptionsPerPoolTree = new java.util.HashMap<>();
            }
            schedulingOptionsPerPoolTree.put(pool, new SchedulingOptions(schedulingOptions));
            return self();
        }

        public TBuilder setWeight(double weight) {
            this.weight = weight;
            return self();
        }

        public TBuilder setAnnotations(YTreeNode annotations) {
            this.annotations = annotations;
            return self();
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            builder = super.toTree(builder);
            builder = toTreeParametersOnly(builder.key("parameters").beginMap()).endMap();
            return builder;
        }

        YTreeBuilder toTreeParametersOnly(YTreeBuilder builder) {
            if (owners != null) {
                YTreeBuilder lb = YTree.listBuilder();
                for (String owner : owners) {
                    lb.value(owner);
                }
                builder.key("owners").value(lb.buildList());
            }
            if (pool != null) {
                builder.key("pool").value(pool);
            }
            if (schedulingOptionsPerPoolTree != null) {
                YTreeBuilder mb = YTree.mapBuilder();
                for (Map.Entry<String, SchedulingOptions> entry : schedulingOptionsPerPoolTree.entrySet()) {
                    mb.key(entry.getKey()).value(entry.getValue().toYTreeNode());
                }
                builder.key("scheduling_options_per_pool_tree").value(mb.buildMap());
            }
            if (weight != null) {
                builder.key("weight").value(weight);
            }
            if (annotations != null) {
                builder.key("annotations").value(annotations);
            }
            return builder;
        }

        public YTreeMapNode toTreeParametersOnly() {
            return toTreeParametersOnly(YTree.mapBuilder()).buildMap();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            YTreeMapNode parameters = toTreeParametersOnly(YTree.mapBuilder()).buildMap();
            try (YsonTextWriter writer = new YsonTextWriter(sb)) {
                sb.append("Parameters: ");
                YTreeNodeUtils.walk(parameters, writer, false, true);
                sb.append("; ");
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("UpdateOperationParameters(");
            writeArgumentsLogString(sb);
            sb.append(")");
            return sb.toString();
        }

        @Override
        public UpdateOperationParameters build() {
            return new UpdateOperationParameters(this);
        }
    }
}
