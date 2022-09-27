package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yson.YsonTextWriter;
import ru.yandex.yt.rpcproxy.TReqUpdateOperationParameters;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class UpdateOperationParameters extends OperationReq<UpdateOperationParameters>
        implements HighLevelRequest<TReqUpdateOperationParameters.Builder> {
    @Nullable private List<String> owners;
    @Nullable private String pool;
    @Nullable private Map<String, SchedulingOptions> schedulingOptionsPerPoolTree;
    @Nullable private Double weight;

    public UpdateOperationParameters(GUID guid) {
        super(guid, null);
    }

    UpdateOperationParameters(String alias) {
        super(null, alias);
    }

    public static UpdateOperationParameters fromAlias(String alias) {
        return new UpdateOperationParameters(alias);
    }

    public UpdateOperationParameters setOwners(List<String> owners) {
        this.owners = owners;
        return this;
    }

    public UpdateOperationParameters setOwners(String... owners) {
        return setOwners(Arrays.asList(owners));
    }

    public UpdateOperationParameters setPool(String pool) {
        this.pool = pool;
        return this;
    }

    public UpdateOperationParameters setSchedulingOptionsPerPoolTree(
            Map<String, SchedulingOptions> schedulingOptionsPerPoolTree) {
        this.schedulingOptionsPerPoolTree = schedulingOptionsPerPoolTree;
        return this;
    }

    public UpdateOperationParameters addSchedulingOptions(String pool, SchedulingOptions schedulingOptions) {
        if (schedulingOptionsPerPoolTree == null) {
            schedulingOptionsPerPoolTree = new java.util.HashMap<>();
        }
        schedulingOptionsPerPoolTree.put(pool, schedulingOptions);
        return this;
    }

    public UpdateOperationParameters setWeight(double weight) {
        this.weight = weight;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUpdateOperationParameters.Builder, ?> requestBuilder) {
        TReqUpdateOperationParameters.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        YTreeNode parameters = toTreeParametersOnly(YTree.mapBuilder()).buildMap();
        messageBuilder.setParameters(ByteString.copyFrom(parameters.toBinary()));
    }

    @Nonnull
    @Override
    protected UpdateOperationParameters self() {
        return this;
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
        return builder;
    }

    YTreeMapNode toTreeParametersOnly() {
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
        throw new RuntimeException("unimplemented build() method");
    }

    @NonNullFields
    @NonNullApi
    public static class SchedulingOptions {
        @Nullable private Double weight;
        @Nullable private ResourceLimits resourceLimits;

        public SchedulingOptions() { }

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

    @NonNullFields
    @NonNullApi
    public static class ResourceLimits {
        @Nullable private Long userSlots;
        @Nullable private Double cpu;
        @Nullable private Long network;
        @Nullable private Long memory;

        public ResourceLimits() { }

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
}
