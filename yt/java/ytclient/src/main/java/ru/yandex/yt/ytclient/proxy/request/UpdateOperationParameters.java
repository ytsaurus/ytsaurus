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
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yson.YsonTextWriter;
import ru.yandex.yt.rpcproxy.TReqUpdateOperationParameters;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullFields
@NonNullApi
public class UpdateOperationParameters extends RequestBase<UpdateOperationParameters>
        implements HighLevelRequest<TReqUpdateOperationParameters.Builder> {
    private final GUID guid;
    @Nullable private List<String> owners;
    @Nullable private String pool;
    @Nullable private Map<String, SchedulingOptions> schedulingOptionsPerPoolTree;

    public UpdateOperationParameters(GUID guid) {
        this.guid = guid;
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

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUpdateOperationParameters.Builder, ?> requestBuilder) {
        YTreeNode parameters = toYTreeNode();
        requestBuilder.body()
                .setOperationId(RpcUtil.toProto(guid))
                .setParameters(ByteString.copyFrom(parameters.toBinary()));
    }

    @Nonnull
    @Override
    protected UpdateOperationParameters self() {
        return this;
    }

    YTreeNode toYTreeNode() {
        YTreeBuilder b = YTree.mapBuilder();
        if (owners != null) {
            YTreeBuilder lb = YTree.listBuilder();
            for (String owner : owners) {
                lb.value(owner);
            }
            b.key("owners").value(lb.buildList());
        }
        if (pool != null) {
            b.key("pool").value(pool);
        }
        if (schedulingOptionsPerPoolTree != null) {
            YTreeBuilder mb = YTree.mapBuilder();
            for (Map.Entry<String, SchedulingOptions> entry : schedulingOptionsPerPoolTree.entrySet()) {
                mb.key(entry.getKey()).value(entry.getValue().toYTreeNode());
            }
            b.key("scheduling_options_per_pool_tree").value(mb.buildMap());
        }
        return b.buildMap();
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Id: ").append(guid).append("; ");
        try (YsonTextWriter writer = new YsonTextWriter(sb)) {
            sb.append("Parameters: ");
            YTreeNodeUtils.walk(toYTreeNode(), writer, false, true);
            sb.append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UpdateOperationParameters(");
        writeArgumentsLogString(sb);
        sb.append(")");
        return sb.toString();
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
