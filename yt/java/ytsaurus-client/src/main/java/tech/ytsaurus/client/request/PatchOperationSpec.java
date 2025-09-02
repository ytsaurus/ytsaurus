package tech.ytsaurus.client.request;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import NYT.NScheduler.NProto.SpecPatch.TSpecPatch;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqPatchOperationSpec;
import tech.ytsaurus.yson.YsonTextWriter;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

public class PatchOperationSpec
        extends OperationReq<PatchOperationSpec.Builder, PatchOperationSpec>
        implements HighLevelRequest<TReqPatchOperationSpec.Builder> {
    @Nullable
    private final List<SpecPatch>  patches;

    public PatchOperationSpec(BuilderBase<?> builder) {
        super(builder);
        this.patches = builder.patches;
    }

    public PatchOperationSpec(GUID guid) {
        this(builder().setOperationId(guid));
    }

    PatchOperationSpec(String alias) {
        this(builder().setOperationAlias(alias));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static PatchOperationSpec fromAlias(String alias) {
        return new PatchOperationSpec(alias);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPatchOperationSpec.Builder, ?> requestBuilder) {
        TReqPatchOperationSpec.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        assert patches != null;
        for (SpecPatch patch : patches) {
            messageBuilder.addPatches(patch.toProto());
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        if (patches != null) {
            try (YsonTextWriter writer = new YsonTextWriter(sb)) {
                sb.append("Patches: ");
                YTreeNodeUtils.walk(toBuilder().toTreePatchesOnly(), writer, false, true);
                sb.append("; ");
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PatchOperationSpec(");
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

        if (patches != null) {
            builder.setPatches(patches);
        }
        return builder;
    }

    public static class SpecPatch {
        @Nullable
        private YPath path;
        @Nullable
        private YTreeNode value;

        public SpecPatch() {
        }

        public SpecPatch(@Nullable YPath path, @Nullable YTreeNode value) {
            this.path = path;
            this.value = value;
        }

        public SpecPatch setPath(YPath path) {
            this.path = path;
            return this;
        }

        public SpecPatch setValue(YTreeNode value) {
            this.value = value;
            return this;
        }

        private YTreeNode toYTreeNode() {
            YTreeBuilder b = YTree.mapBuilder();
            if (path != null) {
                b.key("path").value(path.toString());
            }
            if (value != null) {
                b.key("value").value(value);
            }
            return b.buildMap();
        }

        private NYT.NScheduler.NProto.SpecPatch.TSpecPatch toProto() {
            TSpecPatch.Builder builder = TSpecPatch.newBuilder();
            if (path != null) {
                builder.setPath(path.toString());
            }
            if (value != null) {
                builder.setValue(value.toString());
            }
            return builder.build();
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
            extends OperationReq.Builder<TBuilder, PatchOperationSpec> {
        @Nullable
        private List<SpecPatch> patches;

        public TBuilder setPatches(List<SpecPatch> patches) {
            this.patches = patches;
            return self();
        }

        public TBuilder setPatches(SpecPatch... patches) {
            return setPatches(Arrays.asList(patches));
        }

        public TBuilder addPatch(YPath path, YTreeNode value) {
            return addPatch(new SpecPatch(path, value));
        }

        public TBuilder addPatch(SpecPatch patch) {
            ensurePatchesList();
            this.patches.add(patch);
            return self();
        }

        private void ensurePatchesList() {
            if (this.patches == null) {
                this.patches = new java.util.ArrayList<>();
            }
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            builder = super.toTree(builder);
            builder = toTreePatchesOnly(builder);
            return builder;
        }

        YTreeBuilder toTreePatchesOnly(YTreeBuilder builder) {
            if (patches != null) {
                YTreeBuilder lb = YTree.listBuilder();
                for (SpecPatch patch : patches) {
                    lb.value(patch.toYTreeNode());
                }
                builder.key("patches").value(lb.buildList());
            }
            return builder;
        }

        public YTreeMapNode toTreePatchesOnly() {
            return toTreePatchesOnly(YTree.mapBuilder()).buildMap();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            YTreeMapNode ysonPatches = toTreePatchesOnly(YTree.mapBuilder()).buildMap();
            try (YsonTextWriter writer = new YsonTextWriter(sb)) {
                sb.append("Patches: ");
                YTreeNodeUtils.walk(ysonPatches, writer, false, true);
                sb.append("; ");
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("PatchOperationSpec(");
            writeArgumentsLogString(sb);
            sb.append(")");
            return sb.toString();
        }

        @Override
        public PatchOperationSpec build() {
            return new PatchOperationSpec(this);
        }
    }
}
