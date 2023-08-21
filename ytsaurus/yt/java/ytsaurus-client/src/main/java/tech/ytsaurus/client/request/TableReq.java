package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;


public abstract class TableReq<
        TBuilder extends TableReq.Builder<TBuilder, TRequest>,
        TRequest extends RequestBase<TBuilder, TRequest>>
        extends RequestBase<TBuilder, TRequest> {
    protected final String path;
    protected final MutatingOptions mutatingOptions;
    @Nullable
    protected final TabletRangeOptions tabletRangeOptions;

    TableReq(Builder<?, ?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.mutatingOptions = new MutatingOptions(builder.mutatingOptions);
        this.tabletRangeOptions = builder.tabletRangeOptions;
    }

    public String getPath() {
        return path;
    }

    public <R extends com.google.protobuf.GeneratedMessageV3.Builder<R>>
    R writeTo(R builder) {
        if (tabletRangeOptions != null) {
            builder.setField(
                    builder.getDescriptorForType().findFieldByName("tablet_range_options"),
                    tabletRangeOptions.toProto());
        }

        builder.setField(
                builder.getDescriptorForType().findFieldByName("mutating_options"),
                mutatingOptions.toProto());

        builder.setField(builder.getDescriptorForType().findFieldByName("path"), getPath());

        return builder;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(getPath()).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends RequestBase<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
        @Nullable
        protected String path;
        protected MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());
        @Nullable
        protected TabletRangeOptions tabletRangeOptions;

        Builder() {
        }

        Builder(Builder<?, ?> builder) {
            super(builder);
            this.path = builder.path;
            this.mutatingOptions = new MutatingOptions(builder.mutatingOptions);
            this.tabletRangeOptions = builder.tabletRangeOptions;
        }

        public TBuilder setPath(YPath path) {
            this.path = path.justPath().toString();
            return self();
        }

        public TBuilder setPath(String path) {
            this.path = path;
            return self();
        }

        public TBuilder setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return self();
        }

        public TBuilder setTabletRangeOptions(@Nullable TabletRangeOptions tabletRangeOptions) {
            this.tabletRangeOptions = tabletRangeOptions;
            return self();
        }

        public String getPath() {
            return Objects.requireNonNull(path);
        }

        public <R extends com.google.protobuf.GeneratedMessageV3.Builder<R>>
        R writeTo(R builder) {
            if (tabletRangeOptions != null) {
                builder.setField(
                        builder.getDescriptorForType().findFieldByName("tablet_range_options"),
                        tabletRangeOptions.toProto());
            }

            builder.setField(
                    builder.getDescriptorForType().findFieldByName("mutating_options"),
                    mutatingOptions.toProto());

            builder.setField(builder.getDescriptorForType().findFieldByName("path"), getPath());

            return builder;
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            sb.append("Path: ").append(getPath()).append("; ");
            super.writeArgumentsLogString(sb);
        }
    }
}
