package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;

public abstract class MutatePath<
        TBuilder extends MutatePath.Builder<TBuilder, TRequest>,
        TRequest extends MutatePath<TBuilder, TRequest>> extends MutateNode<TBuilder, TRequest> {
    protected final YPath path;

    MutatePath(Builder<?, ?> other) {
        super(other);
        path = Objects.requireNonNull(other.path);
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("path").apply(path::toTree);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends MutatePath<?, TRequest>>
            extends MutateNode.Builder<TBuilder, TRequest> {
        @Nullable
        protected YPath path;

        protected Builder() {
        }

        protected Builder(Builder<?, ?> builder) {
            super(builder);
            path = builder.path;
        }

        public TBuilder setPath(YPath path) {
            this.path = path.justPath();
            return self();
        }

        public YPath getPath() {
            return Objects.requireNonNull(path);
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            return builder
                    .apply(super::toTree)
                    .key("path").apply(Objects.requireNonNull(path)::toTree);
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            sb.append("Path: ").append(path).append("; ");
            super.writeArgumentsLogString(sb);
        }
    }
}
