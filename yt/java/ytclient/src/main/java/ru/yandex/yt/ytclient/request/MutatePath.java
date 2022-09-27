package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
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
