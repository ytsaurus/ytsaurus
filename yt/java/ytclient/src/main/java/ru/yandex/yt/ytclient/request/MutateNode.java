package ru.yandex.yt.ytclient.request;

import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;

@NonNullFields
@NonNullApi
public abstract class MutateNode<T extends RequestBase.Builder<T>> extends TransactionalRequest<T> {
    protected MutatingOptions mutatingOptions;

    MutateNode(Builder<?> builder) {
        super(builder);
        this.mutatingOptions = new MutatingOptions(builder.mutatingOptions);
    }

    public Optional<MutatingOptions> getMutatingOptions() {
        return Optional.of(mutatingOptions);
    }

    @Override
    protected YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            builder = builder.apply(prerequisiteOptions::toTree);
        }
        return super.toTree(builder);
    }

    public abstract static class Builder<T extends Builder<T>> extends TransactionalRequest.Builder<T> {
        protected MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());

        protected Builder() {
        }

        public Builder(Builder<?> builder) {
            super(builder);
            this.mutatingOptions = new MutatingOptions(builder.mutatingOptions);
        }

        public T setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return self();
        }

        public Optional<MutatingOptions> getMutatingOptions() {
            return Optional.of(mutatingOptions);
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            if (prerequisiteOptions != null) {
                builder = builder.apply(prerequisiteOptions::toTree);
            }
            return super.toTree(builder);
        }
    }
}
