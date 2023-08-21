package tech.ytsaurus.client.request;

import java.util.Optional;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeBuilder;

public abstract class MutateNode<TBuilder extends MutateNode.Builder<TBuilder, TRequest>,
        TRequest extends MutateNode<TBuilder, TRequest>> extends TransactionalRequest<TBuilder, TRequest> {
    protected MutatingOptions mutatingOptions;

    MutateNode(Builder<?, ?> builder) {
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

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends MutateNode<?, TRequest>>
            extends TransactionalRequest.Builder<TBuilder, TRequest> {
        protected MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());

        protected Builder() {
        }

        public Builder(Builder<?, ?> builder) {
            super(builder);
            this.mutatingOptions = new MutatingOptions(builder.mutatingOptions);
        }

        public TBuilder setMutatingOptions(MutatingOptions mutatingOptions) {
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
