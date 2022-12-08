package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeBuilder;

public abstract class TransactionalRequest<
        TBuilder extends RequestBase.Builder<TBuilder, TRequest>,
        TRequest extends RequestBase<TBuilder, TRequest>> extends RequestBase<TBuilder, TRequest> {
    @Nullable
    protected TransactionalOptions transactionalOptions;
    @Nullable
    protected PrerequisiteOptions prerequisiteOptions;

    TransactionalRequest(Builder<?, ?> builder) {
        super(builder);
        this.transactionalOptions = builder.transactionalOptions;
        this.prerequisiteOptions = builder.prerequisiteOptions;
    }

    protected TransactionalRequest(TransactionalRequest<?, ?> other) {
        super(other);
        if (other.transactionalOptions != null) {
            transactionalOptions = new TransactionalOptions(other.transactionalOptions);
        }
        if (other.prerequisiteOptions != null) {
            prerequisiteOptions = new PrerequisiteOptions(other.prerequisiteOptions);
        }
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }

    public Optional<PrerequisiteOptions> getPrerequisiteOptions() {
        return Optional.ofNullable(prerequisiteOptions);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (transactionalOptions != null) {
            transactionalOptions.writeArgumentsLogString(sb);
        }
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            builder = prerequisiteOptions.toTree(builder);
        }
        if (transactionalOptions != null) {
            builder = transactionalOptions.toTree(builder);
        }
        return builder;
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends TransactionalRequest<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
        @Nullable
        protected TransactionalOptions transactionalOptions;
        @Nullable
        protected PrerequisiteOptions prerequisiteOptions;

        /**
         * Construct empty builder.
         */
        Builder() {
        }

        protected Builder(Builder<?, ?> other) {
            super(other);
            if (other.transactionalOptions != null) {
                transactionalOptions = new TransactionalOptions(other.transactionalOptions);
            }
            if (other.prerequisiteOptions != null) {
                prerequisiteOptions = new PrerequisiteOptions(other.prerequisiteOptions);
            }
        }

        public TBuilder setTransactionalOptionsOfTransactionId(GUID transactionId) {
            this.transactionalOptions = new TransactionalOptions(transactionId);
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions to) {
            this.transactionalOptions = to;
            return self();
        }

        public TBuilder setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
            this.prerequisiteOptions = prerequisiteOptions;
            return self();
        }

        public Optional<TransactionalOptions> getTransactionalOptions() {
            return Optional.ofNullable(transactionalOptions);
        }

        public Optional<PrerequisiteOptions> getPrerequisiteOptions() {
            return Optional.ofNullable(prerequisiteOptions);
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            if (transactionalOptions != null) {
                transactionalOptions.writeArgumentsLogString(sb);
            }
        }

        protected YTreeBuilder toTree(YTreeBuilder builder) {
            if (prerequisiteOptions != null) {
                builder = prerequisiteOptions.toTree(builder);
            }
            if (transactionalOptions != null) {
                builder = transactionalOptions.toTree(builder);
            }
            return builder;
        }
    }
}
