package ru.yandex.yt.ytclient.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;

public abstract class TransactionalRequest<T extends RequestBase.Builder<T>> extends RequestBase<T> {
    @Nullable
    protected TransactionalOptions transactionalOptions;
    @Nullable
    protected PrerequisiteOptions prerequisiteOptions;

    TransactionalRequest(Builder<?> builder) {
        super(builder);
        this.transactionalOptions = builder.transactionalOptions;
        this.prerequisiteOptions = builder.prerequisiteOptions;
    }

    protected TransactionalRequest(TransactionalRequest<?> other) {
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

    public abstract static class Builder<T extends Builder<T>> extends RequestBase.Builder<T> {
        @Nullable
        protected TransactionalOptions transactionalOptions;
        @Nullable
        protected PrerequisiteOptions prerequisiteOptions;

        public Builder() {
        }

        protected Builder(Builder<?> other) {
            super(other);
            if (other.transactionalOptions != null) {
                transactionalOptions = new TransactionalOptions(other.transactionalOptions);
            }
            if (other.prerequisiteOptions != null) {
                prerequisiteOptions = new PrerequisiteOptions(other.prerequisiteOptions);
            }
        }

        public T setTransactionalOptionsOfTransactionId(GUID transactionId) {
            this.transactionalOptions = new TransactionalOptions(transactionId);
            return self();
        }

        public T setTransactionalOptions(@Nullable TransactionalOptions to) {
            this.transactionalOptions = to;
            return self();
        }

        public T setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
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
