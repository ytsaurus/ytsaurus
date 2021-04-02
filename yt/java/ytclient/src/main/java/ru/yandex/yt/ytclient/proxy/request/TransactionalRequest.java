package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public abstract class TransactionalRequest<T extends TransactionalRequest<T>> extends RequestBase<T> {
    @Nullable protected TransactionalOptions transactionalOptions;
    @Nullable protected PrerequisiteOptions prerequisiteOptions;

    TransactionalRequest() {
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

    public T setTransactionalOptionsOfTransactionId(GUID transactionId) {
        this.transactionalOptions = new TransactionalOptions(transactionId);
        return self();
    }

    public T setTransactionalOptions(@Nullable TransactionalOptions to) {
        this.transactionalOptions = to;
        return self();
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }

    public T setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        return self();
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
}
