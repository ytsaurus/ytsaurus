package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class MutateNode<T extends MutateNode<T>> extends RequestBase<T> {
    protected @Nullable TransactionalOptions transactionalOptions;
    protected @Nullable PrerequisiteOptions prerequisiteOptions;
    protected @Nullable MutatingOptions mutatingOptions;

    protected MutateNode() {
    }

    protected MutateNode(MutateNode<?> mutateNode) {
        if (mutateNode.transactionalOptions != null) {
            transactionalOptions = new TransactionalOptions(mutateNode.transactionalOptions);
        }
        if (mutateNode.prerequisiteOptions != null) {
            prerequisiteOptions = new PrerequisiteOptions(mutateNode.prerequisiteOptions);
        }
        if (mutateNode.mutatingOptions != null) {
            mutatingOptions = new MutatingOptions(mutateNode.mutatingOptions);
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

    public T setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return self();
    }

    public Optional<MutatingOptions> getMutatingOptions() {
        return Optional.ofNullable(mutatingOptions);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (transactionalOptions != null) {
            sb.append("TransactionId: ").append(transactionalOptions.getTransactionId()).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            builder = builder.apply(prerequisiteOptions::toTree);
        }
        if (mutatingOptions != null) {
            throw new IllegalStateException("mutating options are not supported");
        }
        if (transactionalOptions != null) {
            builder = transactionalOptions.toTree(builder);
        }
        return builder;
    }
}
