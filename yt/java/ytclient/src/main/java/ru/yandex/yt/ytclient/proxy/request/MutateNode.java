package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

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

    public T setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return self();
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }

    public T setPrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
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
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        if (transactionalOptions != null) {
            sb.append("TransactionId: ").append(transactionalOptions.getTransactionId()).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            throw new IllegalStateException("prerequisite options are not supported yet");
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
