package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

public abstract class MutateNode<T extends MutateNode> extends RequestBase<T> {
    protected TransactionalOptions transactionalOptions;
    protected PrerequisiteOptions prerequisiteOptions;
    protected MutatingOptions mutatingOptions;

    protected MutateNode() {
    }

    protected MutateNode(MutateNode<?> mutateNode) {
        transactionalOptions = new TransactionalOptions(mutateNode.transactionalOptions);
        prerequisiteOptions = new PrerequisiteOptions(mutateNode.prerequisiteOptions);
        mutatingOptions = new MutatingOptions(mutateNode.mutatingOptions);
    }

    public T setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return (T)this;
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }

    public T setPrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        return (T)this;
    }

    public Optional<PrerequisiteOptions> getPrerequisiteOptions() {
        return Optional.ofNullable(prerequisiteOptions);
    }

    public T setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return (T)this;
    }

    public Optional<MutatingOptions> getMutatingOptions() {
        return Optional.ofNullable(mutatingOptions);
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            throw new IllegalStateException("prerequisite options are not supported yet");
        }
        if (mutatingOptions != null) {
            throw new IllegalStateException("mutating options are not supported");
        }
        return builder
                .apply(transactionalOptions::toTree)
                ;
    }
}
