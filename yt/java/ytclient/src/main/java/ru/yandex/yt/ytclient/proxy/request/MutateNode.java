package ru.yandex.yt.ytclient.proxy.request;

public abstract class MutateNode<T extends MutateNode> extends RequestBase<T> {
    protected TransactionalOptions transactionalOptions;
    protected PrerequisiteOptions prerequisiteOptions;
    protected MutatingOptions mutatingOptions;

    public T setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return (T)this;
    }

    public T setPrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        return (T)this;
    }

    public T setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return (T)this;
    }
}
