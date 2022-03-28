package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class MutateNode<T extends MutateNode<T>> extends TransactionalRequest<T> {
    protected MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());

    protected MutateNode() {
    }

    protected MutateNode(MutateNode<?> other) {
        super(other);
        mutatingOptions = new MutatingOptions(other.mutatingOptions);
    }

    public T setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return self();
    }

    public Optional<MutatingOptions> getMutatingOptions() {
        return Optional.of(mutatingOptions);
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (prerequisiteOptions != null) {
            builder = builder.apply(prerequisiteOptions::toTree);
        }
        return super.toTree(builder);
    }
}
