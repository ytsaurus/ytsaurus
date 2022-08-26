package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.MapSpec;

@NonNullApi
@NonNullFields
public class MapOperation {
    private final MapSpec spec;

    @Nullable
    private final TransactionalOptions transactionalOptions;
    private final MutatingOptions mutatingOptions;

    MapOperation(Builder builder) {
        if (builder.spec == null) {
            throw new IllegalStateException("Spec wasn't set");
        }
        this.spec = builder.spec;
        this.transactionalOptions = builder.transactionalOptions;
        this.mutatingOptions = builder.mutatingOptions;
    }

    public Builder toBuilder() {
        return builder()
                .setSpec(spec)
                .setMutatingOptions(mutatingOptions)
                .setTransactionalOptions(transactionalOptions);
    }

    public MapSpec getSpec() {
        return spec;
    }

    public @Nullable TransactionalOptions getTransactionalOptions() {
        return transactionalOptions;
    }

    public MutatingOptions getMutatingOptions() {
        return mutatingOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        private MapSpec spec;
        private MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());
        @Nullable
        private TransactionalOptions transactionalOptions;

        public Builder setSpec(MapSpec spec) {
            this.spec = spec;
            return this;
        }

        public Builder setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return this;
        }

        public Builder setTransactionalOptions(TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return this;
        }

        public MapOperation build() {
            return new MapOperation(this);
        }
    }
}
