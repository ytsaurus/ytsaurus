package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.SortSpec;

public class SortOperation {
    private final SortSpec spec;

    @Nullable
    private final TransactionalOptions transactionalOptions;
    private final MutatingOptions mutatingOptions;

    SortOperation(Builder builder) {
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

    public SortSpec getSpec() {
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
        private SortSpec spec;
        private MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());
        @Nullable
        private TransactionalOptions transactionalOptions;

        public Builder setSpec(SortSpec spec) {
            this.spec = spec;
            return this;
        }

        public Builder setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return this;
        }

        public Builder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return this;
        }

        public SortOperation build() {
            return new SortOperation(this);
        }
    }
}
