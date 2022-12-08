package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.operations.Spec;
import tech.ytsaurus.core.GUID;


public class BaseOperation<TSpec extends Spec> {
    private final TSpec spec;

    @Nullable
    private final TransactionalOptions transactionalOptions;
    private final MutatingOptions mutatingOptions;

    BaseOperation(BuilderBase<?, TSpec> builder) {
        if (builder.spec == null) {
            throw new IllegalStateException("Spec wasn't set");
        }
        this.spec = builder.spec;
        this.transactionalOptions = builder.transactionalOptions;
        this.mutatingOptions = builder.mutatingOptions;
    }

    public TSpec getSpec() {
        return spec;
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }

    public MutatingOptions getMutatingOptions() {
        return mutatingOptions;
    }

    public abstract static class BuilderBase<T extends BuilderBase<T, TSpec>, TSpec extends Spec> {
        @Nullable
        private TSpec spec;
        private MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());
        @Nullable
        private TransactionalOptions transactionalOptions;

        public T setSpec(TSpec spec) {
            this.spec = spec;
            return self();
        }

        public T setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return self();
        }

        public T setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        protected abstract T self();
    }
}
