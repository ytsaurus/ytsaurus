package tech.ytsaurus.flow.computation;

public class SourceComputation extends Computation {

    SourceComputation(Builder<?> builder) {
        super(builder);
    }

    /**
     * @return Builder for Source Computation.
     */
    public static Builder<?> builder() {
        return new Builder<>();
    }

    @Override
    public ComputationType getComputationType() {
        return ComputationType.Source;
    }

    public static class Builder<T extends SourceComputation.Builder<T>> extends Computation.Builder<T> {

        @SuppressWarnings("unchecked")
        @Override
        protected T self() {
            return (T) this;
        }

        @Override
        public SourceComputation build() {
            return new SourceComputation(this);
        }
    }
}
