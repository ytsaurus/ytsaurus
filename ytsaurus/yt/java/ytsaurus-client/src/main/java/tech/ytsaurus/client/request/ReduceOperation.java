package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.ReduceSpec;

/**
 * Immutable reduce operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startReduce(ReduceOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce">
 * reduce documentation
 * </a>
 */
public class ReduceOperation extends BaseOperation<ReduceSpec> {
    ReduceOperation(Builder builder) {
        super(builder);
    }

    /**
     * Construct a builder with options set from this request.
     */
    public Builder toBuilder() {
        return builder()
                .setSpec(getSpec())
                .setMutatingOptions(getMutatingOptions())
                .setTransactionalOptions(getTransactionalOptions().orElse(null));
    }

    /**
     * Create empty builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link ReduceOperation}.
     */
    public static class Builder extends BuilderBase<Builder, ReduceSpec> {
        /**
         * Create instance of {@link ReduceOperation}.
         */
        public ReduceOperation build() {
            return new ReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
