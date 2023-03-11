package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.MergeSpec;

/**
 * Immutable merge operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startMerge(MergeOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/merge">
 * merge documentation
 * </a>
 */
public class MergeOperation extends BaseOperation<MergeSpec> {
    MergeOperation(Builder builder) {
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
     * Builder of {@link MergeOperation}.
     */
    public static class Builder extends BuilderBase<Builder, MergeSpec> {
        /**
         * Create instance of {@link MergeOperation}.
         */
        public MergeOperation build() {
            return new MergeOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
