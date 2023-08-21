package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.SortSpec;

/**
 * Immutable sort operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startSort(SortOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/sort">
 * sort documentation
 * </a>
 */
public class SortOperation extends BaseOperation<SortSpec> {
    SortOperation(Builder builder) {
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
     * Builder of {@link SortOperation}.
     */
    public static class Builder extends BuilderBase<Builder, SortSpec> {
        /**
         * Create instance of {@link SortOperation}.
         */
        public SortOperation build() {
            return new SortOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
