package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.RemoteCopySpec;

/**
 * Immutable remote_copy operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startRemoteCopy(RemoteCopyOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/remote-copy">
 * remote_copy documentation
 * </a>
 */
public class RemoteCopyOperation extends BaseOperation<RemoteCopySpec> {
    RemoteCopyOperation(Builder builder) {
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
     * Builder of {@link RemoteCopyOperation}.
     */
    public static class Builder extends BuilderBase<Builder, RemoteCopySpec> {
        /**
         * Create instance of {@link RemoteCopyOperation}.
         */
        public RemoteCopyOperation build() {
            return new RemoteCopyOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
