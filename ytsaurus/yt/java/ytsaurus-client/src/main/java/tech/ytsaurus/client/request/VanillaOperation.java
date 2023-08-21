package tech.ytsaurus.client.request;


import tech.ytsaurus.client.operations.VanillaSpec;

/**
 * Immutable vanilla operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startVanilla(VanillaOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla">
 * vanilla documentation
 * </a>
 */
public class VanillaOperation extends BaseOperation<VanillaSpec> {
    VanillaOperation(Builder builder) {
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
     * Builder of {@link VanillaOperation}.
     */
    public static class Builder extends BuilderBase<Builder, VanillaSpec> {
        /**
         * Create instance of {@link VanillaOperation}.
         */
        public VanillaOperation build() {
            return new VanillaOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
