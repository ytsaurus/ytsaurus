package tech.ytsaurus.client.request;


import tech.ytsaurus.client.operations.VanillaSpec;
public class VanillaOperation extends BaseOperation<VanillaSpec> {
    VanillaOperation(Builder builder) {
        super(builder);
    }

    public Builder toBuilder() {
        return builder()
                .setSpec(getSpec())
                .setMutatingOptions(getMutatingOptions())
                .setTransactionalOptions(getTransactionalOptions().orElse(null));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BuilderBase<Builder, VanillaSpec> {
        public VanillaOperation build() {
            return new VanillaOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
