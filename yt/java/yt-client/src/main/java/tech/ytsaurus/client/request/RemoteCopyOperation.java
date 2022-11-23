package tech.ytsaurus.client.request;


import tech.ytsaurus.client.operations.RemoteCopySpec;
public class RemoteCopyOperation extends BaseOperation<RemoteCopySpec> {
    RemoteCopyOperation(Builder builder) {
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

    public static class Builder extends BuilderBase<Builder, RemoteCopySpec> {
        public RemoteCopyOperation build() {
            return new RemoteCopyOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
