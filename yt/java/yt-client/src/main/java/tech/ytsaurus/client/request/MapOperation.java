package tech.ytsaurus.client.request;


import tech.ytsaurus.client.operations.MapSpec;
public class MapOperation extends BaseOperation<MapSpec> {
    MapOperation(Builder builder) {
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

    public static class Builder extends BuilderBase<Builder, MapSpec> {
        public MapOperation build() {
            return new MapOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
