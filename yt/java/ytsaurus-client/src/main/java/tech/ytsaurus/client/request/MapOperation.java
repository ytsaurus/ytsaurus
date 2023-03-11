package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.MapSpec;

/**
 * Immutable map operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startMap(MapOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/map">
 * map documentation
 * </a>
 */
public class MapOperation extends BaseOperation<MapSpec> {
    MapOperation(Builder builder) {
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
     * Builder of {@link MapOperation}.
     */
    public static class Builder extends BuilderBase<Builder, MapSpec> {
        /**
         * Create instance of {@link MapOperation}.
         */
        public MapOperation build() {
            return new MapOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
