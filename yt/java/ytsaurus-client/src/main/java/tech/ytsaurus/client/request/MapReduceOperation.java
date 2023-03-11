package tech.ytsaurus.client.request;

import tech.ytsaurus.client.operations.MapReduceSpec;

/**
 * Immutable map-reduce operation request.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startMapReduce(MapReduceOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/mapreduce">
 * map_reduce documentation
 * </a>
 */
public class MapReduceOperation extends BaseOperation<MapReduceSpec> {
    MapReduceOperation(Builder builder) {
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
     * Builder of {@link MapReduceOperation}.
     */
    public static class Builder extends BuilderBase<Builder, MapReduceSpec> {
        /**
         * Create instance of {@link MapReduceOperation}.
         */
        public MapReduceOperation build() {
            return new MapReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
