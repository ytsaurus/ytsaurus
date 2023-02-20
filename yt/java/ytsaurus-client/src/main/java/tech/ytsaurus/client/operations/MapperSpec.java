package tech.ytsaurus.client.operations;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Immutable mapper spec.
 */
@NonNullApi
@NonNullFields
public class MapperSpec extends MapperOrReducerSpec {
    /**
     * Construct mapper spec from mapper with other options set to defaults.
     */
    public MapperSpec(Mapper<?, ?> mapper) {
        this(builder().setMapper(mapper));
    }

    protected MapperSpec(Builder builder) {
        super(MapMain.class, builder);
    }

    /**
     * Construct builder for mapper spec with specified mapper.
     */
    public static Builder builder(Mapper<?, ?> mapper) {
        Builder builder = new Builder();
        builder.setMapper(mapper);
        return builder;
    }

    /**
     * Construct empty builder for mapper spec.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MapperSpec}
     */
    @NonNullApi
    @NonNullFields
    public static class Builder extends MapperOrReducerSpec.Builder<Builder> {
        /**
         * Construct {@link MapperSpec} instance.
         */
        @Override
        public MapperSpec build() {
            if (getUserJob() == null) {
                throw new IllegalStateException("Mapper is required and has no default value");
            }
            return new MapperSpec(this);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Set mapper.
         *
         * @see Mapper
         */
        public Builder setMapper(Mapper<?, ?> mapper) {
            return super.setUserJob(mapper);
        }
    }
}
