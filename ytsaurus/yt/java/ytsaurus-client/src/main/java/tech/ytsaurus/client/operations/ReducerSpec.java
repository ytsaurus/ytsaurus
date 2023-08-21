package tech.ytsaurus.client.operations;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Immutable reducer spec.
 */
@NonNullApi
@NonNullFields
public class ReducerSpec extends MapperOrReducerSpec {
    /**
     * Construct reducer spec from reducer with other options set to defaults.
     */
    public ReducerSpec(Reducer<?, ?> reducer) {
        this(builder().setReducer(reducer));
    }

    protected ReducerSpec(Builder builder) {
        super(ReduceMain.class, builder);
    }

    /**
     * Construct empty builder for reducer spec.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct builder for reducer spec with specified reducer.
     */
    public static Builder builder(Reducer<?, ?> reducer) {
        Builder builder = new Builder();
        builder.setReducer(reducer);
        return builder;
    }

    /**
     * Builder for {@link ReducerSpec}
     */
    @NonNullApi
    @NonNullFields
    public static class Builder extends MapperOrReducerSpec.Builder<Builder> {
        /**
         * Construct {@link ReducerSpec} instance.
         */
        @Override
        public ReducerSpec build() {
            if (getUserJob() == null) {
                throw new IllegalStateException("Reducer is required and has no default value");
            }
            return new ReducerSpec(this);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Set reducer.
         *
         * @see Reducer
         */
        public Builder setReducer(Reducer<?, ?> reducer) {
            return setUserJob(reducer);
        }
    }
}
