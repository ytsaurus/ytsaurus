package ru.yandex.yt.ytclient.operations;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class ReducerSpec extends MapperOrReducerSpec {
    public ReducerSpec(Reducer<?, ?> reducer) {
        this(builder().setReducer(reducer));
    }

    protected ReducerSpec(Builder builder) {
        super(ReduceMain.class, builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Reducer<?, ?> reducer) {
        Builder builder = new Builder();
        builder.setReducer(reducer);
        return builder;
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends MapperOrReducerSpec.Builder<Builder> {
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

        public Builder setReducer(Reducer<?, ?> reducer) {
            return setUserJob(reducer);
        }

        /**
         * @deprecated This method is no-op.
         * It's going to be removed.
         */
        @Deprecated
        public Builder setOutputTables(int outputTables) {
            return this;
        }
    }
}
