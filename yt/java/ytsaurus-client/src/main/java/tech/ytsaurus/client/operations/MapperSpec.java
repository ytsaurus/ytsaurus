package tech.ytsaurus.client.operations;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class MapperSpec extends MapperOrReducerSpec {
    public MapperSpec(Mapper<?, ?> mapper) {
        this(builder().setMapper(mapper));
    }

    protected MapperSpec(Builder builder) {
        super(MapMain.class, builder);
    }

    public static Builder builder(Mapper<?, ?> mapper) {
        Builder builder = new Builder();
        builder.setMapper(mapper);
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends MapperOrReducerSpec.Builder<Builder> {
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

        public Builder setMapper(Mapper<?, ?> mapper) {
            return super.setUserJob(mapper);
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
