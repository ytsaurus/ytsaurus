package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.MapReduceSpec;

@NonNullApi
@NonNullFields
public class MapReduceOperation extends BaseOperation<MapReduceSpec> {
    MapReduceOperation(Builder builder) {
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

    @NonNullApi
    @NonNullFields
    public static class Builder extends BuilderBase<Builder, MapReduceSpec> {
        public MapReduceOperation build() {
            return new MapReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
