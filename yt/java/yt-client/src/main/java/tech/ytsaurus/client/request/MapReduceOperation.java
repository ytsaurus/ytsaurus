package tech.ytsaurus.client.request;

import ru.yandex.yt.ytclient.operations.MapReduceSpec;

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

    public static class Builder extends BuilderBase<Builder, MapReduceSpec> {
        public MapReduceOperation build() {
            return new MapReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
