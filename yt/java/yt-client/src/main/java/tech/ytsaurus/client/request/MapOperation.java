package tech.ytsaurus.client.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.MapSpec;

@NonNullApi
@NonNullFields
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

    @NonNullApi
    @NonNullFields
    public static class Builder extends BuilderBase<Builder, MapSpec> {
        public MapOperation build() {
            return new MapOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
