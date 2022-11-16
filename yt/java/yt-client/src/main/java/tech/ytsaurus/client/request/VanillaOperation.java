package tech.ytsaurus.client.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.VanillaSpec;

public class VanillaOperation extends BaseOperation<VanillaSpec> {
    VanillaOperation(Builder builder) {
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
    public static class Builder extends BuilderBase<Builder, VanillaSpec> {
        public VanillaOperation build() {
            return new VanillaOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
