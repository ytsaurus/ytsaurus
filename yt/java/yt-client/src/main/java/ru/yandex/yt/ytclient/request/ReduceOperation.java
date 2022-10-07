package ru.yandex.yt.ytclient.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.ReduceSpec;

@NonNullApi
@NonNullFields
public class ReduceOperation extends BaseOperation<ReduceSpec> {
    ReduceOperation(Builder builder) {
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
    public static class Builder extends BuilderBase<Builder, ReduceSpec> {
        public ReduceOperation build() {
            return new ReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
