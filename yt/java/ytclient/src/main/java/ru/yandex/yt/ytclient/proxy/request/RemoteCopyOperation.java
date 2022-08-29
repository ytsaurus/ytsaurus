package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.RemoteCopySpec;

@NonNullApi
@NonNullFields
public class RemoteCopyOperation extends BaseOperation<RemoteCopySpec> {
    RemoteCopyOperation(Builder builder) {
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
    public static class Builder extends BuilderBase<Builder, RemoteCopySpec> {
        public RemoteCopyOperation build() {
            return new RemoteCopyOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
