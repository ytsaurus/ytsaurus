package tech.ytsaurus.client.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.operations.MergeSpec;

@NonNullApi
@NonNullFields
public class MergeOperation extends BaseOperation<MergeSpec> {
    MergeOperation(Builder builder) {
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
    public static class Builder extends BuilderBase<Builder, MergeSpec> {
        public MergeOperation build() {
            return new MergeOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
