package tech.ytsaurus.client.request;

import ru.yandex.yt.ytclient.operations.SortSpec;

public class SortOperation extends BaseOperation<SortSpec> {
    SortOperation(Builder builder) {
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

    public static class Builder extends BuilderBase<Builder, SortSpec> {
        public SortOperation build() {
            return new SortOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
