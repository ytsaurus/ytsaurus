package tech.ytsaurus.client.request;

import ru.yandex.yt.ytclient.operations.ReduceSpec;

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

    public static class Builder extends BuilderBase<Builder, ReduceSpec> {
        public ReduceOperation build() {
            return new ReduceOperation(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
