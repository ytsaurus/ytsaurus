package tech.ytsaurus.client.operations;

import java.util.Optional;

public class FormatContext {
    private final Integer inputTableCount;
    private final Integer outputTableCount;

    public FormatContext(Builder builder) {
        this(builder.inputTableCount, builder.outputTableCount);
    }

    private FormatContext(Integer inputTableCount, Integer outputTableCount) {
        this.inputTableCount = inputTableCount;
        this.outputTableCount = outputTableCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FormatContext empty() {
        return new FormatContext(null, null);
    }

    public Optional<Integer> getInputTableCount() {
        return Optional.ofNullable(inputTableCount);
    }

    public Optional<Integer> getOutputTableCount() {
        return Optional.ofNullable(outputTableCount);
    }

    public static class Builder {
        private int inputTableCount = 1;
        private int outputTableCount = 1;

        public FormatContext build() {
            return new FormatContext(this);
        }

        public Builder setInputTableCount(int inputTableCount) {
            this.inputTableCount = inputTableCount;
            return this;
        }

        public Builder setOutputTableCount(int outputTableCount) {
            this.outputTableCount = outputTableCount;
            return this;
        }
    }
}
