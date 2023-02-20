package tech.ytsaurus.client.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.ysontree.YTreeNode;

public class FormatContext {
    private final Integer inputTableCount;
    private final Integer outputTableCount;
    @Nullable
    private final YTreeNode outputStreams;

    private FormatContext(Builder builder) {
        this(builder.inputTableCount, builder.outputTableCount, builder.outputStreams);
    }

    private FormatContext(Integer inputTableCount, Integer outputTableCount,
                          @Nullable YTreeNode outputStreams) {
        this.inputTableCount = inputTableCount;
        this.outputTableCount = outputTableCount;
        this.outputStreams = outputStreams;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FormatContext empty() {
        return new FormatContext(null, null, null);
    }

    public Optional<Integer> getInputTableCount() {
        return Optional.ofNullable(inputTableCount);
    }

    public Optional<Integer> getOutputTableCount() {
        return Optional.ofNullable(outputTableCount);
    }

    public Optional<YTreeNode> getOutputStreams() {
        return Optional.ofNullable(outputStreams);
    }

    public static class Builder {
        private int inputTableCount = 1;
        private int outputTableCount = 1;
        @Nullable
        private YTreeNode outputStreams;

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

        public Builder setOutputStreams(YTreeNode outputStreams) {
            this.outputStreams = outputStreams;
            return this;
        }
    }
}
