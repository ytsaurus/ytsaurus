package tech.ytsaurus.core.cypress;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


@NonNullApi
@NonNullFields
public class Range extends RangeCriteria {

    @SuppressWarnings("VisibilityModifier")
    @Nullable
    public final RangeLimit lower;
    @SuppressWarnings("VisibilityModifier")
    @Nullable
    public final RangeLimit upper;

    public Range(@Nullable RangeLimit lower, @Nullable RangeLimit upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public static Range lower(RangeLimit lowerLimit) {
        return new Range(lowerLimit, null);
    }

    public static Range upper(RangeLimit upperLimit) {
        return new Range(null, upperLimit);
    }

    public static Range of(RangeLimit lowerLimit, RangeLimit upperLimit) {
        return new Range(lowerLimit, upperLimit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return Objects.equals(lower, range.lower)
                && Objects.equals(upper, range.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lower, upper);
    }

    @Override
    public YTreeBuilder addRangeCriteria(YTreeBuilder builder) {
        if (lower != null) {
            addReadLimit(builder, lower, "lower_limit");
        }
        if (upper != null) {
            addReadLimit(builder, upper, "upper_limit");
        }
        return builder;
    }

    @Override
    @Nonnull
    public RangeCriteria forRetry(long nextRowIndex) {
        return new Range(lower.toBuilder().setRowIndex(nextRowIndex).build(), upper);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        RangeLimit lowerLimit;
        @Nullable
        RangeLimit upperLimit;

        public Builder setLowerLimit(RangeLimit lowerLimit) {
            this.lowerLimit = lowerLimit;
            return this;
        }

        public Builder setUpperLimit(RangeLimit upperLimit) {
            this.upperLimit = upperLimit;
            return this;
        }

        public Range build() {
            return new Range(lowerLimit, upperLimit);
        }
    }
}
