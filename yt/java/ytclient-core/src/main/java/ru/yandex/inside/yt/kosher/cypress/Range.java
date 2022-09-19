package ru.yandex.inside.yt.kosher.cypress;

import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

/**
 * @author sankear
 */
public class Range extends RangeCriteria {

    @SuppressWarnings("VisibilityModifier")
    public final RangeLimit lower;
    @SuppressWarnings("VisibilityModifier")
    public final RangeLimit upper;

    public Range(RangeLimit lower, RangeLimit upper) {
        this.lower = lower;
        this.upper = upper;
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
        addReadLimit(builder, lower, "lower_limit");
        addReadLimit(builder, upper, "upper_limit");
        return builder;
    }

    @Override
    @Nonnull
    public RangeCriteria forRetry(long nextRowIndex) {
        return new Range(lower.rowIndex(nextRowIndex), upper);
    }
}
