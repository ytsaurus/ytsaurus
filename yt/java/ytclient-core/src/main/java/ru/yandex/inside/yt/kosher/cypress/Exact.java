package ru.yandex.inside.yt.kosher.cypress;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

/**
 * @author and-hom
 */
public class Exact extends RangeCriteria {
    @SuppressWarnings("VisibilityModifier")
    public final RangeLimit exact;

    public Exact(RangeLimit exact) {
        this.exact = exact;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Exact exact1 = (Exact) o;
        return Objects.equals(exact, exact1.exact);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exact);
    }

    @Override
    public YTreeBuilder addRangeCriteria(YTreeBuilder builder) {
        addReadLimit(builder, exact, "exact");
        return builder;
    }

    @Override
    @Nullable
    public RangeCriteria forRetry(long nextRowIndex) {
        if (!exact.key.isEmpty() && exact.rowIndex == -1 && exact.offset == -1) {
            YTreeNode lastPart = YTree.builder()
                    .beginAttributes()
                        .key("type")
                        .value("max")
                    .endAttributes()
                    .entity()
                    .build();
            List<YTreeNode> upperKey = new ArrayList<>(exact.key);
            upperKey.add(lastPart);
            return new Range(
                    new RangeLimit(exact.key, nextRowIndex, -1),
                    new RangeLimit(upperKey, -1, -1));
        } else if (exact.rowIndex == nextRowIndex && exact.offset == -1) {
            return this;
        } else {
            // return new Exact(exact.rowIndex(nextRowIndex));
            // see YTADMINREQ-12492
            // throw new IllegalStateException();
            return null;
        }
    }
}

