package tech.ytsaurus.core.cypress;

import javax.annotation.Nullable;

import tech.ytsaurus.ysontree.YTreeBuilder;

/**
 * @author and-hom
 */
public abstract class RangeCriteria {
    public abstract YTreeBuilder addRangeCriteria(YTreeBuilder builder);

    @Nullable
    public abstract RangeCriteria forRetry(long nextRowIndex);

    YTreeBuilder addReadLimit(YTreeBuilder builder, RangeLimit limit, String key) {
        return builder.key(key).apply(limit::toTree);
    }
}

