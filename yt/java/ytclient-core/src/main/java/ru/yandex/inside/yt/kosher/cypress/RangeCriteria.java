package ru.yandex.inside.yt.kosher.cypress;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

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

