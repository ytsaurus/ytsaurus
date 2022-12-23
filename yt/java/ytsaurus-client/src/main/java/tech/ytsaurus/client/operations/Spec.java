package tech.ytsaurus.client.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;

/**
 * Base interface for specs. Allow to create spec as a yson.
 */
@NonNullApi
public interface Spec {
    YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext specPreparationContext);
}
