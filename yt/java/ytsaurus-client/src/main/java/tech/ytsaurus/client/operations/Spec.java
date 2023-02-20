package tech.ytsaurus.client.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Base interface for specs. Allow to create spec as a yson.
 */
@NonNullApi
public interface Spec {
    YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext specPreparationContext);
}
