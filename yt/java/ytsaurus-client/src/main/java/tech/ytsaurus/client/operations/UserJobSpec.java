package tech.ytsaurus.client.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Interface for specs of user jobs.
 */
@NonNullApi
public interface UserJobSpec {
    YTreeBuilder prepare(
            YTreeBuilder builder,
            TransactionalClient yt,
            SpecPreparationContext specPreparationContext,
            FormatContext formatContext);
}
