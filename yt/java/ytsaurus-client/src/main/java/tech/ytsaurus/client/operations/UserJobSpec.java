package tech.ytsaurus.client.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;

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
