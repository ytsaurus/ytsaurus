package tech.ytsaurus.client.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.ysontree.YTreeBuilder;


public interface UserJobSpec {
    YTreeBuilder prepare(
            YTreeBuilder builder,
            TransactionalClient yt,
            SpecPreparationContext context,
            int outputTableCount);
}
