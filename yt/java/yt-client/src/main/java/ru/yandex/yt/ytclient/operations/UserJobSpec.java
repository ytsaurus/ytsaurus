package ru.yandex.yt.ytclient.operations;

import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.yt.ytclient.proxy.TransactionalClient;

public interface UserJobSpec {
    YTreeBuilder prepare(
            YTreeBuilder builder,
            TransactionalClient yt,
            SpecPreparationContext context,
            int outputTableCount);
}
