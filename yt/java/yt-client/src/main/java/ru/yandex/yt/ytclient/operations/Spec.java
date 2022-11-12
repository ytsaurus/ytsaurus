package ru.yandex.yt.ytclient.operations;

import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.yt.ytclient.proxy.TransactionalClient;

public interface Spec {
    YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context);
}
