package ru.yandex.yt.ytclient.operations;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.ysontree.YTreeBuilder;


public interface Spec {
    YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context);
}
