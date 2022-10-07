package ru.yandex.yt.ytclient.operations;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;

public interface Spec {
    YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context);
}
