package ru.yandex.yt.ytclient.operations;

import java.util.concurrent.CompletableFuture;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public interface Operation {
    GUID getId();

    CompletableFuture<OperationStatus> getStatus();

    CompletableFuture<YTreeNode> getResult();

    CompletableFuture<Void> watch();

    CompletableFuture<Void> abort();
}
