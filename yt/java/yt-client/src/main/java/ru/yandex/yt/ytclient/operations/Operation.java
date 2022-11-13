package ru.yandex.yt.ytclient.operations;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

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
