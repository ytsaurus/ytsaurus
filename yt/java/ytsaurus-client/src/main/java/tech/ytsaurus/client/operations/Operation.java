package tech.ytsaurus.client.operations;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
public interface Operation {
    GUID getId();

    CompletableFuture<OperationStatus> getStatus();

    CompletableFuture<YTreeNode> getResult();

    CompletableFuture<Void> watch();

    CompletableFuture<Void> watchAndThrowIfNotSuccess();

    CompletableFuture<Void> abort();

    CompletableFuture<Void> complete();
}
