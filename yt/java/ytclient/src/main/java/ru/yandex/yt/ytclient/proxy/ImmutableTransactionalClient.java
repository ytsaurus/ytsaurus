package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public interface ImmutableTransactionalClient {
    CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request);

    <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    );

    CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request);

    <T> CompletableFuture<List<T>> versionedLookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    );

    CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request);

    <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeObjectSerializer<T> serializer
    );
}
