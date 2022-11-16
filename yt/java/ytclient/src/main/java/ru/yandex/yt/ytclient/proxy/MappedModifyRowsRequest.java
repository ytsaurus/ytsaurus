package ru.yandex.yt.ytclient.proxy;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;

/**
 * Row modification request that uses YTreeObject annotated classes as table row representation
 *
 * @param <T> YTreeObject class
 *
 * @see ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject
 * @see ModifyRowsRequest
 */
public class MappedModifyRowsRequest<T>
        extends tech.ytsaurus.client.request.MappedModifyRowsRequest.BuilderBase<T, MappedModifyRowsRequest<T>> {

    public MappedModifyRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedModifyRowsRequest(String path, WireRowSerializer<T> serializer) {
        setPath(path).setSerializer(serializer);
    }

    @Override
    protected MappedModifyRowsRequest<T> self() {
        return this;
    }

    @Override
    public tech.ytsaurus.client.request.MappedModifyRowsRequest<T> build() {
        return new tech.ytsaurus.client.request.MappedModifyRowsRequest<>(this);
    }
}
