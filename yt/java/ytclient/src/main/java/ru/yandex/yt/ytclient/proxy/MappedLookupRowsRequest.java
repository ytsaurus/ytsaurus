package ru.yandex.yt.ytclient.proxy;

import tech.ytsaurus.client.rows.MappedRowSerializer;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;

@NonNullApi
@NonNullFields
public class MappedLookupRowsRequest<T> extends tech.ytsaurus.client.request.MappedLookupRowsRequest.BuilderBase<
        T, MappedLookupRowsRequest<T>> {
    public MappedLookupRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedLookupRowsRequest(String path, WireRowSerializer<T> serializer) {
        setPath(path).setSerializer(serializer);
    }

    @Override
    protected MappedLookupRowsRequest<T> self() {
        return this;
    }

    @Override
    public tech.ytsaurus.client.request.MappedLookupRowsRequest<T> build() {
        return new tech.ytsaurus.client.request.MappedLookupRowsRequest<>(this);
    }
}
