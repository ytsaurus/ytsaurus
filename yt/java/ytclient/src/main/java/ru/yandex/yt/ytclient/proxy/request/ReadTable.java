package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;

@NonNullApi
@NonNullFields
public class ReadTable<T> extends ru.yandex.yt.ytclient.request.ReadTable.BuilderBase<T, ReadTable<T>> {

    public ReadTable(YPath path, WireRowDeserializer<T> deserializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(deserializer));
    }

    public ReadTable(YPath path, YTreeObjectSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(serializer));
    }

    public ReadTable(YPath path, YTreeSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(serializer));
    }

    public ReadTable(YPath path, Class<T> objectClazz) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(objectClazz));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, WireRowDeserializer<T> deserializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, WireRowDeserializer<T> deserializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(deserializer));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, YTreeObjectSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeObjectSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(serializer));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path,  YTreeSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.ReadTable.SerializationContext<T>(serializer));
    }

    @Override
    protected ReadTable<T> self() {
        return this;
    }
}
