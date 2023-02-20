package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.client.request.ReadSerializationContext;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.rows.WireRowDeserializer;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;

@NonNullApi
@NonNullFields
public class ReadTable<T> extends tech.ytsaurus.client.request.ReadTable.BuilderBase<T, ReadTable<T>> {

    public ReadTable(YPath path, WireRowDeserializer<T> deserializer) {
        setPath(path).setSerializationContext(
                new ReadSerializationContext<>(deserializer));
    }

    public ReadTable(YPath path, YTreeObjectSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new SerializationContext<T>(serializer));
    }

    public ReadTable(YPath path, YTreeSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new tech.ytsaurus.client.request.SerializationContext<T>(serializer));
    }

    public ReadTable(YPath path, Class<T> objectClazz) {
        setPath(path).setSerializationContext(
                new tech.ytsaurus.client.request.SerializationContext<T>(objectClazz));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, WireRowDeserializer<T> deserializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, WireRowDeserializer<T> deserializer) {
        setPath(path).setSerializationContext(
                new ReadSerializationContext<>(deserializer));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, YTreeObjectSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeObjectSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new tech.ytsaurus.client.request.SerializationContext<T>(serializer));
    }

    /**
     * @deprecated Use {@link #ReadTable(YPath path, YTreeSerializer<T> serializer)} instead.
     */
    @Deprecated
    public ReadTable(String path, YTreeSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new tech.ytsaurus.client.request.SerializationContext<T>(serializer));
    }

    @Override
    protected ReadTable<T> self() {
        return this;
    }
}
