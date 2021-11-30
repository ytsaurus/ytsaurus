package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.object.ObjectsGenerators.Generator;
import ru.yandex.yt.ytclient.proxy.ModifyRowsRequest;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class ObjectsMetadata<T> {

    public static <T> ObjectsMetadata<T> getMetadata(Class<T> clazz, ConsumerSource<T> consumer) {
        return new ObjectsMetadata<>(clazz, consumer, ObjectsGenerators.generator(clazz));
    }

    private final YTreeObjectSerializer<T> yTreeSerializer;
    private final TableSchema tableSchema;
    private final UnversionedRowsetDeserializer unversionedDeserializer;
    private final UnversionedRowSerializer unversionedSerializer;
    private final MappedRowsetDeserializer<T> mappedDeserializer;
    private final MappedRowSerializer<T> mappedSerializer;
    private final Generator<T> objectGenerator;

    private ObjectsMetadata(Class<T> clazz, ConsumerSource<T> consumer, Generator<T> objectGenerator) {
        this.yTreeSerializer = (YTreeObjectSerializer<T>) YTreeObjectSerializerFactory.forClass(clazz);
        this.tableSchema = MappedRowSerializer.asTableSchema(yTreeSerializer.getFieldMap());
        this.unversionedDeserializer = new UnversionedRowsetDeserializer(tableSchema);
        this.unversionedSerializer = new UnversionedRowSerializer(tableSchema);
        this.mappedDeserializer = MappedRowsetDeserializer.forClass(tableSchema, yTreeSerializer, consumer);
        this.mappedSerializer = MappedRowSerializer.forClass(yTreeSerializer);
        this.objectGenerator = Objects.requireNonNull(objectGenerator);
    }

    public YTreeObjectSerializer<T> getyTreeSerializer() {
        return yTreeSerializer;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public UnversionedRowsetDeserializer getUnversionedDeserializer() {
        return unversionedDeserializer;
    }

    public UnversionedRowSerializer getUnversionedSerializer() {
        return unversionedSerializer;
    }

    public MappedRowsetDeserializer<T> getMappedDeserializer() {
        return mappedDeserializer;
    }

    public MappedRowSerializer<T> getMappedSerializer() {
        return mappedSerializer;
    }

    public List<T> generateObjects(int size, Random random) {
        final List<T> target = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            target.add(objectGenerator.generateNext(random));
        }
        return target;
    }

    public List<byte[]> generateAndSerializeObjects(int size, Random random) {
        return serializeMappedObjects(generateObjects(size, random));
    }

    public void deserializeMappedObjects(List<byte[]> chunks) {
        final WireProtocolReader reader = new WireProtocolReader(chunks);
        reader.readUnversionedRowset(this.mappedDeserializer);
    }

    public UnversionedRowset deserializeUnversionedObjects(List<byte[]> chunks) {
        final WireProtocolReader reader = new WireProtocolReader(chunks);
        reader.readUnversionedRowset(this.unversionedDeserializer);
        return this.unversionedDeserializer.getRowset();
    }

    public List<T> deserializeLegacyMappedObjects(List<byte[]> chunks) {
        final UnversionedRowset rowset = deserializeUnversionedObjects(chunks);
        final List<UnversionedRow> rows = rowset.getRows();
        final List<T> result = new ArrayList<>(rows.size());
        for (UnversionedRow row : rows) {
            result.add(yTreeSerializer.deserialize(row.toYTreeMap(tableSchema)));
        }
        return result;
    }

    public List<byte[]> serializeMappedObjects(List<T> rows) {
        final WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(rows, this.mappedSerializer);
        return writer.finish();
    }

    public List<byte[]> serializeMappedObjects(List<T> rows, Function<Integer, Boolean> keyFieldsOnlyFunction) {
        final WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(rows, this.mappedSerializer, keyFieldsOnlyFunction);
        return writer.finish();
    }

    public List<byte[]> serializeUnversionedObjects(List<UnversionedRow> rows) {
        final WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(rows, this.unversionedSerializer);
        return writer.finish();
    }

    public List<byte[]> serializeLegacyMappedObjects(List<T> rows) {
        final List<UnversionedRow> unversionedRows = convertObjectsToUnversioned(rows);
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(unversionedRows, new UnversionedRowSerializer(this.tableSchema));
        return writer.finish();
    }

    public List<UnversionedRow> convertObjectsToUnversioned(List<T> rows) {
        final ModifyRowsRequest request = new ModifyRowsRequest("", tableSchema);
        for (T row : rows) {
            final YTreeBuilder builder = YTree.builder();
            yTreeSerializer.serialize(row, builder);
            request.addUpdate(builder.build().asMap());
        }
        return request.getRows();
    }

}
