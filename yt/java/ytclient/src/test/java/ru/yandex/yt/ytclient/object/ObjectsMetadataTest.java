package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.yt.ytclient.wire.UnversionedRowset;

@RunWith(Parameterized.class)
public class ObjectsMetadataTest<T> {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {SmallObjectClass.class},
                {SmallPrimitiveClass.class},
                {LargeObjectClass.class},
                {LargePrimitiveClass.class},
                {LargeFlattenObjectClass.class},
                {LargeFlattenPrimitiveClass.class},
                {LargeUnflattenObjectClass.class},
                {LargeUnflattenPrimitiveClass.class}
        };
    }

    @Parameterized.Parameter
    public Class<T> clazz;

    private List<T> list;
    private ObjectsMetadata<T> metadata;

    @Before
    public void init() {
        list = new ArrayList<>();
        metadata = ObjectsMetadata.getMetadata(clazz, list::add);
    }

    @Test
    public void testGenerate0() {
        Assert.assertEquals(0, metadata.generateObjects(0).size());
    }

    @Test
    public void testGenerate3() {
        final List<T> items = metadata.generateObjects(3);
        Assert.assertEquals(3, items.size());
        Assert.assertNotEquals(items.get(0), items.get(1));
        Assert.assertNotEquals(items.get(0), items.get(2));
    }

    @Test
    public void testSerializeDeserializeMapped() {
        final List<T> expect = metadata.generateObjects(3);

        final List<byte[]> serialized = metadata.serializeMappedObjects(expect);

        list.clear();
        metadata.deserializeMappedObjects(serialized);
        Assert.assertEquals(expect, list);

        // Список не распухает
        list.clear();
        metadata.deserializeMappedObjects(serialized);
        Assert.assertEquals(expect, list);

    }

    @Test
    public void testSerializeDeserializeUnversioned() {
        final List<T> expect = metadata.generateObjects(3);

        final List<byte[]> serialized = metadata.serializeMappedObjects(expect);

        list.clear();
        metadata.deserializeMappedObjects(serialized);
        Assert.assertEquals(expect, list);

        final UnversionedRowset actual = metadata.deserializeUnversionedObjects(serialized);
        Assert.assertEquals(expect.size(), actual.getRows().size());

        // Сериализация/десерализация работает при многократном конвертировании
        list.clear();
        final List<byte[]> serializedAgain = metadata.serializeUnversionedObjects(actual.getRows());
        metadata.deserializeMappedObjects(serializedAgain);
        Assert.assertEquals(expect, this.list);
    }

    @Test
    public void testSerializeDeserializeLegacy() {
        final List<T> expect = metadata.generateObjects(3);
        final List<byte[]> serialized = metadata.serializeLegacyMappedObjects(expect);

        final List<T> actual = metadata.deserializeLegacyMappedObjects(serialized);
        Assert.assertEquals(expect, actual);
    }
}
