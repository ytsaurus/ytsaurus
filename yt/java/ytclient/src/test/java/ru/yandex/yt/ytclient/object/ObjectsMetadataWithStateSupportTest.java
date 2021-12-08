package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;

@RunWith(Parameterized.class)
public class ObjectsMetadataWithStateSupportTest<T extends YTreeStateSupport<T>> extends ObjectsMetadataTest<T> {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {SmallObjectClassWithStateSupport.class},
                {LargeObjectClassWithStateSupport.class},
                {LargeFlattenObjectClassWithStateSupport.class},
                {LargeUnflattenObjectClassWithStateSupport.class},
                {SmallObjectWithComplexFieldAndStateSupport.class}
        };
    }

    @Test
    public void testSerializeDeserializeMappedWithStateSupport() {
        final Random random = new Random(42);
        final List<YTreeObjectField<?>> keyFields = metadata.getyTreeSerializer().getFieldMap().values().stream()
                .filter(field -> field.isKeyField)
                .collect(Collectors.toList());

        final List<T> toSave = metadata.generateObjects(3, random);
        final List<T> expect = new ArrayList<>();
        toSave.forEach(YTreeStateSupport.saveProxy(value -> {
            // В таком режиме будут сериализованы только ключевые поля
            final T newValue = metadata.getyTreeSerializer().newInstance();
            keyFields.forEach(key -> {
                try {
                    key.field.set(newValue, key.field.get(value));
                } catch (IllegalAccessException | IllegalArgumentException ex) {
                    throw new RuntimeException(ex);
                }
            });
            expect.add(newValue);
        }));

        final List<byte[]> serialized = metadata.serializeMappedObjects(toSave);

        list.clear();
        metadata.deserializeMappedObjects(serialized);
        Assert.assertEquals(expect, list);

        // Список не распухает
        list.clear();
        metadata.deserializeMappedObjects(serialized);
        Assert.assertEquals(expect, list);

    }


}
