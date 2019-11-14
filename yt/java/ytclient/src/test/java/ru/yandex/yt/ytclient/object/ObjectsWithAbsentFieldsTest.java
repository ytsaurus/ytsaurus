package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullDeserializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ObjectsWithAbsentFieldsTest {

    private ObjectsMetadata<OuterClassV1> metadataV1;

    @Before
    public void init() {
        metadataV1 = ObjectsMetadata.getMetadata(OuterClassV1.class, v -> {
        });
    }

    @Test(expected = RuntimeException.class)
    public void testSerializationDefault() {
        final ObjectsMetadata<OuterClassV2Default> metadataV2 =
                ObjectsMetadata.getMetadata(OuterClassV2Default.class, v -> {
                });

        final OuterClassV1 v1 = buildSample();

        final List<byte[]> bytes = metadataV1.serializeMappedObjects(Collections.singletonList(v1));
        metadataV2.deserializeMappedObjects(bytes);
    }

    @Test
    public void testSerializationWithAbsentFields() {

        final List<OuterClassV2> listV2 = new ArrayList<>();
        final ObjectsMetadata<OuterClassV2> metadataV2 = ObjectsMetadata.getMetadata(OuterClassV2.class, listV2::add);

        final OuterClassV1 v1 = buildSample();
        final InnerClassV1 i1 = v1.inner;

        final List<byte[]> bytes = metadataV1.serializeMappedObjects(Collections.singletonList(v1));
        metadataV2.deserializeMappedObjects(bytes);

        final OuterClassV2 v2 = listV2.get(0);
        assertNotEquals(v1, v2);

        final InnerClassV2 i2 = v2.innerList.get(0);
        assertEquals(v2.inner, i2);
        assertEquals(v2.innerFlatten, i2);
        assertEquals(i1.iValue, i2.iValue);
        assertEquals(i1.sValue, i2.sValue);
        assertEquals(v2.sample, v1.sample);

        assertEquals(0, i2.dValue, 1E-06);
    }

    @Test
    public void testUnsupportedConfigurationFlattenOk() {
        ObjectsMetadata.getMetadata(OuterClassV1FlattenOk.class, v -> {
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsupportedConfigurationFlattenIsNotLast() {
        ObjectsMetadata.getMetadata(OuterClassV1FlattenIsNotLast.class, v -> {
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsupportedConfigurationFlattenMultiple() {
        ObjectsMetadata.getMetadata(OuterClassV1FlattenMultiple.class, v -> {
        });
    }

    static OuterClassV1 buildSample() {
        final OuterClassV1 v1 = new OuterClassV1();
        final InnerClassV1 i1 = new InnerClassV1();
        i1.iValue = 1;
        i1.sValue = "test";
        v1.innerList = Collections.singletonList(i1);
        v1.inner = i1;
        v1.innerFlatten = i1;
        v1.sample = Collections.emptyList();
        return v1;
    }

    @YTreeObject
    static class OuterClassV1 {
        InnerClassV1 inner;
        List<InnerClassV1> innerList;
        List<String> sample;
        @YTreeFlattenField
        InnerClassV1 innerFlatten;
    }

    @YTreeObject
    static class OuterClassV2 {
        InnerClassV1 inner;
        List<InnerClassV2> innerList;
        List<String> sample;
        @YTreeFlattenField
        InnerClassV2 innerFlatten;
    }

    @YTreeObject
    static class OuterClassV2Default {
        InnerClassV1 inner;
        List<InnerClassV2Default> innerList;
        List<String> sample;
        @YTreeFlattenField
        InnerClassV2Default innerFlatten;
    }

    @YTreeObject(nullDeserializationStrategy = NullDeserializationStrategy.IGNORE_ABSENT_FIELDS)
    static class OuterClassV1FlattenOk {
        InnerClassV1 inner;
        List<InnerClassV1> innerList;
        List<String> sample;
        @YTreeFlattenField
        InnerClassV1 innerFlatten;
    }

    @YTreeObject(nullDeserializationStrategy = NullDeserializationStrategy.IGNORE_ABSENT_FIELDS)
    static class OuterClassV1FlattenIsNotLast {
        @YTreeFlattenField
        InnerClassV1 innerFlatten;
        InnerClassV1 inner;
    }

    @YTreeObject(nullDeserializationStrategy = NullDeserializationStrategy.IGNORE_ABSENT_FIELDS)
    static class OuterClassV1FlattenMultiple {
        @YTreeFlattenField
        InnerClassV1 innerFlatten1;
        @YTreeFlattenField
        InnerClassV3 innerFlatten3;
    }

    @YTreeObject
    static class InnerClassV1 {
        int iValue;
        String sValue;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof InnerClassV1)) {
                return false;
            }
            InnerClassV1 that = (InnerClassV1) o;
            return iValue == that.iValue &&
                    Objects.equals(sValue, that.sValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(iValue, sValue);
        }
    }

    @YTreeObject
    static class InnerClassV2Default extends InnerClassV1 {
        double dValue;
    }

    // Без этой опции класс нельзя было бы корректно десериализовать
    @YTreeObject(nullDeserializationStrategy = NullDeserializationStrategy.IGNORE_ABSENT_FIELDS)
    static class InnerClassV2 extends InnerClassV1 {
        double dValue;
    }


    @YTreeObject
    static class InnerClassV3 {
        double dValue;
    }
}
