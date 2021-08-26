package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeSaveAlways;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.AbstractYTreeStateSupport;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;
import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.object.ObjectsMetadata;
import ru.yandex.yt.ytclient.wire.WireProtocolTest;

public class MappedModifyRowsRequestTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedModifyRowsRequestTest.class);

    @Test
    public void testMappedModifyRowsRequest() {
        this.testMappedModifyRowsRequestImpl(
                KvClass1.class,
                KvClass1::new,
                obj -> obj.stringField = "Changed value",
                (from, to) -> to.intField = from.intField,
                (from, to) -> to.stringField = from.stringField);
    }

    @Test
    public void testMappedModifyRowsRequestWithPersistentDoubleField() {
        this.testMappedModifyRowsRequestImpl(
                KvClass2.class,
                KvClass2::new,
                obj -> obj.stringField = "Changed value",
                (from, to) -> to.intField = from.intField,
                (from, to) -> {
                    to.doubleField = from.doubleField; // поле помечено аннотацией и всегда сохраняется
                    to.stringField = from.stringField;
                });
    }

    private <T extends YTreeStateSupport<T>> void testMappedModifyRowsRequestImpl(
            Class<T> clazz, Supplier<T> newInstance, Consumer<T> changeSetter,
            BiConsumer<T, T> keySetter, BiConsumer<T, T> persistentSetter)
    {

        final Random random = new Random(42);
        final ObjectsMetadata<T> metadata = ObjectsMetadata.getMetadata(clazz, value -> { });

        final MappedModifyRowsRequest<T> request =
                new MappedModifyRowsRequest<>("//tmp", metadata.getMappedSerializer());
        final List<T> sample1 = metadata.generateObjects(1, random);
        final List<T> sample2 = metadata.generateObjects(2, random);
        final List<T> sample3 = metadata.generateObjects(1, random);
        final List<T> sample4 = metadata.generateObjects(2, random);
        final List<T> sample5 = metadata.generateObjects(1, random);
        final List<T> sample6 = metadata.generateObjects(2, random);

        Stream.of(sample1, sample2).flatMap(Collection::stream).forEach(YTreeStateSupport.saveProxy(changeSetter));

        request.addInsert(sample1.get(0));
        request.addInserts(sample2);

        request.addInsert(sample3.get(0));
        request.addInserts(sample4);

        request.addDelete(sample5.get(0));
        request.addDeletes(sample6);

        Assert.assertEquals(Arrays.asList(
                ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE,
                ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE,
                ERowModificationType.RMT_DELETE, ERowModificationType.RMT_DELETE, ERowModificationType.RMT_DELETE),
                request.getRowModificationTypes());

        final List<T> all = new ArrayList<>();
        Stream.of(sample1, sample2).flatMap(Collection::stream).forEach(from -> {
            final T to = newInstance.get();
            keySetter.accept(from, to);
            persistentSetter.accept(from, to);
            all.add(to);
        });
        all.addAll(sample3);
        all.addAll(sample4);

        // Сохраняем весь объект целиком - он сериализованы все равно будут только ключи
        all.addAll(sample5);

        // Сохраняем только ключи (проверяем, что объект будет корректно сериализован)
        sample6.forEach(from -> {
            final T to = newInstance.get();
            keySetter.accept(from, to);
            all.add(to);
        });

        all.forEach(obj -> LOGGER.info("{}", obj));

        final List<byte[]> expect = metadata.serializeMappedObjects(all, i -> i >= 6);

        final List<byte[]> actual = new ArrayList<>();
        request.serializeRowsetTo(actual);

        Assert.assertArrayEquals(WireProtocolTest.mergeChunks(expect), WireProtocolTest.mergeChunks(actual));
    }


    @YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
    static class KvClass1 extends AbstractYTreeStateSupport<KvClass1> {
        @YTreeKeyField
        private Integer intField;
        private Long longField;
        private Float floatField;
        private Double doubleField;
        private Boolean booleanField;
        private String stringField;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KvClass1 that = (KvClass1) o;
            return Objects.equals(intField, that.intField) &&
                    Objects.equals(longField, that.longField) &&
                    Objects.equals(floatField, that.floatField) &&
                    Objects.equals(doubleField, that.doubleField) &&
                    Objects.equals(booleanField, that.booleanField) &&
                    Objects.equals(stringField, that.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intField, longField, floatField, doubleField, booleanField, stringField);
        }

        @Override
        public String toString() {
            return "KvClass1{" +
                    "intField=" + intField +
                    ", longField=" + longField +
                    ", floatField=" + floatField +
                    ", doubleField=" + doubleField +
                    ", booleanField=" + booleanField +
                    ", stringField='" + stringField + '\'' +
                    '}';
        }
    }

    @YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
    static class KvClass2 extends AbstractYTreeStateSupport<KvClass2> {
        @YTreeKeyField
        private Integer intField;
        private Long longField;
        private Float floatField;
        @YTreeSaveAlways
        private Double doubleField;
        private Boolean booleanField;
        private String stringField;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KvClass2 that = (KvClass2) o;
            return Objects.equals(intField, that.intField) &&
                    Objects.equals(longField, that.longField) &&
                    Objects.equals(floatField, that.floatField) &&
                    Objects.equals(doubleField, that.doubleField) &&
                    Objects.equals(booleanField, that.booleanField) &&
                    Objects.equals(stringField, that.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intField, longField, floatField, doubleField, booleanField, stringField);
        }

        @Override
        public String toString() {
            return "KvClass2{" +
                    "intField=" + intField +
                    ", longField=" + longField +
                    ", floatField=" + floatField +
                    ", doubleField=" + doubleField +
                    ", booleanField=" + booleanField +
                    ", stringField='" + stringField + '\'' +
                    '}';
        }
    }

}
