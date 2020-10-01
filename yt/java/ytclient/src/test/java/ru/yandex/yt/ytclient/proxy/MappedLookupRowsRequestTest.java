package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;
import ru.yandex.yt.ytclient.object.ObjectsMetadata;
import ru.yandex.yt.ytclient.object.SmallObjectClassWithStateSupport;
import ru.yandex.yt.ytclient.wire.WireProtocolTest;

public class MappedLookupRowsRequestTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedLookupRowsRequestTest.class);

    @Test
    public void testMappedModifyRowsRequest() {
        final ObjectsMetadata<SmallObjectClassPartial> metadata =
                ObjectsMetadata.getMetadata(SmallObjectClassPartial.class, value -> {

                });

        final MappedLookupRowsRequest<SmallObjectClassPartial> request =
                new MappedLookupRowsRequest<>("", metadata.getMappedSerializer());
        final List<SmallObjectClassPartial> sample1 = metadata.generateObjects(1);
        final List<SmallObjectClassPartial> sample2 = metadata.generateObjects(2);
        final List<SmallObjectClassPartial> sample3 = metadata.generateObjects(1);
        final List<SmallObjectClassPartial> sample4 = metadata.generateObjects(2);
        final List<SmallObjectClassPartial> sample5 = metadata.generateObjects(1);
        final List<SmallObjectClassPartial> sample6 = metadata.generateObjects(2);

        Stream.of(sample1, sample2).flatMap(Collection::stream)
                .forEach(YTreeStateSupport.saveProxy(obj -> obj.setStringField("Changed value")));

        request.addFilter(sample1.get(0));
        request.addFilters(sample2);

        request.addFilter(sample3.get(0));
        request.addFilters(sample4);

        request.addFilter(sample5.get(0));
        request.addFilters(sample6);

        // Сохраняем только ключи (проверяем, что объект будет корректно сериализован)
        final List<SmallObjectClassPartial> all = new ArrayList<>();
        Stream.of(sample1, sample2, sample3, sample4, sample5, sample6).flatMap(Collection::stream).forEach(obj -> {
            final SmallObjectClassPartial changedValue = new SmallObjectClassPartial();
            changedValue.setIntField(obj.getIntField());
            all.add(changedValue);
        });

        all.forEach(obj -> LOGGER.info("{}", obj));

        final List<byte[]> expect = metadata.serializeMappedObjects(all, i -> i >= 6);

        final List<byte[]> actual = new ArrayList<>();
        request.serializeRowsetTo(actual);

        Assert.assertArrayEquals(WireProtocolTest.mergeChunks(expect), WireProtocolTest.mergeChunks(actual));
    }


    // nullSerializationStrategy только для того, чтобы отрендерить корректный expect
    @YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
    private static class SmallObjectClassPartial extends SmallObjectClassWithStateSupport {

    }
}
