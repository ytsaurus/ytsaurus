package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.object.ObjectsMetadata;
import ru.yandex.yt.ytclient.object.SmallObjectClass;
import ru.yandex.yt.ytclient.wire.WireProtocolTest;

public class MappedModifyRowsRequestTest {

    @Test
    public void testMappedModifyRowsRequest() {
        final ObjectsMetadata<SmallObjectClass> metadata =
                ObjectsMetadata.getMetadata(SmallObjectClass.class, value -> {

                });

        final MappedModifyRowsRequest<SmallObjectClass> request =
                new MappedModifyRowsRequest<>("", metadata.getMappedSerializer());
        final List<SmallObjectClass> sample1 = metadata.generateObjects(1);
        final List<SmallObjectClass> sample2 = metadata.generateObjects(2);
        final List<SmallObjectClass> sample3 = metadata.generateObjects(1);
        final List<SmallObjectClass> sample4 = metadata.generateObjects(2);
        final List<SmallObjectClass> sample5 = metadata.generateObjects(1);
        final List<SmallObjectClass> sample6 = metadata.generateObjects(2);

        request.addUpdate(sample1.get(0));
        request.addUpdates(sample2);

        request.addInsert(sample3.get(0));
        request.addInserts(sample4);

        request.addDelete(sample5.get(0));
        request.addDeletes(sample6);

        Assert.assertEquals(Arrays.asList(
                ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE,
                ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE, ERowModificationType.RMT_WRITE,
                ERowModificationType.RMT_DELETE, ERowModificationType.RMT_DELETE, ERowModificationType.RMT_DELETE),
                request.getRowModificationTypes());

        final List<SmallObjectClass> all = new ArrayList<>();
        all.addAll(sample1);
        all.addAll(sample2);
        all.addAll(sample3);
        all.addAll(sample4);
        all.addAll(sample5);

        // Сохраняем только ключи (проверяем, что объект будет корректи сериализован)
        sample6.forEach(s -> {
            final SmallObjectClass key = new SmallObjectClass();
            key.setIntField(s.getIntField());
            all.add(key);
        });

        final List<byte[]> expect = metadata.serializeMappedObjects(all, i -> i >= 6);

        final List<byte[]> actual = new ArrayList<>();
        request.serializeRowsetTo(actual);

        Assert.assertArrayEquals(WireProtocolTest.mergeChunks(expect), WireProtocolTest.mergeChunks(actual));
    }
}
