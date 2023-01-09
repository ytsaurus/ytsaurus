package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;


@RunWith(value = Parameterized.class)
public class ReadWriteObjectTest extends ReadWriteTestBase {
    public ReadWriteObjectTest(YtClient yt) {
        super(yt);
    }

    @Test
    public void testTableObjectReadWrite() throws Exception {
        ObjectsRowsGenerator generator = new ObjectsRowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-2");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        TableWriter<Row> writer = yt.writeTable(new WriteTable<>(path,
                new SerializationContext<>(YTreeObjectSerializerFactory.forClass(Row.class)))).join();

        List<Row> rows = generator.nextRows();

        while (rows != null) {
            while (rows != null && writer.write(rows)) {
                rows = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<Row> reader = yt.readTable(
                new ReadTable<>(
                        path,
                        new SerializationContext<>(
                                (YTreeObjectSerializer<Row>) YTreeObjectSerializerFactory.forClass(Row.class)))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        while (reader.canRead()) {
            List<Row> rowset;

            while ((rowset = reader.read()) != null) {
                for (Row row : rowset) {
                    long intValue = row.intValue;

                    Assert.assertEquals(row.toString(), row.key, "key-" + intValue);
                    Assert.assertEquals(row.toString(), row.value, "value-" + intValue);

                    Assert.assertEquals(rowsSeen.get(currentRowNumber), false);
                    rowsSeen.set(currentRowNumber, true);

                    currentRowNumber += 1;
                }
            }
            reader.readyEvent().join();
        }

        reader.close().join();

        for (int i = 0; i < rowsSeen.size(); ++i) {
            Assert.assertTrue("row #" + i + " wasn't seen", rowsSeen.get(i));
        }
    }


    @Test
    public void testTableObjectReadWriteV2() {
        ObjectsRowsGenerator generator = new ObjectsRowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-2");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        AsyncWriter<Row> writer = yt.writeTableV2(new WriteTable<>(
                path, new SerializationContext<>(YTreeObjectSerializerFactory.forClass(Row.class)))).join();

        List<Row> rows = generator.nextRows();

        while (rows != null) {
            writer.write(rows).join();
            rows = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<Row> reader = yt.readTableV2(
                new ReadTable<>(
                        path,
                        new SerializationContext<>(
                                (YTreeObjectSerializer<Row>) YTreeObjectSerializerFactory.forClass(Row.class)))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        final List<Row> rowset = new ArrayList<>();
        var executor = Executors.newSingleThreadExecutor();
        reader.acceptAllAsync(rowset::add, executor).join();

        for (Row row : rowset) {
            long intValue = row.intValue;

            Assert.assertEquals(row.toString(), row.key, "key-" + intValue);
            Assert.assertEquals(row.toString(), row.value, "value-" + intValue);

            Assert.assertEquals(rowsSeen.get(currentRowNumber), false);
            rowsSeen.set(currentRowNumber, true);

            currentRowNumber += 1;
        }

        for (int i = 0; i < rowsSeen.size(); ++i) {
            Assert.assertTrue("row #" + i + " wasn't seen", rowsSeen.get(i));
        }
    }
}
