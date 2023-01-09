package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteSerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.MappedRowSerializer;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;

@RunWith(value = Parameterized.class)
public class ReadWriteYTreeMapNodeTest extends ReadWriteTestBase {
    public ReadWriteYTreeMapNodeTest(YtClient yt) {
        super(yt);
    }

    static class YTreeMapNodeRowsGenerator {
        private long currentRowNumber = 0;
        private final TableSchema schema = createSchema();

        static TableSchema createSchema() {
            TableSchema.Builder builder = new TableSchema.Builder();
            builder.setUniqueKeys(false);
            builder.addValue("key", ColumnValueType.STRING);
            builder.addValue("value", ColumnValueType.STRING);
            builder.addValue("intValue", ColumnValueType.INT64);
            return builder.build().toWrite();
        }

        TableSchema getSchema() {
            return this.schema;
        }

        List<YTreeMapNode> nextRows() {
            if (currentRowNumber >= 10000) {
                return null;
            }

            List<YTreeMapNode> rows = new ArrayList<>();

            for (int i = 0; i < 10; ++i) {
                String key = "key-" + currentRowNumber;
                String value = "value-" + currentRowNumber;

                rows.add(
                        YTree.builder()
                                .beginMap()
                                .key("key").value(key)
                                .key("value").value(value)
                                .key("intValue").value(currentRowNumber)
                                .endMap()
                                .build().mapNode());

                currentRowNumber += 1;
            }

            return rows;
        }

        long rowsCount() {
            return currentRowNumber;
        }
    }

    @Test
    public void testTableYTreeMapNodeReadWrite() throws Exception {
        YTreeMapNodeRowsGenerator generator = new YTreeMapNodeRowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-3");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        YTreeSerializer<YTreeMapNode> serializer = YTreeObjectSerializerFactory.forClass(YTreeMapNode.class);

        TableWriter<YTreeMapNode> writer = yt.writeTable(new WriteTable<>(
                path, new SerializationContext<>(serializer))).join();

        List<YTreeMapNode> rows = generator.nextRows();

        while (rows != null) {
            while (rows != null && writer.write(rows, generator.getSchema())) {
                rows = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<YTreeMapNode> reader = yt.readTable(new ReadTable<>(
                path, new SerializationContext<>(serializer))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        while (reader.canRead()) {
            List<YTreeMapNode> rowset;

            while ((rowset = reader.read()) != null) {
                for (YTreeMapNode row : rowset) {
                    int intValue = row.getInt("intValue");
                    String key = "key-" + intValue;
                    String value = "value-" + intValue;

                    Assert.assertEquals(row.toString(), row.getString("key"), key);
                    Assert.assertEquals(row.toString(), row.getString("value"), value);

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
    public void testTableYTreeMapNodeReadWriteV2() {
        YTreeMapNodeRowsGenerator generator = new YTreeMapNodeRowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-3");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        YTreeSerializer<YTreeMapNode> serializer = YTreeObjectSerializerFactory.forClass(YTreeMapNode.class);

        List<YTreeMapNode> rows = generator.nextRows();

        AsyncWriter<YTreeMapNode> writer = yt.writeTableV2(new WriteTable<>(
                path,
                new WriteSerializationContext<>(MappedRowSerializer.forClass(serializer)),
                generator.getSchema())
        ).join();

        while (rows != null) {
            writer.write(rows).join();
            rows = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<YTreeMapNode> reader = yt.readTableV2(new ReadTable<>(
                path, new SerializationContext<>(serializer))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        List<YTreeMapNode> rowset;

        while ((rowset = reader.next().join()) != null) {
            for (YTreeMapNode row : rowset) {
                int intValue = row.getInt("intValue");
                String key = "key-" + intValue;
                String value = "value-" + intValue;

                Assert.assertEquals(row.toString(), row.getString("key"), key);
                Assert.assertEquals(row.toString(), row.getString("value"), value);

                Assert.assertEquals(rowsSeen.get(currentRowNumber), false);
                rowsSeen.set(currentRowNumber, true);

                currentRowNumber += 1;
            }
        }

        for (int i = 0; i < rowsSeen.size(); ++i) {
            Assert.assertTrue("row #" + i + " wasn't seen", rowsSeen.get(i));
        }
    }
}
