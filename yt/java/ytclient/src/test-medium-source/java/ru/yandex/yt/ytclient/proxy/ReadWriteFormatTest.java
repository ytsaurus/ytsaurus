package ru.yandex.yt.ytclient.proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentYsonReader;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.Format;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.request.ReadTable;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

@RunWith(value = Parameterized.class)
public class ReadWriteFormatTest {
    YtClient yt;

    public ReadWriteFormatTest(YtClient yt) {
        this.yt = yt;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0), true);
        String localProxy = System.getenv("YT_PROXY");

        YtClient yt = new YtClient(
                connector,
                List.of(new YtCluster(localProxy)),
                "default",
                null,
                new RpcCredentials("root", ""),
                new RpcCompression().setRequestCodecId(Compression.None),
                new RpcOptions());

        YtClient ytWithCompression = new YtClient(
                connector,
                List.of(new YtCluster(localProxy)),
                "default",
                null,
                new RpcCredentials("root", ""),
                new RpcCompression().setRequestCodecId(Compression.Zlib_6),
                new RpcOptions());

        yt.waitProxies().join();

        ytWithCompression.waitProxies().join();

        return List.of(
                new Object[]{yt},
                new Object[]{ytWithCompression});
    }

    static class RowsGenerator {
        private long currentRowNumber = 0;

        private final TableSchema schema = createSchema();

        static TableSchema createSchema() {
            TableSchema.Builder builder = new TableSchema.Builder();
            builder.setUniqueKeys(false);
            builder.addValue("key", ColumnValueType.STRING);
            builder.addValue("value", ColumnValueType.STRING);
            builder.addValue("int", ColumnValueType.INT64);
            return builder.build().toWrite();
        }

        UnversionedRowset nextRows() {
            if (currentRowNumber >= 1000) {
                return null;
            }

            List<UnversionedRow> rows = new ArrayList<>();

            for (int i = 0; i < 10; ++i) {
                String key = "key-" + currentRowNumber;
                String value = "value-" + currentRowNumber;
                Long integer = currentRowNumber;

                List<?> values = List.of(key, value, integer);
                List<UnversionedValue> row = new ArrayList<>(values.size());

                ApiServiceUtil.convertValueColumns(row, schema, values, true, false,
                        YandexSerializationResolver.getInstance());
                rows.add(new UnversionedRow(row));

                currentRowNumber += 1;
            }

            return new UnversionedRowset(schema, rows);
        }

        long rowsCount() {
            return currentRowNumber;
        }

        void reset() {
            currentRowNumber = 0;
        }
    }

    @Test
    public void testYsonFormatRead() throws Exception {
        RowsGenerator generator = new RowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-1");

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        TableWriter<UnversionedRow> writer =
                yt.writeTable(new WriteTable<>(path, new UnversionedRowSerializer())).join();

        UnversionedRowset rowset = generator.nextRows();

        while (rowset != null) {
            while (rowset != null && writer.write(rowset.getRows(), rowset.getSchema())) {
                rowset = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<YTreeNode> reader = yt.readTable(
                new ReadTable<YTreeNode>(path, new ReadTable.SerializationContext<>(Format.ysonBinary())),
                /*new ReadTable<>(path, (WireRowDeserializer<YTreeNode>) null)
                        .setDesiredRowsetFormat(ERowsetFormat.RF_FORMAT)
                        .setFormat(Format.ysonBinary()),*/
                new TableAttachmentYsonReader()
        ).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        List<YTreeNode> rows;
        while (reader.canRead()) {
            while ((rows = reader.read()) != null) {
                for (YTreeNode row : rows) {
                    Map<String, YTreeNode> mapRow = row.asMap();
                    long intValue = mapRow.get("int").longValue();

                    Assert.assertEquals(row.toString(), mapRow.get("key").stringValue(), "key-" + intValue);
                    Assert.assertEquals(row.toString(), mapRow.get("value").stringValue(), "value-" + intValue);

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
    public void testYsonFormatWrite() throws Exception {
        YPath path = YPath.simple("//tmp/write-table-example-1");

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        TableWriter<YTreeMapNode> writer =
                yt.writeTable(new WriteTable<>(
                        path,
                        YTreeObjectSerializerFactory.forClass(YTreeMapNode.class),
                        Format.ysonBinary()
                )).join();

        List<YTreeMapNode> rows = List.of(
                YTree.builder().beginMap()
                        .key("key").value("key-0")
                        .key("int").value(0)
                        .endMap().build().mapNode(),
                YTree.builder().beginMap()
                        .key("key").value("key-1")
                        .key("int").value(1)
                        .endMap().build().mapNode()
        );

        writer.write(rows, TableSchema.newBuilder().build());

        writer.readyEvent().join();

        rows = List.of(
                YTree.builder().beginMap()
                        .key("key").value("key-2")
                        .key("value").value("value-2")
                        .key("int").value(2)
                        .endMap().build().mapNode(),
                YTree.builder().beginMap()
                        .key("key").value("key-3")
                        .key("value").value("value-3")
                        .key("int").value(3)
                        .endMap().build().mapNode()
        );

        writer.write(rows, TableSchema.newBuilder().build());

        writer.close().join();

        TableReader<YTreeMapNode> reader = yt.readTable(new ReadTable<>(
                path,
                new ReadTable.SerializationContext<>(YTreeObjectSerializerFactory.forClass(YTreeMapNode.class)))
        ).join();

        int currentRowNumber = 0;

        while (reader.canRead()) {
            while ((rows = reader.read()) != null) {
                for (YTreeMapNode row : rows) {
                    Map<String, YTreeNode> mapRow = row.asMap();
                    long intValue = mapRow.get("int").longValue();
                    Assert.assertEquals(currentRowNumber, intValue);
                    Assert.assertEquals(row.toString(), mapRow.get("key").stringValue(), "key-" + intValue);
                    if (intValue >= 2) {
                        Assert.assertEquals(row.toString(), mapRow.get("value").stringValue(), "value-" + intValue);
                    } else {
                        Assert.assertFalse(mapRow.containsKey("value"));
                    }
                    currentRowNumber += 1;
                }
            }
            reader.readyEvent().join();
        }

        Assert.assertEquals(4, currentRowNumber);

        reader.close().join();
    }
}
