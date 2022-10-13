package ru.yandex.yt.ytclient.proxy;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.UnversionedRowDeserializer;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.request.ReadTable;
import ru.yandex.yt.ytclient.request.WriteTable;
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
public class ReadWriteTest {
    YtClient yt;

    private static final AtomicInteger counter = new AtomicInteger(0);

    public ReadWriteTest(YtClient yt) {
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

    private String getDigestString(MessageDigest md) {
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
        }

        return sb.toString();
    }

    @Test
    public void testFileReadWrite() throws Exception {
        String path = "//tmp/bigfile1";

        yt.createNode(new CreateNode(path, ObjectType.File).setForce(true)).join();

        Random random = new Random();

        MessageDigest md = MessageDigest.getInstance("MD5");

        int currentCounter = counter.getAndIncrement();

        String testDataFile = "test-" + currentCounter + ".bin";

        Base64.Encoder base64Encoder = Base64.getEncoder();

        FileOutputStream fo = new FileOutputStream(testDataFile);
        byte[] fileData = new byte[1024];
        for (int i = 0; i < 100 * 1024; ++i) {
            random.nextBytes(fileData);

            byte[] readableData = base64Encoder.encode(fileData);

            fo.write(readableData);
            fo.write("\n".getBytes());

            md.update(readableData);
            md.update("\n".getBytes());
        }
        fo.close();


        String localMd5 = getDigestString(md);

        FileWriter writer = yt.writeFile(new WriteFile(path)
                .setWindowSize(10000000L)
                .setPacketSize(1000000L)
                .setComputeMd5(true)
        ).join();

        FileInputStream fi = new FileInputStream(testDataFile);

        byte[] data = new byte[40960];

        int size = fi.read(data);
        while (size > 0) {
            while (size > 0 && writer.write(data, 0, size)) {
                size = fi.read(data);
            }

            writer.readyEvent().join();
        }
        writer.close().join();
        fi.close();

        String remoteMd5 = yt.getNode(path + "/@md5").join().stringValue();

        Assert.assertEquals(localMd5, remoteMd5);

        FileReader reader = yt.readFile(new ReadFile(path)).join();

        md.reset();

        String testReadFile = testDataFile + ".out";
        fo = new FileOutputStream(testReadFile);

        while (reader.canRead()) {
            while ((data = reader.read()) != null) {
                md.update(data);

                fo.write(data);
            }

            reader.readyEvent().join();
        }

        fo.close();

        reader.close().join();

        String readMd5 = getDigestString(md);

        Assert.assertEquals(localMd5, readMd5);
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
            if (currentRowNumber >= 10000) {
                return null;
            }

            List<UnversionedRow> rows = new ArrayList<>();

            for (int i = 0; i < 10; ++i) {
                String key = "key-" + currentRowNumber;
                String value = "value-" + currentRowNumber;
                Long integer = currentRowNumber;

                List<?> values = List.of(key, value, integer);
                List<UnversionedValue> row = new ArrayList<>(values.size());

                ApiServiceUtil.convertValueColumns(
                        row, schema, values, true, false,
                        YandexSerializationResolver.getInstance());
                rows.add(new UnversionedRow(row));

                currentRowNumber += 1;
            }

            return new UnversionedRowset(schema, rows);
        }

        long rowsCount() {
            return currentRowNumber;
        }
    }

    @YTreeObject
    static class Row {
        final String key;
        final String value;
        final long intValue;

        Row(String key, String value, long intValue) {
            this.key = key;
            this.value = value;
            this.intValue = intValue;
        }
    }

    static class ObjectsRowsGenerator {
        private long currentRowNumber = 0;

        List<Row> nextRows() {
            if (currentRowNumber >= 10000) {
                return null;
            }

            List<Row> rows = new ArrayList<>();

            for (int i = 0; i < 10; ++i) {
                String key = "key-" + currentRowNumber;
                String value = "value-" + currentRowNumber;

                rows.add(new Row(key, value, currentRowNumber));

                currentRowNumber += 1;
            }

            return rows;
        }

        long rowsCount() {
            return currentRowNumber;
        }
    }

    @Test
    public void testTableReadWrite() throws Exception {
        RowsGenerator generator = new RowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-1");

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        TableWriter<UnversionedRow> writer =
                yt.writeTable(new WriteTable<>(
                        path,
                        new WriteTable.SerializationContext<>(new UnversionedRowSerializer()))).join();

        UnversionedRowset rowset = generator.nextRows();

        while (rowset != null) {
            while (rowset != null && writer.write(rowset.getRows(), rowset.getSchema())) {
                rowset = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<UnversionedRow> reader =
                yt.readTable(new ReadTable<>(
                        path, new ReadTable.SerializationContext<>(new UnversionedRowDeserializer()))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        List<UnversionedRow> rows;
        while (reader.canRead()) {
            while ((rows = reader.read()) != null) {
                for (UnversionedRow row : rows) {
                    List<UnversionedValue> values = row.getValues();
                    Assert.assertEquals(row.toString(), values.size(), 3);

                    Assert.assertEquals(row.toString(), values.get(0).getType(), ColumnValueType.STRING);
                    Assert.assertEquals(row.toString(), values.get(1).getType(), ColumnValueType.STRING);
                    Assert.assertEquals(row.toString(), values.get(2).getType(), ColumnValueType.INT64);

                    long intValue = values.get(2).longValue();

                    Assert.assertEquals(row.toString(), values.get(0).stringValue(), "key-" + intValue);
                    Assert.assertEquals(row.toString(), values.get(1).stringValue(), "value-" + intValue);

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
    public void testTableReadWriteV2() {
        RowsGenerator generator = new RowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-1");

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        UnversionedRowset rowset = generator.nextRows();

        AsyncWriter<UnversionedRow> writer = yt.writeTableV2(
                new WriteTable<>(path, new WriteTable.SerializationContext<>(UnversionedRow.class), rowset.getSchema())
        ).join();

        while (rowset != null) {
            writer.write(rowset.getRows()).join();
            rowset = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<UnversionedRow> reader =
                yt.readTableV2(new ReadTable<>(
                        path,
                        new ReadTable.SerializationContext<>(new UnversionedRowDeserializer()))).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        List<UnversionedRow> rows;
        while ((rows = reader.next().join()) != null) {
            for (UnversionedRow row : rows) {
                List<UnversionedValue> values = row.getValues();
                Assert.assertEquals(row.toString(), values.size(), 3);

                Assert.assertEquals(row.toString(), values.get(0).getType(), ColumnValueType.STRING);
                Assert.assertEquals(row.toString(), values.get(1).getType(), ColumnValueType.STRING);
                Assert.assertEquals(row.toString(), values.get(2).getType(), ColumnValueType.INT64);

                long intValue = values.get(2).longValue();

                Assert.assertEquals(row.toString(), values.get(0).stringValue(), "key-" + intValue);
                Assert.assertEquals(row.toString(), values.get(1).stringValue(), "value-" + intValue);

                Assert.assertEquals(rowsSeen.get(currentRowNumber), false);
                rowsSeen.set(currentRowNumber, true);

                currentRowNumber += 1;
            }
        }

        for (int i = 0; i < rowsSeen.size(); ++i) {
            Assert.assertTrue("row #" + i + " wasn't seen", rowsSeen.get(i));
        }
    }

    @Test
    public void testTableObjectReadWrite() throws Exception {
        ObjectsRowsGenerator generator = new ObjectsRowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-2");

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        TableWriter<Row> writer = yt.writeTable(new WriteTable<>(path,
                new WriteTable.SerializationContext<>(YTreeObjectSerializerFactory.forClass(Row.class)))).join();

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
                        new ReadTable.SerializationContext<>(
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

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        AsyncWriter<Row> writer = yt.writeTableV2(new WriteTable<>(
                path, new WriteTable.SerializationContext<>(YTreeObjectSerializerFactory.forClass(Row.class)))).join();

        List<Row> rows = generator.nextRows();

        while (rows != null) {
            writer.write(rows).join();
            rows = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<Row> reader = yt.readTableV2(
                new ReadTable<>(
                        path,
                        new ReadTable.SerializationContext<>(
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

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        YTreeSerializer<YTreeMapNode> serializer = YTreeObjectSerializerFactory.forClass(YTreeMapNode.class);

        TableWriter<YTreeMapNode> writer = yt.writeTable(new WriteTable<>(
                path, new WriteTable.SerializationContext<>(serializer))).join();

        List<YTreeMapNode> rows = generator.nextRows();

        while (rows != null) {
            while (rows != null && writer.write(rows, generator.getSchema())) {
                rows = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<YTreeMapNode> reader = yt.readTable(new ReadTable<>(
                path,new ReadTable.SerializationContext<>(serializer))).join();

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

        yt.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

        YTreeSerializer<YTreeMapNode> serializer = YTreeObjectSerializerFactory.forClass(YTreeMapNode.class);

        List<YTreeMapNode> rows = generator.nextRows();

        AsyncWriter<YTreeMapNode> writer = yt.writeTableV2(new WriteTable<>(
                path,
                new WriteTable.SerializationContext<>(MappedRowSerializer.forClass(serializer)),
                generator.getSchema())
        ).join();

        while (rows != null) {
            writer.write(rows).join();
            rows = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<YTreeMapNode> reader = yt.readTableV2(new ReadTable<>(
                path, new ReadTable.SerializationContext<>(serializer))).join();

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
