package tech.ytsaurus.client;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadSerializationContext;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteSerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowDeserializer;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import ru.yandex.yt.ytclient.proxy.YandexSerializationResolver;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;

@RunWith(value = Parameterized.class)
public class ReadWriteTest extends ReadWriteTestBase {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    public ReadWriteTest(YtClient yt) {
        super(yt);
    }

    @Test
    public void testFileReadWrite() throws Exception {
        String path = "//tmp/bigfile1";

        yt.createNode(new CreateNode(path, CypressNodeType.FILE).setForce(true)).join();

        Random random = new Random();

        MessageDigest md = MessageDigest.getInstance("MD5");

        int currentCounter = COUNTER.getAndIncrement();

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

    @Test
    public void testTableReadWrite() throws Exception {
        RowsGenerator generator = new RowsGenerator();

        YPath path = YPath.simple("//tmp/write-table-example-1");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        TableWriter<UnversionedRow> writer =
                yt.writeTable(new WriteTable<>(
                        path,
                        new WriteSerializationContext<>(new UnversionedRowSerializer()))).join();

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
                        path, new ReadSerializationContext<>(new UnversionedRowDeserializer()))).join();

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

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        UnversionedRowset rowset = generator.nextRows();

        AsyncWriter<UnversionedRow> writer = yt.writeTableV2(
                new WriteTable<>(path, new SerializationContext<>(UnversionedRow.class), rowset.getSchema())
        ).join();

        while (rowset != null) {
            writer.write(rowset.getRows()).join();
            rowset = generator.nextRows();
        }

        writer.finish().join();

        AsyncReader<UnversionedRow> reader =
                yt.readTableV2(new ReadTable<>(
                        path,
                        new ReadSerializationContext<>(new UnversionedRowDeserializer()))).join();

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

}
