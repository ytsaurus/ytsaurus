package ru.yandex.yt.ytclient.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.TableWriter;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

public class WriteTableExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteTableExample.class);

    public static void main(String[] args) {
        try {
            logger.debug("Starting");
            mainUnsafe(args);
        } catch (Throwable e) {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private static TableSchema createSchema(String randomColumnName) {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("key", ColumnValueType.STRING);
        builder.addValue("value", ColumnValueType.STRING);
        builder.addValue("int", ColumnValueType.INT64);

        if (randomColumnName != null) {
            builder.addValue(randomColumnName, ColumnValueType.STRING);
        }

        return builder.build().toWrite();
    }

    private static long currentRowNumber = 0;
    private static Random random = new Random();

    private static void resetGenerator() {
        currentRowNumber = 0;
    }

    private static UnversionedRowset nextRows() {
        if (currentRowNumber >= 100000) {
            return null;
        }

        List<UnversionedRow> rows = Cf.arrayList();

        String randomColumnName = "column-" + random.nextInt(10);
        TableSchema schema = createSchema(randomColumnName);

        for (int i = 0; i < 10; ++i) {
            String key = "key-" + currentRowNumber;
            String value = "value-" + currentRowNumber;
            String randomValue = "rnd-" + currentRowNumber;
            Long integer = currentRowNumber;

            List<?> values = List.of(key, value, integer, randomValue);
            List<UnversionedValue> row = new ArrayList<>(values.size());

            ApiServiceUtil.convertValueColumns(row, schema, values, true, false);
            rows.add(new UnversionedRow(row));

            currentRowNumber += 1;
        }

        return new UnversionedRowset(schema, rows);
    }

    private static void mainUnsafe(String[] args) {
        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Write table");

                String path = "//tmp/write-table-example-1";

                client.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

                TableWriter writer = client.writeTable(new WriteTable<>(path, new UnversionedRowSerializer())).join();

                resetGenerator();

                UnversionedRowset rowset = nextRows();

                while (rowset != null) {
                    while (rowset != null && writer.write(rowset.getRows(), rowset.getSchema())) {
                        rowset = nextRows();
                    }

                    writer.readyEvent().join();
                }

                writer.close().join();

            } catch (Throwable ex) {
                logger.error("Error -> {}", ex);
                System.exit(-1);
            }
        });

        ExamplesUtil.enableCompression();

        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Write table");

                String path = "//tmp/write-table-example-2";

                client.createNode(new CreateNode(path, ObjectType.Table).setForce(true)).join();

                TableWriter writer = client.writeTable(new WriteTable<>(path, new UnversionedRowSerializer())).join();

                resetGenerator();

                UnversionedRowset rowset = nextRows();

                while (rowset != null) {
                    while (rowset != null && writer.write(rowset.getRows(), rowset.getSchema())) {
                        rowset = nextRows();
                    }

                    writer.readyEvent().join();
                }

                writer.close().join();

            } catch (Throwable ex) {
                logger.error("Error -> {}", ex);
                System.exit(-1);
            }
        });
    }
}
