package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

public class SelectRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsExample.class);

    public static void main(String[] args) {
        ExamplesUtil.runExampleWithBalancing(client -> {
            long t0 = System.nanoTime();
            UnversionedRowset rowset = client.selectRows(
                    "timestamp, host, rack, utc_time, data FROM [//home/dev/andozer/autorestart_nodes_copy] LIMIT 10")
                    .join();
            long t1 = System.nanoTime();
            logger.info("Request time: {}", (t1 - t0) / 1000000.0);
            logger.info("Result schema:");
            for (ColumnSchema column : rowset.getSchema().getColumns()) {
                logger.info("    {}", column.getName());
            }
            for (UnversionedRow row : rowset.getRows()) {
                logger.info("Row:");
                for (UnversionedValue value : row.getValues()) {
                    logger.info("    value: {}", value);
                }
            }
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }
        });
    }
}
