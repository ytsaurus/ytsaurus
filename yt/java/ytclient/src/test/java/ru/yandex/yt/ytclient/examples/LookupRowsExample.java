package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

public class LookupRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(LookupRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("timestamp", ColumnValueType.INT64)
                .addKey("host", ColumnValueType.STRING)
                .addKey("rack", ColumnValueType.STRING)
                .addValue("utc_time", ColumnValueType.STRING)
                .addValue("data", ColumnValueType.STRING)
                .build();
        logger.info("start");
        ExamplesUtil.runExampleWithBalancing(client -> {
            long t0 = System.nanoTime();
            LookupRowsRequest request = new LookupRowsRequest("//home/dev/andozer/autorestart_nodes_copy", schema.toLookup())
                    .addFilter(1486113922563016L, "s04-sas.hahn.yt.yandex.net", "SAS2.4.3-13")
                    .addFilter(1486113924172063L, "s04-sas.hahn.yt.yandex.net", "SAS2.4.3-13")
                    .addFilter(1486113992045484L, "s04-sas.hahn.yt.yandex.net", "SAS2.4.3-13")
                    .addFilter(1486113992591731L, "s04-sas.hahn.yt.yandex.net", "SAS2.4.3-13")
                    .addFilter(1486113997734536L, "n4137-sas.hahn.yt.yandex.net", "SAS2.4.3-13")
                    .addLookupColumns("utc_time", "data");
            long t1 = System.nanoTime();
            UnversionedRowset rowset = client.lookupRows(request).join();
            long t2 = System.nanoTime();
            logger.info("Request time: {}ms + {}ms", (t1 - t0) / 1000000.0, (t2 - t1) / 1000000.0);
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
