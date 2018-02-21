package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.VersionedRow;
import ru.yandex.yt.ytclient.wire.VersionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedValue;

public class VersionedLookupRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(VersionedLookupRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("ClientID", ColumnValueType.UINT64)
                .addKey("cid", ColumnValueType.UINT64)
                .addKey("GroupExportID", ColumnValueType.UINT64)
                .addKey("PhraseID", ColumnValueType.UINT64)
                .addKey("OrderID", ColumnValueType.UINT64)
                .addKey("UpdateTime", ColumnValueType.INT64)
                .build();
        ExamplesUtil.runExample(client -> {
            long t0 = System.nanoTime();
            LookupRowsRequest request = new LookupRowsRequest("//yabs/GPStat3.dynamic", schema)
                    .addFilter(8102567, 16145160, 1158063745, 844923, 7590772, 1452978000)
                    .addFilter(8139356, 16270816, 1472956303, 105660307, 9058766, 1461963600)
                    .addFilter(2317001, 15108415, 1011501759, 169044313, 6853052, 1448571600)
                    .addLookupColumns("OrderID", "UpdateTime", "ClientID", "Shows", "Clicks");
            long t1 = System.nanoTime();
            VersionedRowset rowset = client.versionedLookupRows(request).join();
            long t2 = System.nanoTime();
            logger.info("Request time: {}ms + {}ms", (t1 - t0) / 1000000.0, (t2 - t1) / 1000000.0);
            logger.info("Result schema:");
            for (ColumnSchema column : rowset.getSchema().getColumns()) {
                logger.info("    {}", column.getName());
            }
            for (VersionedRow row : rowset.getRows()) {
                logger.info("Row:");
                for (UnversionedValue key : row.getKeys()) {
                    logger.info("    key: {}", key);
                }
                for (VersionedValue value : row.getValues()) {
                    logger.info("    value: {}", value);
                }
            }
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }
        });
    }
}
