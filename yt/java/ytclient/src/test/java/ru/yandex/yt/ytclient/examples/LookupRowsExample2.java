package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

public class LookupRowsExample2 {
    private static final Logger logger = LoggerFactory.getLogger(LookupRowsExample2.class);

    public static void main(String[] args) {
        ExamplesUtil.runExample(client -> {
            TableSchema schema = new TableSchema.Builder()
                    .addKey("key", ColumnValueType.STRING)
                    .addValue("value", ColumnValueType.STRING)
                    .build();
            LookupRowsRequest request = new LookupRowsRequest(
                    "//home/direct/tmp/snaury/dyn-key-value", schema.toLookup())
                    .addFilter("00207142481476964638")
                    .addLookupColumns("key", "value");
            UnversionedRowset rowset = client.lookupRows(request).join();
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }
        });
    }
}
