package ru.yandex.yt.ytclient.examples;

import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.ApiServiceTransactionOptions;
import tech.ytsaurus.client.LookupRowsRequest;
import tech.ytsaurus.client.ModifyRowsRequest;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.yt.rpcproxy.ETransactionType;

public class ModifyRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(ModifyRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("timestamp", ColumnValueType.INT64)
                .addKey("host", ColumnValueType.STRING)
                .addKey("rack", ColumnValueType.STRING)
                .addValue("utc_time", ColumnValueType.STRING)
                .addValue("data", ColumnValueType.STRING)
                .build();
        ExamplesUtil.runExampleWithBalancing(client -> {
            ApiServiceTransactionOptions transactionOptions =
                    new ApiServiceTransactionOptions(ETransactionType.TT_MASTER)
                            .setSticky(true);
            try (ApiServiceTransaction transaction = client.startTransaction(transactionOptions).join()) {
                logger.info("Transaction started: {} (timestamp={}, ping={}, sticky={})",
                        transaction.getId(),
                        transaction.getStartTimestamp(),
                        transaction.isPing(),
                        transaction.isSticky());

                transaction.ping().join();
                logger.info("Transaction ping succeeded!");

                ModifyRowsRequest request =
                        new ModifyRowsRequest("//home/dev/andozer/autorestart_nodes_copy", schema)
                                .addInsert(Arrays.asList(10, "myhost1", "myrack1", "utc_time1", "data1"))
                                .addInsert(Arrays.asList(11, "myhost2", "myrack2", "utc_time2", "data2"))
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("timestamp").value(1486190036109192L)
                                        .key("host").value("n0344-sas.hahn.yt.yandex.net")
                                        .key("rack").value("SAS2.4.3-15")
                                        .key("data").value("XXX " + UUID.randomUUID().toString())
                                        .buildMap().asMap())
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("timestamp").value(1486190037953802L)
                                        .key("host").value("s03-sas.hahn.yt.yandex.net")
                                        .key("rack").value("SAS2.4.3-15")
                                        .key("data").value("XXX " + UUID.randomUUID().toString())
                                        .buildMap().asMap());
                long t0 = System.nanoTime();
                transaction.modifyRows(request).join();
                long t1 = System.nanoTime();

                logger.info("Request time: {}ms", (t1 - t0) / 1000000.0);

                t0 = System.nanoTime();

                LookupRowsRequest lookup = new LookupRowsRequest("//home/dev/andozer/autorestart_nodes_copy", schema.toLookup())
                        .addFilter(1486190036109192L, "n0344-sas.hahn.yt.yandex.net", "SAS2.4.3-15")
                        .addFilter(1486190037953802L, "s03-sas.hahn.yt.yandex.net", "SAS2.4.3-15")
                        .addLookupColumns("timestamp", "data");


                t1 = System.nanoTime();
                UnversionedRowset rowset = client.lookupRows(lookup).join();
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

                transaction.commit().join();
                logger.info("Transaction committed!");
            }
        });
    }
}
