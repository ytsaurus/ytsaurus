package ru.yandex.yt.ytclient.examples;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.ModifyRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.ytree.YTreeBuilder;
import ru.yandex.yt.rpcproxy.ETransactionType;

public class ModifyRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(ModifyRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("dbname", ColumnValueType.STRING)
                .addValue("server_schema", ColumnValueType.STRING)
                .addValue("is_imported", ColumnValueType.BOOLEAN)
                .build();
        ExamplesUtil.runExample(client -> {
            ApiServiceTransactionOptions transactionOptions =
                    new ApiServiceTransactionOptions(ETransactionType.MASTER)
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
                        new ModifyRowsRequest("//home/direct/tmp/snaury/dyn/mysql-sync-states", schema)
                                .addInsert(Arrays.asList("foobar:1", "my schema", false))
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("dbname").value("foobar:1")
                                        .key("is_imported").value(true)
                                        .buildMap()
                                        .mapValue())
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("dbname").value("foobar:2")
                                        .key("server_schema").value("some foobar:2 schema")
                                        .buildMap()
                                        .mapValue());
                long t0 = System.nanoTime();
                transaction.modifyRows(request).join();
                long t1 = System.nanoTime();
                logger.info("Request time: {}ms", (t1 - t0) / 1000000.0);

                transaction.commit().join();
                logger.info("Transaction committed!");
            }
        });
    }
}
