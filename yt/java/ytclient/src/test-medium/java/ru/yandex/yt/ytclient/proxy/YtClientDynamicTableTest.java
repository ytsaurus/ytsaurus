package ru.yandex.yt.ytclient.proxy;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class YtClientDynamicTableTest extends YtClientTestBase {
    static final TableSchema keyValueTableSchema = TableSchema.builder()
            .setUniqueKeys(true).setStrict(true)
            .addKey("key", ColumnValueType.INT64)
            .addValue("value", ColumnValueType.STRING)
            .build();

    private YPath keyValueTablePath;
    private YtClient yt;

    @Before
    public void setUpTables() {
        var ytFixture = createYtFixture();
        yt = ytFixture.yt;

        keyValueTablePath = ytFixture.testDirectory.child("key-value-table");
        yt.createNode(
                new CreateNode(keyValueTablePath, ObjectType.Table)
                        .setRecursive(true)
                        .setForce(true)
                        .setAttributes(
                                Map.of(
                                        "dynamic", YTree.booleanNode(true),
                                        "schema", keyValueTableSchema.toYTree()
                                )
                        )
        ).join();
        yt.mountTable(keyValueTablePath.toString(), null, false, true).join();
    }

    @Test(timeout = 10000)
    public void testBanningProxyHoldingTransaction() {
        // YT-13770: if proxy is banned while we are holding transaction
        // we should not release or close client until working with transaction is done
        var tx = yt.startTransaction(
                new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                .setSticky(true)
        ).join();

        var txProxyAddress = tx.getRpcProxyAddress();
        {
            yt.banProxy(txProxyAddress).join();
        }

        tx.commit().join();
    }
}
