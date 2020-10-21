package ru.yandex.yt.ytclient.proxy;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class YtClientDynamicTableTest {
    static final TableSchema keyValueTableSchema = TableSchema.builder()
            .setUniqueKeys(true).setStrict(true)
            .addKey("key", ColumnValueType.INT64)
            .addValue("value", ColumnValueType.STRING)
            .build();

    @Rule
    public TestName name = new TestName();

    private YtClient yt;
    private YPath testDirectory;
    private YPath keyValueTablePath;

    @Before
    public void before() {
        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setNewDiscoveryServiceEnabled(true);

        yt = YtClient.builder()
                .setCluster(LocalYt.getAddress())
                .setRpcOptions(rpcOptions)
                .setRpcCredentials(new RpcCredentials("root", ""))
                .build();

        testDirectory = YPath.simple("//tmp/ytclient-test/" + name.getMethodName());

        yt.createNode(
                new CreateNode(testDirectory, ObjectType.MapNode)
                        .setRecursive(true)
                        .setForce(true)
        ).join();

        keyValueTablePath = YPath.simple(testDirectory + "/key-value-table");

        {
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
    }

    @After
    public void after() {
        if (yt == null) {
            return;
        }

        try (var ignored = yt) {
            yt.removeNode(new RemoveNode(testDirectory).setForce(true)).join();
        }
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
            int banned = yt.banProxy(txProxyAddress).join();
            assertThat(banned, greaterThan(0));
        }

        tx.commit().join();
    }
}
