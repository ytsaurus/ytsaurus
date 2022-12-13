package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcCredentials;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.type_info.TiType;
import tech.ytsaurus.ysontree.YTree;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.ytclient.proxy.MappedModifyRowsRequest;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.MountTable;

import static org.hamcrest.MatcherAssert.assertThat;

public class MultiYtClientIntegrationTest {
    static final TableSchema KEY_VALUE_TABLE_SCHEMA = TableSchema.builderWithUniqueKeys()
            .addKey("key", TiType.string())
            .addValue("value", TiType.int64())
            .build();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    YTreeObjectSerializer<KeyValue> serializer =
            new YTreeObjectSerializer<>(MultiYtClientIntegrationTest.KeyValue.class);

    OutageController outageControllerOne = new OutageController();
    OutageController outageControllerTwo = new OutageController();

    YtClient clientOne = YtClient.builder()
            .setCluster(System.getenv("YT_PROXY_ONE"))
            .setRpcCredentials(new RpcCredentials("root", ""))
            .setRpcOptions(new RpcOptions().setTestingOptions(
                    new TestingOptions().setOutageController(outageControllerOne)))
            .build();

    YtClient clientTwo = YtClient.builder()
            .setCluster(System.getenv("YT_PROXY_TWO"))
            .setRpcCredentials(new RpcCredentials("root", ""))
            .setRpcOptions(new RpcOptions().setTestingOptions(
                    new TestingOptions().setOutageController(outageControllerTwo)))
            .build();

    String tablePath = "//some-table";

    @Test
    public void testBasic() {
        init();

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(clientOne).build(),
                        MultiYtClient.YtClientOptions.builder(clientTwo)
                                .setInitialPenalty(Duration.ofSeconds(1)).build()
                )
                .setBanDuration(Duration.ofMillis(200))
                .setBanPenalty(Duration.ofMillis(100))
                .build();

        List<KeyValue> res = multiClient.lookupRows(
                new LookupRowsRequest(tablePath, KEY_VALUE_TABLE_SCHEMA.toLookup()).addFilter("foo"), serializer
        ).join();

        assertThat("Response from cluster One", res.get(0).value == 1);

        outageControllerOne.addDelays("LookupRows", 3, Duration.ofSeconds(3));

        res = multiClient.lookupRows(
                new LookupRowsRequest(tablePath, KEY_VALUE_TABLE_SCHEMA.toLookup()).addFilter("foo"), serializer
        ).join();

        assertThat("Response from cluster Two", res.get(0).value == 2);
    }

    private void init() {
        clientOne.createNode(new CreateNode(tablePath, CypressNodeType.TABLE)
                .addAttribute("dynamic", YTree.booleanNode(true))
                .addAttribute("schema", KEY_VALUE_TABLE_SCHEMA.toYTree())
        ).join();

        clientTwo.createNode(new CreateNode(tablePath, CypressNodeType.TABLE)
                .addAttribute("dynamic", YTree.booleanNode(true))
                .addAttribute("schema", KEY_VALUE_TABLE_SCHEMA.toYTree())
        ).join();

        clientOne.mountTableAndWaitTablets(new MountTable(YPath.simple(tablePath))).join();
        clientTwo.mountTableAndWaitTablets(new MountTable(YPath.simple(tablePath))).join();

        clientOne.retryWithTabletTransaction(
                tx -> {
                    var modifyRows = new MappedModifyRowsRequest<>(tablePath, serializer);
                    modifyRows.addInsert(new KeyValue("foo", 1));

                    return tx.modifyRows(modifyRows);
                },
                executor,
                RetryPolicy.noRetries()
        ).join();

        clientTwo.retryWithTabletTransaction(
                tx -> {
                    var modifyRows = new MappedModifyRowsRequest<>(tablePath, serializer);
                    modifyRows.addInsert(new KeyValue("foo", 2));

                    return tx.modifyRows(modifyRows);
                },
                executor,
                RetryPolicy.noRetries()
        ).join();
    }

    @YTreeObject
    static class KeyValue {
        @YTreeKeyField
        String key;

        int value;

        KeyValue(String key, int value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyValue keyValue = (KeyValue) o;
            return value == keyValue.value && Objects.equals(key, keyValue.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "KeyValue{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    '}';
        }
    }

}
