package tech.ytsaurus.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.ETransactionType;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.yt.ytclient.proxy.request.CreateNode;

public class RpcBackendTest {
    private YtClient yt;

    @Before
    public void setup() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("yt_proxy_port.txt"));
        final int proxyPort = Integer.valueOf(br.readLine());
        // final int proxyPort = 80;

        final BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        final String host = "localhost";

        final String user = "root";
        final String token = "";

        yt = new YtClient(
                connector,
                List.of(new YtCluster("local", host, proxyPort)),
                "local",
                null,
                YTsaurusClientAuth.builder()
                        .setUser(user)
                        .setToken(token)
                        .build(),
                new RpcCompression(Compression.Zlib_6),
                new RpcOptions()
        );

        yt.waitProxies().join();
    }

    @Test
    public void testCompression() {
        String path = "//tmp/test_compression" + UUID.randomUUID().toString();

        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        Map<String, YTreeNode> attributes = new HashMap<>();

        attributes.put("dynamic", new YTreeBuilder().value(true).build());
        attributes.put("schema", schema.toYTree());

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE, attributes)).join();
        yt.mountTable(path).join();

        while (true) {
            String state = yt.getNode(path + "/@tablet_state").join().stringValue();
            if (state.equals("mounted")) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Throwable e) {
            }
        }

        ModifyRowsRequest req = new ModifyRowsRequest(path, schema)
                .addInsert(Arrays.asList("key1", "value1"))
                .addInsert(Arrays.asList("key2", "value2"));

        ApiServiceTransactionOptions transactionOptions =
                new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                        .setSticky(true);
        ApiServiceTransaction t = yt.startTransaction(transactionOptions).join();

        t.modifyRows(req).join();
        t.commit().join();

        UnversionedRowset rows = yt.selectRows(String.format("* from [%s]", path)).join();

        Assert.assertTrue(
                rows.getYTreeRows().toString().equals("[{\"value\"=\"value1\";\"key\"=\"key1\";}, " +
                        "{\"value\"=\"value2\";\"key\"=\"key2\";}]") ||
                        rows.getYTreeRows().toString().equals("[{\"value\"=\"value2\";\"key\"=\"key2\";}, " +
                                "{\"value\"=\"value1\";\"key\"=\"key1\";}]"));
    }
}
