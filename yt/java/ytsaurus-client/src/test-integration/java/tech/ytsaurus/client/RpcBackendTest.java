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
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.ETransactionType;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

public class RpcBackendTest extends YTsaurusClientTestBase {
    private YTsaurusClient yt;

    @Before
    public void setup() throws IOException {
        final int proxyPort = localYTsaurus != null ? localYTsaurus.getMappedPort(80) :
                Integer.parseInt(
                        new BufferedReader(new FileReader("yt_proxy_port.txt")).readLine()
                );

        final BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        final String host = localYTsaurus != null ? localYTsaurus.getHost() : "localhost";

        final String user = "root";
        final String token = "";

        yt = YTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setClusters(List.of(new YTsaurusCluster("local", host, proxyPort)))
                .setPreferredClusterName("local")
                .setAuth(YTsaurusClientAuth.builder()
                        .setUser(user)
                        .setToken(token)
                        .build()
                ).setRpcCompression(new RpcCompression(Compression.Zlib_6))
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(new RpcOptions())
                                .build()
                )
                .disableValidation()
                .build();

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

        yt.createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(CypressNodeType.TABLE)
                .setAttributes(attributes)
                .build()
        ).join();
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

        ModifyRowsRequest req = ModifyRowsRequest.builder()
                .setPath(path)
                .setSchema(schema)
                .addInsert(Arrays.asList("key1", "value1"))
                .addInsert(Arrays.asList("key2", "value2"))
                .build();

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
