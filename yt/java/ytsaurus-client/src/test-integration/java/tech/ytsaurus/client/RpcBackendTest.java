package tech.ytsaurus.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientListener;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestDescriptor;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.ETransactionType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

public class RpcBackendTest extends YTsaurusClientTestBase {
    private YTsaurusClient yt;

    @Before
    public void setup() throws IOException {
        GenericContainer<?> ytsaurusContainer = getYtsaurusContainer();
        final int proxyPort = ytsaurusContainer != null
                ? ytsaurusContainer.getMappedPort(80)
                : Integer.parseInt(System.getenv("YT_PROXY").split(":")[1]);

        final BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        final String host = ytsaurusContainer != null ? ytsaurusContainer.getHost() : "localhost";

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
    public void testRpcClientListener() {
        List<Event> events = Collections.synchronizedList(new ArrayList<>());

        var rpcOptions = new RpcOptions()
                .setRpcClientListener((ctx, bytes) -> {
                    events.add(new Event(ctx.getMethod(), ctx.getService(), ctx.isStream(),
                            bytes));
                });
        YTsaurusClient ytWithListener = YTsaurusClient.builder()
                .setClusters(yt.getClusters())
                .setPreferredClusterName("local")
                .setAuth(YTsaurusClientAuth.builder().setUser("root").setToken("").build())
                .setConfig(YTsaurusClientConfig.builder()
                        .setRpcOptions(rpcOptions)
                        .build())
                .disableValidation()
                .build();

        ytWithListener.waitProxies().join();

        String path = "//tmp/test_listener_" + UUID.randomUUID();
        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        ytWithListener.createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of("schema", schema.toYTree()))
                .build()).join();

        var writer = ytWithListener.writeTableV2(WriteTable.builder(YTreeMapNode.class)
                .setPath(YPath.simple(path))
                .build()).join();

        writer.write(List.of(YTree.mapBuilder().key("key").value("k1").key("value").value("v1").buildMap())).join();
        writer.finish().join();

        List<Event> expected = List.of(
                new Event("CreateNode", "ApiService", false),
                new Event("StartTransaction", "ApiService", false),
                new Event("CreateNode", "ApiService", false),
                new Event("LockNode", "ApiService", false),
                new Event("GetNode", "ApiService", false),
                new Event("StartTransaction", "ApiService", false),
                new Event("WriteTable", "ApiService", false),
                new Event("WriteTable", "ApiService", true),
                new Event("CommitTransaction", "ApiService", false),
                new Event("CommitTransaction", "ApiService", false)
        );

        int idx = 0;
        for (Event e : events) {
            if (idx >= expected.size()) {
                break;
            }
            var exp = expected.get(idx);
            Assert.assertEquals(exp.method, e.method);
            Assert.assertEquals(exp.service, e.service);
            Assert.assertEquals(exp.isStream, e.isStream);
            Assert.assertTrue(e.bytes > 0);
            idx++;
        }
        Assert.assertEquals("Did not observe expected RPC sequence", expected.size(), idx);
    }

    @Test
    public void testDirectYTsaurusClientWithRpcClientListener() {
        List<Event> events = Collections.synchronizedList(new ArrayList<>());

        var rpcOptions = new RpcOptions()
                .setRpcClientListener((ctx, bytes) -> {
                    events.add(new Event(ctx.getMethod(), ctx.getService(), ctx.isStream(),
                            bytes));
                });

        // Get a working RPC proxy address from the existing YTsaurusClient
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        RpcClient rpcClient;
        try {
            rpcClient = yt.getClientPool().peekClient(releaseFuture).join();
        } finally {
            releaseFuture.complete(null); // Release the client immediately since we only need the address
        }

        String proxyAddress = rpcClient.getAddressString();
        if (proxyAddress == null) {
            throw new RuntimeException("Cannot get proxy address from RPC client");
        }

        String[] addressParts = proxyAddress.split(":");
        String proxyHost = addressParts[0];
        int proxyPort = Integer.parseInt(addressParts[1]);

        final BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        DirectYTsaurusClient directYt = DirectYTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setAddress(new java.net.InetSocketAddress(proxyHost, proxyPort))
                .setAuth(YTsaurusClientAuth.builder().setUser("root").setToken("").build())
                .setConfig(YTsaurusClientConfig.builder()
                        .setRpcOptions(rpcOptions)
                        .build())
                .build();

        String path = "//tmp/test_direct_listener_" + UUID.randomUUID();
        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        directYt.createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of("schema", schema.toYTree()))
                .build()).join();

        var writer = directYt.writeTableV2(WriteTable.builder(YTreeMapNode.class)
                .setPath(YPath.simple(path))
                .build()).join();

        writer.write(List.of(YTree.mapBuilder().key("key").value("k1").key("value").value("v1").buildMap())).join();
        writer.finish().join();

        directYt.close();
        connector.close();

        List<Event> expected = List.of(
                new Event("CreateNode", "ApiService", false),
                new Event("StartTransaction", "ApiService", false),
                new Event("CreateNode", "ApiService", false),
                new Event("LockNode", "ApiService", false),
                new Event("GetNode", "ApiService", false),
                new Event("StartTransaction", "ApiService", false),
                new Event("WriteTable", "ApiService", false),
                new Event("WriteTable", "ApiService", true),
                new Event("CommitTransaction", "ApiService", false),
                new Event("CommitTransaction", "ApiService", false)
        );

        int idx = 0;
        for (Event e : events) {
            if (idx >= expected.size()) {
                break;
            }
            var exp = expected.get(idx);
            Assert.assertEquals(exp.method, e.method);
            Assert.assertEquals(exp.service, e.service);
            Assert.assertEquals(exp.isStream, e.isStream);
            Assert.assertTrue(e.bytes > 0);
            idx++;
        }
        Assert.assertEquals("Did not observe expected RPC sequence for DirectYTsaurusClient", expected.size(), idx);
    }

    @Test
    public void testRpcClientListenerBytesReceived() {
        List<Event> sentEvents = Collections.synchronizedList(new ArrayList<>());
        List<Event> receivedEvents = Collections.synchronizedList(new ArrayList<>());

        var rpcOptions = new RpcOptions()
                .setRpcClientListener(new RpcClientListener() {
                    @Override
                    public void onBytesSent(RpcRequestDescriptor ctx, long bytes) {
                        sentEvents.add(new Event(ctx.getMethod(), ctx.getService(), ctx.isStream(), bytes));
                    }

                    @Override
                    public void onBytesReceived(RpcRequestDescriptor ctx, long bytes) {
                        receivedEvents.add(new Event(ctx.getMethod(), ctx.getService(), ctx.isStream(), bytes));
                    }
                });
        YTsaurusClient ytWithListener = YTsaurusClient.builder()
                .setClusters(yt.getClusters())
                .setPreferredClusterName("local")
                .setAuth(YTsaurusClientAuth.builder().setUser("root").setToken("").build())
                .setConfig(YTsaurusClientConfig.builder()
                        .setRpcOptions(rpcOptions)
                        .build())
                .disableValidation()
                .build();

        ytWithListener.waitProxies().join();

        // Non-streaming RPC: GetNode
        ytWithListener.getNode("//tmp/@").join();

        boolean hasGetNodeReceived = receivedEvents.stream()
                .anyMatch(e -> "GetNode".equals(e.method) && e.bytes > 0);
        Assert.assertTrue("Expected onBytesReceived for GetNode with bytes > 0", hasGetNodeReceived);

        // Streaming RPC: ReadTable
        String path = "//tmp/test_bytes_received_" + UUID.randomUUID();
        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        ytWithListener.createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of("schema", schema.toYTree()))
                .build()).join();

        var writer = ytWithListener.writeTableV2(WriteTable.builder(YTreeMapNode.class)
                .setPath(YPath.simple(path))
                .build()).join();
        writer.write(List.of(YTree.mapBuilder().key("key").value("k1").key("value").value("v1").buildMap())).join();
        writer.finish().join();

        receivedEvents.clear();
        readTable(ytWithListener, YPath.simple(path));

        boolean hasReadTableReceived = receivedEvents.stream()
                .anyMatch(e -> "ReadTable".equals(e.method) && e.isStream && e.bytes > 0);
        Assert.assertTrue("Expected onBytesReceived for ReadTable streaming with bytes > 0", hasReadTableReceived);
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

    private static class Event {
        final String method;
        final String service;
        final boolean isStream;
        long bytes;

        Event(String method, String service, boolean isStream, long bytes) {
            this.method = method;
            this.service = service;
            this.isStream = isStream;
            this.bytes = bytes;
        }

        Event(String method, String service, boolean isStream) {
            this.method = method;
            this.service = service;
            this.isStream = isStream;
        }
    }
}
