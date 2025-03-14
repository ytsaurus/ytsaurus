package tech.ytsaurus.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.utility.MountableFile;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.testlib.LocalYTsaurus;
import tech.ytsaurus.testlib.LoggingUtils;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class YTsaurusClientTestBase {
    static {
        LoggingUtils.loadJULConfig(
                YTsaurusClientTestBase.class.getResourceAsStream("/logging.properties")
        );

        if (!System.getenv().containsKey("YT_PROXY")) {
            LocalYTsaurus.startContainer(new LocalYTsaurus.Config()
                    .setRpcProxyCount(1)
                    .setRpcProxyPorts(List.of(10111))
                    .setRpcProxyConfigFile(MountableFile.forClasspathResource("/rpc_proxy_config.yson"))
                    .setProxyConfigFile(MountableFile.forClasspathResource("/proxy_config.yson"))
                    .setQueueAgentCount(1)
                    .setDiscoveryServerCount(1)
                    .setDiscoveryServerPorts(List.of(10125))
            );
        }
    }

    @Rule
    public TestName name = new TestName();
    private final GUID runId = GUID.create();
    List<YTsaurusFixture> ytFixtures = new ArrayList<>();

    public final YTsaurusFixture createYtFixture() {
        RpcOptions rpcOptions = new RpcOptions();
        return createYtFixture(rpcOptions);
    }

    public final YTsaurusFixture createYtFixture(RpcOptions rpcOptions) {
        var methodName = name.getMethodName().replaceAll("[\\[\\]]", "-");
        var testDirectory = YPath.simple("//tmp/ytsaurus-client-test/" + runId + "-" + methodName);

        YTsaurusFixture fixture = YTsaurusFixture.builder()
                .setYTsaurusAddress(LocalYTsaurus.getAddress())
                .setContainerRunning(LocalYTsaurus.getContainer() != null)
                .setRpcOptions(rpcOptions)
                .setTestDirectoryPath(testDirectory)
                .build();
        ytFixtures.add(fixture);
        return fixture;
    }

    protected static String getYTsaurusAddress() {
        return LocalYTsaurus.getAddress();
    }

    protected static String getYTsaurusHost() {
        return LocalYTsaurus.getHost();
    }

    protected static int getYTsaurusPort() {
        return LocalYTsaurus.getPort();
    }

    protected void writeTable(YTsaurusClient yt, YPath path, TableSchema tableSchema, List<YTreeMapNode> data) {
        try {
            yt.createNode(path.justPath().toString(), CypressNodeType.TABLE).join();
        } catch (CompletionException ignored) {
        }

        TableWriter<YTreeMapNode> writer = yt.writeTable(
                new WriteTable<>(path, new SerializationContext<>(YTreeMapNode.class))).join();
        try {
            writer.write(data, tableSchema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected List<YTreeMapNode> readTable(YTsaurusClient yt, YPath path) {
        TableReader<YTreeMapNode> reader = yt.readTable(
                new ReadTable<>(path, new SerializationContext<>(YTreeMapNode.class))).join();
        List<YTreeMapNode> result = new ArrayList<>();
        List<YTreeMapNode> rows;
        try {
            while (reader.canRead()) {
                while ((rows = reader.read()) != null) {
                    result.addAll(rows);
                }
                reader.readyEvent().join();
            }
            reader.close().join();
            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void waitTabletCells(YTsaurusClient yt) {
        while (!yt.getNode("//sys/tablet_cell_bundles/default/@health").join().stringValue().equals("good")) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public final void tearDown() throws Throwable {
        Throwable error = null;
        for (var fixture : ytFixtures) {
            try {
                fixture.stop();
            } catch (Throwable ex) {
                if (error == null) {
                    error = new RuntimeException("Error while tear down test", ex);
                } else {
                    error.addSuppressed(ex);
                }
            }
        }
        ytFixtures.clear();
        if (error != null) {
            throw error;
        }
    }
}
