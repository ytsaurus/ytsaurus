package tech.ytsaurus.client;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.testlib.LocalYTsaurus;
import tech.ytsaurus.testlib.LoggingUtils;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class YTsaurusClientTestBase {
    protected static class YTsaurusFixture {
        final HostPort address;
        final YTsaurusClient yt;
        final YPath testDirectory;

        YTsaurusFixture(HostPort address, YTsaurusClient yt, YPath testDirectory) {
            this.address = address;
            this.yt = yt;
            this.testDirectory = testDirectory;
        }

        public YTsaurusClient getYt() {
            return yt;
        }

        public YPath getTestDirectory() {
            return testDirectory;
        }
    }

    protected static GenericContainer<?> localYTsaurus;

    static {
        LoggingUtils.loadJULConfig(
                YTsaurusClientTestBase.class.getResourceAsStream("/logging.properties")
        );

        if (!System.getenv().containsKey("YT_PROXY")) {
            localYTsaurus = new FixedHostPortGenericContainer<>("ytsaurus/local:dev")
                    .withFixedExposedPort(10110, 80) // http
                    .withFixedExposedPort(10111, 10111) // rpc_proxy
                    .withNetwork(Network.newNetwork())
                    .withCommand(
                            "--proxy-config", "/tmp/proxy_config.yson",
                            "--rpc-proxy-count", "1",
                            "--rpc-proxy-port", "10111",
                            "--enable-debug-logging"
                    )
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("/proxy_config.yson"),
                            "/tmp/proxy_config.yson"
                    ).waitingFor(Wait.forLogMessage(".*Local YT started.*", 1));
            localYTsaurus.start();
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
        var address = getYTsaurusAddress();

        String javaHome = localYTsaurus == null ?
                System.getProperty("java.home") : "/opt/jdk11";

        var yt = YTsaurusClient.builder()
                .setCluster(address)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(rpcOptions)
                                .setJavaBinary(javaHome + "/bin/java")
                                .setJobSpecPatch(null)
                                .setSpecPatch(null)
                                .setOperationPingPeriod(Duration.ofMillis(500))
                                .build()
                )
                .setAuth(
                        YTsaurusClientAuth.builder()
                                .setUser("root")
                                .setToken("")
                                .build()
                )
                .build();

        var methodName = name.getMethodName().replaceAll("[\\[\\]]", "-");
        var testDirectory = YPath.simple("//tmp/ytsaurus-client-test/" + runId + "-" + methodName);

        yt.createNode(
                CreateNode.builder()
                        .setPath(testDirectory)
                        .setType(CypressNodeType.MAP)
                        .setRecursive(true)
                        .setForce(true)
                        .build()
        ).join();

        YTsaurusFixture result = new YTsaurusFixture(HostPort.parse(address), yt, testDirectory);
        ytFixtures.add(result);
        return result;
    }

    protected static String getYTsaurusAddress() {
        return localYTsaurus != null ?
                localYTsaurus.getHost() + ":" + localYTsaurus.getMappedPort(80)
                : LocalYTsaurus.getAddress();
    }

    protected static String getYTsaurusHost() {
        return localYTsaurus != null ? localYTsaurus.getHost() : LocalYTsaurus.getHost();
    }

    protected static int getYTsaurusPort() {
        return localYTsaurus != null ? localYTsaurus.getMappedPort(80) : LocalYTsaurus.getPort();
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

    @After
    public final void tearDown() throws Throwable {
        Throwable error = null;
        for (var fixture : ytFixtures) {
            try (var yt = fixture.yt) {
                var rpcRequestsTestingController = yt.rpcOptions.getTestingOptions().getRpcRequestsTestingController();
                if (rpcRequestsTestingController != null) {
                    var transactionIds = rpcRequestsTestingController.getStartedTransactions();
                    for (GUID transactionId : transactionIds) {
                        try {
                            yt.abortTransaction(transactionId).join();
                        } catch (Exception ignored) {
                        }
                    }
                }

                yt.removeNode(RemoveNode.builder().setPath(fixture.testDirectory).setForce(true).build()).join();
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
