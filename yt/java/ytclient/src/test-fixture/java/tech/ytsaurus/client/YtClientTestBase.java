package tech.ytsaurus.client;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;

public class YtClientTestBase {
    protected static class YtFixture {
        final HostPort address;
        final YtClient yt;
        final YPath testDirectory;

        YtFixture(HostPort address, YtClient yt, YPath testDirectory) {
            this.address = address;
            this.yt = yt;
            this.testDirectory = testDirectory;
        }

        public YtClient getYt() {
            return yt;
        }

        public YPath getTestDirectory() {
            return testDirectory;
        }
    }

    @Rule
    public TestName name = new TestName();
    private final GUID runId = GUID.create();
    List<YtFixture> ytFixtures = new ArrayList<>();

    public final YtFixture createYtFixture() {
        RpcOptions rpcOptions = new RpcOptions();
        return createYtFixture(rpcOptions);
    }

    public final YtFixture createYtFixture(RpcOptions rpcOptions) {
        var address = LocalYt.getAddress();
        var yt = YtClient.builder()
                .setCluster(address)
                .setYtClientConfiguration(
                        YtClientConfiguration.builder()
                                .setRpcOptions(rpcOptions)
                                .setJavaBinary(System.getProperty("java.home") + "/bin/java")
                                .setJobSpecPatch(null)
                                .setSpecPatch(null)
                                .setOperationPingPeriod(Duration.ofMillis(500))
                                .build()
                )
                .setAuth(YTsaurusClientAuth.builder()
                        .setUser("root")
                        .setToken("")
                        .build())
                .build();

        var methodName = name.getMethodName().replaceAll("[\\[\\]]", "-");
        var testDirectory = YPath.simple("//tmp/ytclient-test/" + runId + "-" + methodName);

        yt.createNode(
                new CreateNode(testDirectory, CypressNodeType.MAP)
                        .setRecursive(true)
                        .setForce(true)
        ).join();

        YtFixture result = new YtFixture(HostPort.parse(address), yt, testDirectory);
        ytFixtures.add(result);
        return result;
    }

    protected void writeTable(YtClient yt, YPath path, TableSchema tableSchema, List<YTreeMapNode> data) {
        try {
            yt.createNode(path.justPath().toString(), CypressNodeType.TABLE).join();
        } catch (CompletionException ignored) {
        }

        TableWriter<YTreeMapNode> writer = yt.writeTable(new WriteTable<>(path, YTreeMapNode.class)).join();
        try {
            writer.write(data, tableSchema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected List<YTreeMapNode> readTable(YtClient yt, YPath path) {
        TableReader<YTreeMapNode> reader = yt.readTable(
                new ReadTable<>(path, YTreeObjectSerializerFactory.forClass(YTreeMapNode.class))).join();
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
                        } catch (Exception ex) {
                        }
                    }
                }

                yt.removeNode(new RemoveNode(fixture.testDirectory).setForce(true)).join();
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
