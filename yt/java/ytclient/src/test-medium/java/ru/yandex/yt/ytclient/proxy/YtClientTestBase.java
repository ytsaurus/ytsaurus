package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClientTestBase {
    static class YtFixture {
        final HostPort address;
        final YtClient yt;
        final YPath testDirectory;

        YtFixture(HostPort address, YtClient yt, YPath testDirectory) {
            this.address = address;
            this.yt = yt;
            this.testDirectory = testDirectory;
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
        rpcOptions.setNewDiscoveryServiceEnabled(true);
        var address = LocalYt.getAddress();
        var yt = YtClient.builder()
                .setCluster(address)
                .setRpcOptions(rpcOptions)
                .setRpcCredentials(new RpcCredentials("root", ""))
                .build();

        var methodName = name.getMethodName().replaceAll("[\\[\\]]", "-");
        var testDirectory = YPath.simple("//tmp/ytclient-test/" + runId + "-" + methodName);

        yt.createNode(
                new CreateNode(testDirectory, ObjectType.MapNode)
                        .setRecursive(true)
                        .setForce(true)
        ).join();

        YtFixture result = new YtFixture(HostPort.parse(address), yt, testDirectory);
        ytFixtures.add(result);
        return result;
    }

    @After
    public final void tearDown() throws Throwable {
        Throwable error = null;
        for (var fixture : ytFixtures) {
            try (var yt = fixture.yt) {
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
