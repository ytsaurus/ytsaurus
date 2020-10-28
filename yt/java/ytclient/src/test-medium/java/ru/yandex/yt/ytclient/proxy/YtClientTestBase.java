package ru.yandex.yt.ytclient.proxy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClientTestBase {
    @Rule
    public TestName name = new TestName();
    private final GUID runId = GUID.create();
    YtClient yt;
    YPath testDirectory;

    @Before
    final public void setUpClient() {
        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setNewDiscoveryServiceEnabled(true);

        yt = YtClient.builder()
                .setCluster(LocalYt.getAddress())
                .setRpcOptions(rpcOptions)
                .setRpcCredentials(new RpcCredentials("root", ""))
                .build();

        testDirectory = YPath.simple("//tmp/ytclient-test/" + runId + "-" + name.getMethodName());

        yt.createNode(
                new CreateNode(testDirectory, ObjectType.MapNode)
                        .setRecursive(true)
                        .setForce(true)
        ).join();
    }

    @After
    final public void tearDownClient() {
        if (yt == null) {
            return;
        }

        try (var ignored = yt) {
            yt.removeNode(new RemoveNode(testDirectory).setForce(true)).join();
        }
    }
}
