package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.utility.MountableFile;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.testlib.LocalYTsaurus;
import tech.ytsaurus.testlib.LoggingUtils;
import tech.ytsaurus.testlib.YTsaurusFixture;

public class YTsaurusClientMulticellTestBase {
    static {
        LoggingUtils.loadJULConfig(
                YTsaurusClientMulticellTestBase.class.getResourceAsStream("/logging.properties")
        );

        if (!System.getenv().containsKey("YT_PROXY")) {
            LocalYTsaurus.startContainer(new LocalYTsaurus.Config()
                    .setSecondaryMasterCellCount(3)
                    .setRpcProxyCount(1)
                    .setRpcProxyPorts(List.of(10111))
                    .setProxyConfigFile(MountableFile.forClasspathResource("/proxy_config.yson"))
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
