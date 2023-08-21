package tech.ytsaurus.client;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;


@RunWith(Parameterized.class)
public class YTsaurusClientMiscellaneousTest extends YTsaurusClientTestBase {
    private final YTsaurusClientAuth auth = YTsaurusClientAuth.builder()
            .setUser("root")
            .setToken("")
            .build();
    private final RpcOptions options;
    private String clusterName;

    public YTsaurusClientMiscellaneousTest(RpcOptions options) {
        this.options = options;
    }

    @Parameterized.Parameters
    public static List<RpcOptions> getRpcOptions() {
        return List.of(
                new RpcOptions(),
                new RpcOptions()
        );
    }

    @Before
    public void before() {
        clusterName = getYTsaurusAddress();
    }

    @Test
    public void testGetAliveDestinationClusterNameNormalization() {
        YTsaurusCluster.normalizationLowersHostName = true;
        SafelyClosable defer = () -> YTsaurusCluster.normalizationLowersHostName = false;

        final String denormalizedClusterName = clusterName.toUpperCase();

        try (defer) {
            var busConnector = new DefaultBusConnector();
            var yt = YTsaurusClient.builder()
                    .setSharedBusConnector(busConnector)
                    .setClusters(List.of(new YTsaurusCluster(denormalizedClusterName)))
                    .setAuth(auth)
                    .setConfig(YTsaurusClientConfig.builder()
                            .setRpcOptions(options)
                            .build()
                    )
                    .disableValidation()
                    .build();
            try (yt; busConnector) {
                yt.waitProxies().join();
                var aliveDestinations = yt.getAliveDestinations();
                assertThat(aliveDestinations, hasKey(denormalizedClusterName));
                assertThat(aliveDestinations, not(hasKey(clusterName)));
            }
        }
    }
}

interface SafelyClosable extends AutoCloseable {
    void close();
}
