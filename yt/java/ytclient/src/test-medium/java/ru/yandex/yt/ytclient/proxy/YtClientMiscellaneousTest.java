package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;


@RunWith(Parameterized.class)
public class YtClientMiscellaneousTest {
    private final RpcCredentials credentials = new RpcCredentials("root", "");
    private final RpcOptions options;
    private String clusterName;

    public YtClientMiscellaneousTest(RpcOptions options) {
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
        clusterName = LocalYt.getAddress();
    }

    @Test
    public void testGetAliveDestinationClusterNameNormalization() {
        YtCluster.normalizationLowersHostName = true;
        SafelyClosable defer = () -> YtCluster.normalizationLowersHostName = false;

        final String denormalizedClusterName = clusterName.toUpperCase();

        try (defer) {
            var busConnector = new DefaultBusConnector();
            var yt = new YtClient(
                    busConnector,
                    denormalizedClusterName,
                    credentials,
                    options);
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
