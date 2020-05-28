package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.DiscoveryMethod;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class PeriodicDiscoveryTest {

    private List<String> empty = new ArrayList<>();
    private List<String> nonEmpty = Collections.singletonList("address");

    private PeriodicDiscovery createPeriodicDiscovery(TestArguments arguments) {
        BusConnector connector = mock(BusConnector.class);
        PeriodicDiscoveryListener listener = mock(PeriodicDiscoveryListener.class);

        RpcOptions options = new RpcOptions();
        options.setPreferableDiscoveryMethod(arguments.discoveryMethod);

        return new PeriodicDiscovery("test", arguments.initialAddresses, null,
                arguments.clusterUrl, connector, options, new RpcCredentials(), new RpcCompression(), listener
        );
    }

    private static class TestArguments {
        DiscoveryMethod discoveryMethod;
        List<String> initialAddresses;
        String clusterUrl;
        DiscoveryMethod expected;

        TestArguments(DiscoveryMethod discoveryMethod,
                      List<String> initialAddresses,
                      String clusterUrl,
                      DiscoveryMethod expected) {
            this.discoveryMethod = discoveryMethod;
            this.initialAddresses = initialAddresses;
            this.clusterUrl = clusterUrl;
            this.expected = expected;
        }
    }

    @Test
    public void testDiscoveryMethod() {
        List<TestArguments> arguments = Arrays.asList(
                new TestArguments(DiscoveryMethod.HTTP, empty, "url", DiscoveryMethod.HTTP),
                new TestArguments(DiscoveryMethod.HTTP, nonEmpty, "url", DiscoveryMethod.HTTP),
                new TestArguments(DiscoveryMethod.HTTP, empty, null, DiscoveryMethod.RPC),
                new TestArguments(DiscoveryMethod.HTTP, nonEmpty, null, DiscoveryMethod.RPC),

                new TestArguments(DiscoveryMethod.RPC, empty, "url", DiscoveryMethod.HTTP),
                new TestArguments(DiscoveryMethod.RPC, nonEmpty, "url", DiscoveryMethod.RPC),
                new TestArguments(DiscoveryMethod.RPC, empty, null, DiscoveryMethod.RPC),
                new TestArguments(DiscoveryMethod.RPC, nonEmpty, null, DiscoveryMethod.RPC)
        );

        for (TestArguments arg : arguments) {
            assertThat(createPeriodicDiscovery(arg).selectDiscoveryMethod(), is(arg.expected));
        }
    }


}
