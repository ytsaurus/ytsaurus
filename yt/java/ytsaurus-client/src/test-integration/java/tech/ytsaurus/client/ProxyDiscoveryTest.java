package tech.ytsaurus.client;

import java.util.concurrent.CompletionException;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

public class ProxyDiscoveryTest {
    @Test
    public void testUseTLS() {
        testDiscoveryProxyURI(
                "some.address.org",
                true,
                "https://some.address.org:443/api/v4/discover_proxies?type=rpc"
        );
    }

    @Test
    public void testDoNotUseTLS() {
        testDiscoveryProxyURI(
                "some.address.org",
                false,
                "http://some.address.org:80/api/v4/discover_proxies?type=rpc"
        );
    }

    @Test
    public void testSpecificPort() {
        testDiscoveryProxyURI(
                "some.address.org:123",
                false,
                "http://some.address.org:123/api/v4/discover_proxies?type=rpc"
        );
    }

    private void testDiscoveryProxyURI(String cluster, boolean useTLS, String expectedDiscoveryProxiesURI) {
        try (var client = YTsaurusClient.builder()
                .setCluster(cluster)
                .setAuth(YTsaurusClientAuth.builder().setToken("123").build())
                .setConfig(YTsaurusClientConfig.builder().setUseTLS(useTLS).build())
                .build()
        ) {
            client.waitProxies().join();
        } catch (CompletionException ex) {
            String message = ex.getCause().getCause().getMessage();
            Assert.assertTrue(message.contains(expectedDiscoveryProxiesURI));
            return;
        }
        Assert.fail();
    }
}
