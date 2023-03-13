package tech.ytsaurus.client;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcClient;

import static tech.ytsaurus.FutureUtils.waitOkResult;

public class YandexClientPoolTest extends ClientPoolTestBase {
    @Test
    public void testProxySelector() {
        ClientPool clientPool = newClientPool(YandexProxySelector.pessimizing(DC.MAN));

        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("man-host:2"),
                                HostPort.parse("sas-host:3"),
                                HostPort.parse("vla-host:4"),
                                HostPort.parse("iva-host:5")
                        )),
                100
        );

        List<String> selectedClients = Arrays.stream(clientPool.getAliveClients())
                .map(RpcClient::getAddressString)
                .collect(Collectors.toList());

        Assert.assertFalse(selectedClients.contains("man-host:2"));
    }
}
