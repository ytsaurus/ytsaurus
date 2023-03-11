package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ProxySelectorTest {

    @Test
    public void testPessimizingProxySelector() {
        var selector = YandexProxySelector.pessimizing(DC.IVA);

        var proxyList = new ArrayList<>(
                List.of(
                        HostPort.parse("iva-idgvfhe.yandex.net:9103"),
                        HostPort.parse("man-oduwg35.yandex.net:9103"),
                        HostPort.parse("iva-v8y333.yandex.net:9103"),
                        HostPort.parse("iw38wfee.yandex.net:9103"),
                        HostPort.parse("sas-uwg734.yandex.net:9103"),
                        HostPort.parse("o83hfhy.yandex.net:9103")
                )
        );

        selector.rank(proxyList);

        var subList = proxyList.subList(4, 6);
        Assert.assertTrue(subList.contains(HostPort.parse("iva-v8y333.yandex.net:9103")));
        Assert.assertTrue(subList.contains(HostPort.parse("iva-idgvfhe.yandex.net:9103")));
    }

    @Test
    public void testPreferringProxySelector() {
        var selector = YandexProxySelector.preferring(DC.IVA);

        var proxyList = new ArrayList<>(
                List.of(
                        HostPort.parse("iva-idgvfhe.yandex.net:9103"),
                        HostPort.parse("man-oduwg35.yandex.net:9103"),
                        HostPort.parse("iva-v8y333.yandex.net:9103"),
                        HostPort.parse("iw38wfee.yandex.net:9103"),
                        HostPort.parse("sas-uwg734.yandex.net:9103"),
                        HostPort.parse("o83hfhy.yandex.net:9103")
                )
        );

        selector.rank(proxyList);

        var subList = proxyList.subList(0, 2);
        Assert.assertTrue(subList.contains(HostPort.parse("iva-v8y333.yandex.net:9103")));
        Assert.assertTrue(subList.contains(HostPort.parse("iva-idgvfhe.yandex.net:9103")));
    }
}
