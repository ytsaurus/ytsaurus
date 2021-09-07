package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import ru.yandex.yt.ytclient.DC;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class ProxySelectorTest {

    @Test
    public void testPessimizingProxySelector() {
        var selector = ProxySelector.pessimizing(DC.IVA);

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

        assertThat(proxyList.subList(4, 6), containsInAnyOrder(
                HostPort.parse("iva-v8y333.yandex.net:9103"),
                HostPort.parse("iva-idgvfhe.yandex.net:9103")
        ));
    }

    @Test
    public void testPreferringProxySelector() {
        var selector = ProxySelector.preferring(DC.IVA);

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

        assertThat(proxyList.subList(0, 2), containsInAnyOrder(
                HostPort.parse("iva-v8y333.yandex.net:9103"),
                HostPort.parse("iva-idgvfhe.yandex.net:9103")
        ));
    }
}
