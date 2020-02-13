package ru.yandex.yt.ytclient.rpc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableMap;

import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author aozeritsky
 */
public class YtClientTest {
    class DCData {
        final String name;
        final int destinations;
        final double weight;

        DCData(String name, int destinations, double weight) {
            this.name = name;
            this.destinations = destinations;
            this.weight = weight;
        }
    }

    // doesn't make sense since pr 1106444
    // TODO: remove?
    // @Test
    public void selectDestinations() {
        Map<String, DCData> testData = ImmutableMap.of(
                "dc1", new DCData("dc1", 10, 0.3),
                "dc2", new DCData("dc2", 10, 0.1),
                "dc3", new DCData("dc3", 10, 0.2));

        DataCenter[] dcs = new DataCenter[testData.size()];

        int k = 0;
        RpcClientFactory factory = new RpcClientTestStubs.RpcClientFactoryStub();
        Random rnd = new Random(0);

        for (Map.Entry<String, DCData> entry : testData.entrySet()) {
            String dc = entry.getKey();
            DCData d = entry.getValue();
            HashSet<HostPort> proxies = new HashSet<>(d.destinations);
            for (int i = 0; i < d.destinations; ++i) {
                proxies.add(HostPort.parse("localhost:" + i));
            }

            DataCenter dataCenter = new DataCenter(dc, d.weight);
            dataCenter.setProxies(proxies, factory, rnd);
            dcs[k++] = dataCenter;
        }
        List<RpcClient> res;

        res = Manifold.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/6, dc3/9]"));

        res = Manifold.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/5, dc2/3]"));

        res = Manifold.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc3/7, dc3/3, dc1/9]"));

        res = Manifold.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/6, dc3/9]"));

        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/6, dc1/5, dc2/5, dc2/3, dc3/9, dc3/7]"));

        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/6, dc3/3, dc3/9, dc2/5, dc2/3]"));

        res = Manifold.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc3/9, dc3/7, dc1/5, dc1/6, dc2/5, dc2/7]"));

        res = Manifold.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc1/6, dc1/9, dc3/3, dc3/9, dc2/7, dc2/3]"));
    }
}
