package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableMap;

import ru.yandex.yt.ytclient.proxy.internal.BalancingDestination;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;

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

        DCData (String name, int destinations, double weight) {
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

        DataCenter [] dcs = new DataCenter[testData.size()];

        int k = 0;

        for (Map.Entry<String, DCData> entry : testData.entrySet()) {
            String dc = entry.getKey();
            DCData d = entry.getValue();
            BalancingDestination[] dst = new BalancingDestination[d.destinations];
            for (int i = 0; i < d.destinations; ++i) {
                dst[i] = new BalancingDestination(dc, i);
            }

            dcs[k++] = new DataCenter(dc, dst, d.weight);
        }


        Random rnd = new Random(0);

        List<RpcClient> res;

        res = Manifold.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/2, dc3/5]"));

        res = Manifold.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/1, dc1/6, dc2/4]"));

        res = Manifold.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc3/2, dc3/8, dc2/9]"));

        res = Manifold.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc2/0, dc2/4, dc1/6]"));

        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/8, dc1/9, dc2/3, dc2/7, dc3/5, dc3/8]"));

        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/7, dc1/6, dc3/2, dc3/5, dc2/5, dc2/2]"));

        res = Manifold.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc2/3, dc2/2, dc3/0, dc3/1, dc1/1, dc1/7]"));

        res = Manifold.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc2/3, dc2/0, dc3/6, dc3/3, dc1/8, dc1/1]"));

        // filter dead proxies
        k = testData.get(dcs[0].getName()).destinations;
        for (int i = 0; i < k; ++i) {
//            dcs[0].setDead(0, new Exception());
        }
        assertThat(dcs[0].isAlive(), is(false));
        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc2/2, dc2/4, dc3/7, dc3/6]"));

//        dcs[0].setAlive(0);
//        dcs[0].setAlive(1);
        assertThat(dcs[0].isAlive(), is(true));
        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/6, dc3/5, dc3/6, dc2/4, dc2/9]"));
        res = Manifold.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/6, dc1/9, dc2/6, dc2/4, dc3/3, dc3/2]"));

        res = Manifold.selectDestinations(dcs, 6, true, rnd, true);
        assertThat(res.toString(), is("[dc1/6, dc1/9, dc2/0, dc2/9, dc3/5, dc3/4]"));

        res = Manifold.selectDestinations(dcs, 6, false, rnd, true);
        assertThat(res.toString(), is("[dc2/5, dc2/2, dc3/5, dc3/3, dc1/9, dc1/6]"));
    }
}
