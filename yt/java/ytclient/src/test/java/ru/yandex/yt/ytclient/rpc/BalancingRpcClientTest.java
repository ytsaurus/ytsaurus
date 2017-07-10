package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import java.util.Random;
import org.junit.Test;

import ru.yandex.yt.ytclient.rpc.internal.BalancingDestination;
import ru.yandex.yt.ytclient.rpc.internal.DataCenter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author aozeritsky
 */
public class BalancingRpcClientTest {
    @Test
    public void selectDestinations() {
        Map<String, Integer> testData = ImmutableMap.of("dc1", 10, "dc2", 10, "dc3", 10);

        DataCenter [] dcs = new DataCenter[testData.size()];

        int k = 0;

        for (Map.Entry<String, Integer> entry : testData.entrySet()) {
            String dc = entry.getKey();
            int n = entry.getValue();
            BalancingDestination[] dst = new BalancingDestination[n];
            for (int i = 0; i < n; ++i) {
                dst[i] = new BalancingDestination(dc, i);
            }

            dcs[k++] = new DataCenter(dc, dst);
        }


        Random rnd = new Random(0);

        List<BalancingDestination> res;

        res = BalancingRpcClient.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/9, dc1/2, dc3/5]"));

        res = BalancingRpcClient.selectDestinations(dcs, 3, true, rnd);
        assertThat(res.toString(), is("[dc1/1, dc1/6, dc2/4]"));

        res = BalancingRpcClient.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc3/2, dc3/8, dc2/9]"));

        res = BalancingRpcClient.selectDestinations(dcs, 3, false, rnd);
        assertThat(res.toString(), is("[dc2/0, dc2/4, dc1/6]"));

        res = BalancingRpcClient.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/8, dc1/9, dc2/3, dc2/7, dc3/5, dc3/8]"));

        res = BalancingRpcClient.selectDestinations(dcs, 6, true, rnd);
        assertThat(res.toString(), is("[dc1/7, dc1/6, dc3/2, dc3/5, dc2/5, dc2/2]"));

        res = BalancingRpcClient.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc2/3, dc2/2, dc3/0, dc3/1, dc1/1, dc1/7]"));

        res = BalancingRpcClient.selectDestinations(dcs, 6, false, rnd);
        assertThat(res.toString(), is("[dc2/3, dc2/0, dc3/6, dc3/3, dc1/8, dc1/1]"));
    }
}
