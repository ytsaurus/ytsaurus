package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import ru.yandex.yt.ytclient.rpc.RpcClient;

public class Manifold {
    private Manifold() {
    }

    public static List<RpcClient> selectDestinations(
            DataCenter[] dataCenters,
            int maxSelect,
            boolean hasLocal,
            Random rnd
    ) {
        return selectDestinations(dataCenters, maxSelect, hasLocal, rnd, false);
    }

    public static List<RpcClient> selectDestinations(
            DataCenter[] dataCenters,
            int maxSelect,
            boolean hasLocal,
            Random rnd,
            boolean sortDcByPing
    ) {
        List<RpcClient> r = new ArrayList<>();

        int n = dataCenters.length;
        DataCenter[] selectedDc = Arrays.copyOf(dataCenters, n);

        int from = 0;
        if (hasLocal) {
            from = 1;
        }

        if (sortDcByPing) {
            Arrays.sort(
                    selectedDc,
                    from,
                    n,
                    (a, b) -> (int) (1000 * (a.weight() - b.weight()))
            );
        } else {
            // shuffle
            for (int i = from; i < n; ++i) {
                int j = i + rnd.nextInt(n - i);
                DataCenter t = selectedDc[j];
                selectedDc[j] = selectedDc[i];
                selectedDc[i] = t;
            }
        }

        for (int i = 0; i < n; ++i) {
            DataCenter dc = selectedDc[i];

            List<RpcClient> select = dc.selectDestinations(Math.min(maxSelect, 2), rnd);
            maxSelect -= select.size();

            r.addAll(select);

            if (maxSelect <= 0) {
                break;
            }
        }

        return r;
    }
}
