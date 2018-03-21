package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.rpc.internal.BalancingDestination;
import ru.yandex.yt.ytclient.rpc.internal.BalancingResponseHandler;
import ru.yandex.yt.ytclient.rpc.internal.DataCenter;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolderImpl;

import static java.lang.Integer.min;

/**
 * @author aozeritsky
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration failoverTimeout;
    final private Duration pingTimeout;
    final private Duration globalTimeout;
    final private String dataCenterName;
    final private DataCenter[] dataCenters;
    private DataCenter localDataCenter;
    final private Random rnd = new Random();
    final private ScheduledExecutorService executorService;
    final private RpcFailoverPolicy failoverPolicy;

    final private BalancingDestinationMetricsHolder balancingDestinationMetricsHolder;
    final private BalancingResponseHandlerMetricsHolder balancingResponseHandlerMetricsHolder;
    final private DataCenterMetricsHolder dataCenterMetricsHolder;

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcClient ... destinations) {

        this(failoverTimeout, globalTimeout, pingTimeout, connector, new DefaultRpcFailoverPolicy(), destinations);
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcFailoverPolicy failoverPolicy,
        RpcClient ... destinations) {

        this(
            failoverTimeout,
            globalTimeout,
            pingTimeout,
            connector,
            failoverPolicy,
            "unknown",
            ImmutableMap.of("unknown", Arrays.stream(destinations).collect(Collectors.toList()))
        );
    }
    public BalancingRpcClient(
            Duration failoverTimeout,
            Duration globalTimeout,
            Duration pingTimeout,
            BusConnector connector,
            RpcFailoverPolicy failoverPolicy,
            String dataCenter,
            Map<String, List<RpcClient>> dataCenters)
    {
        this(
                failoverTimeout,
                globalTimeout,
                pingTimeout,
                connector,
                failoverPolicy,
                dataCenter,
                dataCenters,
                BalancingDestinationMetricsHolderImpl.instance,
                BalancingResponseHandlerMetricsHolderImpl.instance,
                DataCenterMetricsHolderImpl.instance
        );
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcFailoverPolicy failoverPolicy,
        String dataCenter,
        Map<String, List<RpcClient>> dataCenters,
        BalancingDestinationMetricsHolder balancingDestinationMetricsHolder,
        BalancingResponseHandlerMetricsHolder balancingResponseHandlerMetricsHolder,
        DataCenterMetricsHolder dataCenterMetricsHolder)
    {
        assert failoverTimeout.compareTo(globalTimeout) <= 0;

        this.balancingDestinationMetricsHolder = balancingDestinationMetricsHolder;
        this.balancingResponseHandlerMetricsHolder = balancingResponseHandlerMetricsHolder;
        this.dataCenterMetricsHolder = dataCenterMetricsHolder;

        this.dataCenterName = dataCenter;
        this.failoverPolicy = failoverPolicy;
        this.failoverTimeout = failoverTimeout;
        this.pingTimeout = pingTimeout;
        this.globalTimeout = globalTimeout;
        this.executorService = connector.executorService();
        this.dataCenters = new DataCenter[dataCenters.size()];
        this.localDataCenter = null;

        int i = 0;
        for (Map.Entry<String, List<RpcClient>> entity : dataCenters.entrySet()) {
            String dcName = entity.getKey();
            List<RpcClient> clients = entity.getValue();
            BalancingDestination [] destinations = new BalancingDestination[clients.size()];

            int j = 0;
            int index = 0;

            for (RpcClient client : clients) {
                index = j++;
                destinations[index] = new BalancingDestination(dcName, client, index, balancingDestinationMetricsHolder);
            }
            DataCenter dc = new DataCenter(dcName, destinations, dataCenterMetricsHolder);

            index = i++;
            this.dataCenters[index] = dc;
            if (dcName.equals(dataCenterName)) {
                this.localDataCenter = dc;
                this.dataCenters[index] = this.dataCenters[0];
                this.dataCenters[0] = this.localDataCenter;
            }
        }

        schedulePing();
    }

    @Override
    public void close() {
        for (DataCenter dc : dataCenters) {
            dc.close();
        }
    }

    public Map<String, List<RpcClient>> getAliveDestinations() {
        Map<String, List<RpcClient>> aliveDestinations = new HashMap<>();
        Stream.of(dataCenters).forEach(dc -> aliveDestinations.put(dc.getName(), dc.getAliveDestinations()));
        return aliveDestinations;
    }

    public RpcClient getAliveClient() {
        List<RpcClient> r = selectDestinations(dataCenters, 1, localDataCenter != null, rnd, ! failoverPolicy.randomizeDcs());
        if (r.isEmpty()) {
            return null;
        } else {
            return r.get(0);
        }
    }

    static List<RpcClient> selectDestinations(DataCenter [] dataCenters, int maxSelect, boolean hasLocal, Random rnd) {
        return selectDestinations(dataCenters, maxSelect, hasLocal, rnd, false);
    }

    static List<RpcClient> selectDestinations(DataCenter [] dataCenters, int maxSelect, boolean hasLocal, Random rnd, boolean sortDcByPing) {
        List<RpcClient> r = new ArrayList<>();

        int n = dataCenters.length;
        DataCenter [] selectedDc = Arrays.copyOf(dataCenters, n);

        int from = 0;
        if (hasLocal) {
            from = 1;
        }

        if (sortDcByPing) {
            Arrays.sort(selectedDc, from, n, (a, b) ->
                (int)(1000*(a.weight() - b.weight()))
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

            List<RpcClient> select = dc.selectDestinations(min(maxSelect, 2), rnd);
            maxSelect -= select.size();

            r.addAll(select);

            if (maxSelect <= 0) {
                break;
            }
        }

        return r;
    }

    private void schedulePing() {
        executorService.schedule(
            () -> pingDataCenters(),
            pingTimeout.toMillis(),
            TimeUnit.MILLISECONDS);
    }

    private void pingDataCenters() {
        logger.debug("ping");

        CompletableFuture<Void> futures[] = new CompletableFuture[dataCenters.length];
        int i = 0;
        for (DataCenter entry : dataCenters) {
            futures[i++] = entry.ping(executorService, pingTimeout);
        }

        schedulePing();
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        List<RpcClient> destinations = selectDestinations(dataCenters, 3, localDataCenter != null, rnd, ! failoverPolicy.randomizeDcs());

        CompletableFuture<List<byte[]>> f = new CompletableFuture<>();

        BalancingResponseHandler h = new BalancingResponseHandler(
            executorService,
            failoverPolicy,
            globalTimeout,
            failoverTimeout,
            f,
            request,
            destinations,
            balancingResponseHandlerMetricsHolder);

        f.whenComplete((result, error) -> {
            h.cancel();
            if (error == null) {
                handler.onResponse(result);
            } else {
                handler.onError(error);
            }
        });

        return () -> f.cancel(true);
    }

    public String destinationName() {
        return "multidestination";
    }

    @Override
    public <V> ScheduledFuture<V> schedule(
            Callable<V> callable,
            long delay, TimeUnit unit)
    {
        return executorService.schedule(callable, delay, unit);
    }
}
