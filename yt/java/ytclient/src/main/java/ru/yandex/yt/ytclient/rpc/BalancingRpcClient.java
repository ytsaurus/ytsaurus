package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.FailoverRpcExecutor;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolderImpl;

/**
 * @author aozeritsky
 * @deprecated {@link ru.yandex.yt.ytclient.proxy.YtClient}
 */
@Deprecated
public class BalancingRpcClient implements RpcClient {
    private final DataCenter[] dataCenters;
    private final Random rnd = new Random();
    private final ScheduledExecutorService executorService;
    private final RpcFailoverPolicy failoverPolicy;
    private DataCenter localDataCenter;

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcClient... destinations
    ) {

        this(failoverTimeout, globalTimeout, pingTimeout, connector, new DefaultRpcFailoverPolicy(), destinations);
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcFailoverPolicy failoverPolicy,
        RpcClient... destinations
    ) {
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
            Map<String, List<RpcClient>> dataCenters) {
        this(
                failoverTimeout,
                globalTimeout,
                pingTimeout,
                connector,
                failoverPolicy,
                dataCenter,
                dataCenters,
                BalancingDestinationMetricsHolderImpl.INSTANCE,
                BalancingResponseHandlerMetricsHolderImpl.INSTANCE,
                DataCenterMetricsHolderImpl.INSTANCE
        );
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
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
        DataCenterMetricsHolder dataCenterMetricsHolder
    ) {
        assert failoverTimeout.compareTo(globalTimeout) <= 0;

        this.failoverPolicy = failoverPolicy;
        this.executorService = connector.eventLoopGroup();
        this.dataCenters = new DataCenter[dataCenters.size()];
        this.localDataCenter = null;

        int i = 0;
        for (Map.Entry<String, List<RpcClient>> entity : dataCenters.entrySet()) {
            String dcName = entity.getKey();
            List<RpcClient> clients = entity.getValue();

            DataCenter dc = new DataCenter(
                    dcName,
                    clients,
                    -1.0,
                    dataCenterMetricsHolder,
                    new RpcOptions()
            );

            this.dataCenters[i] = dc;
            if (dcName.equals(dataCenter)) {
                this.localDataCenter = dc;
                this.dataCenters[i] = this.dataCenters[0];
                this.dataCenters[0] = this.localDataCenter;
            }
            i++;
        }
    }

    @Override
    public void ref() {
        throw new IllegalStateException("Not implemented for deprecated BalancingRpcClient");
    }

    @Override
    public void unref() {
        throw new IllegalStateException("Not implemented for deprecated BalancingRpcClient");
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
        List<RpcClient> r = Manifold.selectDestinations(
                dataCenters,
                1,
                localDataCenter != null,
                rnd,
                !failoverPolicy.randomizeDcs()
        );
        if (r.isEmpty()) {
            return null;
        } else {
            return r.get(0);
        }
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient unused,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        throw new IllegalArgumentException();
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        List<RpcClient> destinations = Manifold.selectDestinations(
                dataCenters,
                3,
                localDataCenter != null,
                rnd,
                !failoverPolicy.randomizeDcs()
        );
        return FailoverRpcExecutor.execute(
                executorService,
                RpcClientPool.collectionPool(destinations),
                request,
                handler,
                options,
                destinations.size());
    }

    public String destinationName() {
        return "multidestination";
    }

    @Override
    public String getAddressString() {
        return null;
    }

    @Override
    public ScheduledExecutorService executor() {
        return executorService;
    }
}
