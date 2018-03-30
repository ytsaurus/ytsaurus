package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.BalancingDestination;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClient extends ApiServiceClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    private final List<PeriodicDiscovery> discovery; // TODO: stop
    private final Random rnd = new Random();

    final private DataCenter[] dataCenters;
    final private ScheduledExecutorService executorService;
    final private RpcOptions options;
    final private DataCenter localDataCenter;

    public YtClient(
           BusConnector connector,
           Map<String, List<String>> initialAddresses,
           String localDataCenterName,
           RpcCredentials credentials,
           RpcOptions options)
    {
        super(options);
        discovery = new ArrayList<>();

        this.dataCenters = new DataCenter[initialAddresses.size()];
        this.executorService = connector.executorService();
        this.options = options;

        int dataCenterIndex = 0;

        DataCenter localDataCenter = null;

        for (Map.Entry<String, List<String>> entry : initialAddresses.entrySet()) {
            final String dataCenterName = entry.getKey();

            final DataCenter dc = new DataCenter(
                    dataCenterName,
                    new BalancingDestination[0],
                    -1.0,
                    options);

            dataCenters[dataCenterIndex++] = dc;

            if (dataCenterName.equals(localDataCenterName)) {
                localDataCenter = dc;
            }

            final PeriodicDiscoveryListener listener = new PeriodicDiscoveryListener() {
                @Override
                public void onProxiesAdded(Set<RpcClient> proxies) {
                    dc.addProxies(proxies);
                }

                @Override
                public void onProxiesRemoved(Set<RpcClient> proxies) {
                    dc.removeProxies(proxies);
                }
            };

            discovery.add(
                    new PeriodicDiscovery(
                            dataCenterName,
                            entry.getValue(),
                            connector,
                            options,
                            credentials,
                            listener));
        }

        this.localDataCenter = localDataCenter;

        schedulePing();
    }

    public YtClient(
            BusConnector connector,
            List<String> addresses,
            RpcCredentials credentials,
            RpcOptions options)
    {
        this(connector, Cf.map("unknown", addresses), "unknown", credentials, options);
    }

    @Override
    public void close() {
        for (PeriodicDiscovery disco : discovery) {
            disco.close();
        }

        for (DataCenter dc : dataCenters) {
            dc.close();
        }
    }

    private void schedulePing() {
        executorService.schedule(
                this::pingDataCenters,
                options.getPingTimeout().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void pingDataCenters() {
        logger.debug("ping");

        CompletableFuture<Void> futures[] = new CompletableFuture[dataCenters.length];
        int i = 0;
        for (DataCenter entry : dataCenters) {
            futures[i++] = entry.ping(executorService, options.getPingTimeout());
        }

        schedulePing();
    }

    public Map<String, List<ApiServiceClient>> getAliveDestinations() {
        final Map<String, List<ApiServiceClient>> result = new HashMap<>();
        for (DataCenter dc : dataCenters) {
            result.put(dc.getName(), dc.getAliveDestinations(BalancingDestination::getService));
        }
        return result;
    }

    private List<RpcClient> selectDestinations() {
        return Manifold.selectDestinations(
                dataCenters, 3,
                localDataCenter != null,
                rnd,
                ! options.getFailoverPolicy().randomizeDcs());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<ResponseType> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder)
    {
        return builder.invokeVia(selectDestinations());
    }
}
