package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.BalancingDestination;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClient extends DestinationsSelector implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    private final List<PeriodicDiscovery> discovery;
    private final Random rnd = new Random();

    final private DataCenter[] dataCenters;
    final private ScheduledExecutorService executorService;
    final private RpcOptions options;
    final private DataCenter localDataCenter;

    final private LinkedList<CompletableFuture<Void>> waiting = new LinkedList<>();
    final private ConcurrentHashMap<PeriodicDiscoveryListener, Boolean> discoveriesFailed = new ConcurrentHashMap<>();


    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, null, credentials, options);
    }

    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            String proxyRole,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, proxyRole, credentials, new RpcCompression(), options);
    }

    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            String proxyRole,
            RpcCredentials credentials,
            RpcCompression compression,
            RpcOptions options) {
        super(options);
        discovery = new ArrayList<>();

        this.dataCenters = new DataCenter[clusters.size()];
        this.executorService = connector.eventLoopGroup();
        this.options = options;

        int dataCenterIndex = 0;

        DataCenter localDataCenter = null;

        try {
            for (YtCluster entry : clusters) {
                final String dataCenterName = entry.name;

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
                        wakeUp();
                    }

                    @Override
                    public void onError(Throwable e) {
                        discoveriesFailed.put(this, true);
                        if (discoveriesFailed.size() == clusters.size()) {
                            wakeUp(e);
                        }
                    }

                    @Override
                    public void onProxiesRemoved(Set<RpcClient> proxies) {
                        dc.removeProxies(proxies);
                    }
                };

                discovery.add(
                        new PeriodicDiscovery(
                                dataCenterName,
                                entry.addresses,
                                entry.proxyRole.orElse(proxyRole),
                                String.format("%s:%d", entry.balancerFqdn, entry.httpPort),
                                connector,
                                options,
                                credentials,
                                compression,
                                listener));
            }

            for (PeriodicDiscovery discovery : discovery) {
                discovery.start();
            }
        } catch (Throwable e) {
            logger.error("Cannot start periodic discovery", e);
            IoUtils.closeQuietly(this);
            throw e;
        }

        this.localDataCenter = localDataCenter;

        try {
            schedulePing();
        } catch (Throwable e) {
            logger.error("Cannot schedule ping", e);
            IoUtils.closeQuietly(this);
            throw e;
        }
    }

    public YtClient(
            BusConnector connector,
            YtCluster cluster,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, Cf.list(cluster), cluster.name, credentials, options);
    }

    public YtClient(
            BusConnector connector,
            String clusterName,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, new YtCluster(clusterName), credentials, options);
    }

    public YtClient(
            BusConnector connector,
            String clusterName,
            RpcCredentials credentials) {
        this(connector, clusterName, credentials, new RpcOptions());
    }

    private void wakeUp()
    {
        synchronized (waiting) {
            while (!waiting.isEmpty()) {
                waiting.pop().complete(null);
            }
        }
    }

    private void wakeUp(Throwable e) {
        synchronized (waiting) {
            while (!waiting.isEmpty()) {
                waiting.pop().completeExceptionally(e);
            }
        }
    }

    public CompletableFuture<Void> waitProxies() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (waiting) {
            // TODO: fix posible memleak here
            waiting.push(future);
        }

        int proxies = 0;
        for (DataCenter dataCenter: dataCenters) {
            proxies += dataCenter.getAliveDestinations().size();
        }
        if (proxies > 0) {
            return CompletableFuture.completedFuture(null);
        } else if (discoveriesFailed.size() == dataCenters.length) {
            future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("cannot initialize proxies"));
            return future;
        } else {
            return future;
        }
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

    public List<RpcClient> selectDestinations() {
        return Manifold.selectDestinations(
                dataCenters, 3,
                localDataCenter != null,
                rnd,
                !options.getFailoverPolicy().randomizeDcs());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<ResponseType> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder) {
        return builder.invokeVia(selectDestinations());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType> RpcClientStreamControl startStream(
            RpcClientRequestBuilder<RequestType, ResponseType> builder) {
        return builder.startStream(selectDestinations());
    }
}
