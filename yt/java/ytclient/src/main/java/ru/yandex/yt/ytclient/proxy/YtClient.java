package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.misc.RandomList;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactoryImpl;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClient extends DestinationsSelector implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    private static final Object KEY = new Object();

    private final List<PeriodicDiscovery> discovery;
    private final Random rnd = new Random();

    private final DataCenter[] dataCenters;
    private final RpcOptions options;
    private final DataCenter localDataCenter;

    private final CompletableFuture<Void> waiting = new CompletableFuture<>();
    private final ConcurrentHashMap<PeriodicDiscoveryListener, Boolean> discoveriesFailed = new ConcurrentHashMap<>();

    /*
     * Кэширующая обертка для ytClient. Обертка кэширует бэкенды, которые доступны для запроса.
     * Это позволяет избежать постоянные блокировки.
     * Помимо этого обертка убирает копирование хостов на каждый запрос и делает возможность ретраить запрос больше 3 раз.
     *
     * Включается опцией useClientsCache = true и clientsCacheSize > 0
     */
    private final LoadingCache<Object, List<RpcClient>> clientsCache;

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
        this.options = options;

        if (options.getUseClientsCache() && options.getClientsCacheSize() > 0
                && options.getClientCacheExpiration() != null && options.getClientCacheExpiration().toMillis() > 0) {
            this.clientsCache = CacheBuilder.newBuilder()
                    .maximumSize(options.getClientsCacheSize())
                    .expireAfterAccess(options.getClientCacheExpiration().toMillis(), TimeUnit.MILLISECONDS)
                    .build(CacheLoader.from(() -> {
                        final List<RpcClient> clients = Arrays.stream(getDataCenters())
                                .map(DataCenter::getAliveDestinations)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                        return new RandomList<>(rnd, clients); // TODO: Временное решение, будет исправлено позже
                    }));
        } else {
            this.clientsCache = null;
        }

        int dataCenterIndex = 0;

        DataCenter localDataCenter = null;

        RpcClientFactory rpcClientFactory = new RpcClientFactoryImpl(connector, credentials, compression);
        Random rnd = new Random();

        try {
            for (YtCluster entry : clusters) {
                final String dataCenterName = entry.name;

                final DataCenter dc = new DataCenter(dataCenterName, -1.0, options);

                dataCenters[dataCenterIndex++] = dc;

                if (dataCenterName.equals(localDataCenterName)) {
                    localDataCenter = dc;
                }

                final PeriodicDiscoveryListener listener = new PeriodicDiscoveryListener() {
                    @Override
                    public void onProxiesSet(Set<HostPort> proxies) {
                        if (!proxies.isEmpty()) {
                            dc.setProxies(proxies, rpcClientFactory, rnd);
                            wakeUp();
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        discoveriesFailed.put(this, true);
                        if (discoveriesFailed.size() == clusters.size()) {
                            wakeUp(e);
                        }
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

    private void wakeUp() {
        waiting.complete(null);
    }

    private void wakeUp(Throwable e) {
        waiting.completeExceptionally(e);
    }

    public CompletableFuture<Void> waitProxies() {
        return waitProxiesImpl().thenRun(clientsCache != null ?
                clientsCache::invalidateAll : () -> {
        });
    }

    public CompletableFuture<Void> waitProxiesImpl() {
        int proxies = 0;
        for (DataCenter dataCenter : dataCenters) {
            proxies += dataCenter.getAliveDestinations().size();
        }
        if (proxies > 0) {
            return CompletableFuture.completedFuture(null);
        } else if (discoveriesFailed.size() == dataCenters.length) {
            waiting.completeExceptionally(new IllegalStateException("cannot initialize proxies"));
            return waiting;
        } else {
            return waiting;
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

    public Map<String, List<ApiServiceClient>> getAliveDestinations() {
        final Map<String, List<ApiServiceClient>> result = new HashMap<>();
        for (DataCenter dc : dataCenters) {
            result.put(dc.getName(), dc.getAliveDestinations(slot -> new ApiServiceClient(slot.getClient(), options)));
        }
        return result;
    }

    public List<RpcClient> selectDestinations() {
        if (clientsCache != null) {
            return clientsCache.getUnchecked(KEY);
        } else {
            return Manifold.selectDestinations(
                    dataCenters, 3,
                    localDataCenter != null,
                    rnd,
                    !options.getFailoverPolicy().randomizeDcs());
        }
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

    protected DataCenter[] getDataCenters() {
        return this.dataCenters;
    }
}
