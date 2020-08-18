package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.MessageLite;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactoryImpl;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class YtClient extends DestinationsSelector implements AutoCloseable {
    private static final Object KEY = new Object();

    private final ScheduledExecutorService executor;
    private final ClientPoolProvider poolProvider;

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

        this.executor = connector.executorService();

        RpcClientFactory rpcClientFactory = new RpcClientFactoryImpl(connector, credentials, compression);
        poolProvider = new OldClientPoolProvider(
                connector,
                clusters,
                localDataCenterName,
                proxyRole,
                rpcClientFactory,
                credentials,
                compression,
                options);
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

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public CompletableFuture<Void> waitProxies() {
        return poolProvider.waitProxies();
    }

    @Override
    public void close() {
        poolProvider.close();
    }

    @SuppressWarnings("unused")
    public Map<String, List<ApiServiceClient>> getAliveDestinations() {
        return poolProvider.getAliveDestinations();
    }

    @Override
    public RpcClientPool getClientPool() {
        return poolProvider.getClientPool();
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<ResponseType> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder)
    {
        return builder.invokeVia(executor, poolProvider.getClientPool());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType>
    CompletableFuture<RpcClientStreamControl> startStream(RpcClientRequestBuilder<RequestType, ResponseType> builder) {
        return builder.startStream(executor, poolProvider.getClientPool());
    }

    private interface ClientPoolProvider extends AutoCloseable {
        void close();
        CompletableFuture<Void> waitProxies();
        Map<String, List<ApiServiceClient>> getAliveDestinations();
        RpcClientPool getClientPool();
    }

    @NonNullApi
    static class OldClientPoolProvider implements ClientPoolProvider {
        private final DataCenter[] dataCenters;
        private final List<PeriodicDiscovery> discovery;
        private final DataCenter localDataCenter;
        private final RpcOptions options;
        private final Random random = new Random();
        private final ConcurrentHashMap<PeriodicDiscoveryListener, Boolean> discoveriesFailed = new ConcurrentHashMap<>();
        private final CompletableFuture<Void> waiting = new CompletableFuture<>();

        /*
         * Кэширующая обертка для ytClient. Обертка кэширует бэкенды, которые доступны для запроса.
         * Это позволяет избежать постоянные блокировки.
         * Помимо этого обертка убирает копирование хостов на каждый запрос и делает возможность ретраить запрос больше 3 раз.
         *
         * Включается опцией useClientsCache = true и clientsCacheSize > 0
         */
        private final @Nullable LoadingCache<Object, List<RpcClient>> clientsCache;

        OldClientPoolProvider(
                BusConnector connector,
                List<YtCluster> clusters,
                String localDataCenterName,
                @Nullable String proxyRole,
                RpcClientFactory rpcClientFactory,
                RpcCredentials credentials,
                RpcCompression compression,
                RpcOptions options)
        {
            dataCenters = new DataCenter[clusters.size()];
            discovery = new ArrayList<>();
            this.options = options;

            if (options.getUseClientsCache() && options.getClientsCacheSize() > 0
                    && options.getClientCacheExpiration() != null && options.getClientCacheExpiration().toMillis() > 0) {
                this.clientsCache = CacheBuilder.newBuilder()
                        .maximumSize(options.getClientsCacheSize())
                        .expireAfterAccess(options.getClientCacheExpiration().toMillis(), TimeUnit.MILLISECONDS)
                        .build(CacheLoader.from(() -> {
                            final List<RpcClient> clients = Arrays.stream(dataCenters)
                                    .map(DataCenter::getAliveDestinations)
                                    .flatMap(Collection::stream)
                                    .collect(Collectors.toList());
                            Collections.shuffle(clients, random);
                            return new RandomList<>(random, clients); // TODO: Временное решение, будет исправлено позже
                        }));
            } else {
                this.clientsCache = null;
            }

            int dataCenterIndex = 0;
            DataCenter tmpLocalDataCenter = null;
            for (YtCluster entry : clusters) {
                final String dataCenterName = entry.name;

                final DataCenter currentDataCenter = new DataCenter(dataCenterName, -1.0, options);

                dataCenters[dataCenterIndex++] = currentDataCenter;

                if (dataCenterName.equals(localDataCenterName)) {
                    tmpLocalDataCenter = currentDataCenter;
                }

                final PeriodicDiscoveryListener listener = new PeriodicDiscoveryListener() {
                    @Override
                    public void onProxiesSet(Set<HostPort> proxies) {
                        if (!proxies.isEmpty()) {
                            currentDataCenter.setProxies(proxies, rpcClientFactory, random);
                            waiting.complete(null);
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        discoveriesFailed.put(this, true);
                        if (discoveriesFailed.size() == clusters.size()) {
                            waiting.completeExceptionally(e);
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
            this.localDataCenter = tmpLocalDataCenter;
        }

        @Override
        public RpcClientPool getClientPool() {
            return RpcClientPool.collectionPool(selectDestinations());
        }

        @Override
        public Map<String, List<ApiServiceClient>> getAliveDestinations() {
            final Map<String, List<ApiServiceClient>> result = new HashMap<>();
            for (DataCenter dc : dataCenters) {
                result.put(dc.getName(), dc.getAliveDestinations(slot -> new ApiServiceClient(slot.getClient(), options)));
            }
            return result;
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

        @Override
        public CompletableFuture<Void> waitProxies() {
            return waitProxiesImpl().thenRun(clientsCache != null ?
                    clientsCache::invalidateAll : () -> {
            });
        }

        private CompletableFuture<Void> waitProxiesImpl() {
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

        private List<RpcClient> selectDestinations() {
            if (clientsCache != null) {
                return clientsCache.getUnchecked(KEY);
            } else {
                return Manifold.selectDestinations(
                        dataCenters, 3,
                        localDataCenter != null,
                        random,
                        !options.getFailoverPolicy().randomizeDcs());
            }
        }
    }
}

class RandomList<T> implements List<T> {
    private final Random random;
    private final List<T> data;

    public RandomList(final Random random, final List<T> data) {
        this.random = random;
        this.data = data;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return data.contains(o);
    }

    @Nonnull
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !isEmpty();
            }

            @Override
            public T next() {
                // NB. Base class implementation returns random element each time.
                return get(0);
            }
        };
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T1> T1[] toArray(@Nonnull final T1[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(final T t) {
        return true;
    }

    @Override
    public boolean remove(final Object o) {
        return true;
    }

    @Override
    public boolean containsAll(@Nonnull final Collection<?> c) {
        return data.containsAll(c);
    }

    @Override
    public boolean addAll(@Nonnull final Collection<? extends T> c) {
        return true;
    }

    @Override
    public boolean addAll(final int index, @Nonnull final Collection<? extends T> c) {
        return true;
    }

    @Override
    public boolean removeAll(@Nonnull final Collection<?> c) {
        return true;
    }

    @Override
    public boolean retainAll(@Nonnull final Collection<?> c) {
        return true;
    }

    @Override
    public void clear() {
    }

    @Override
    public T get(final int index) {
        return data.get(random.nextInt(data.size()));
    }

    @Override
    public T set(final int index, final T element) {
        return null;
    }

    @Override
    public void add(final int index, final T element) {
    }

    @Override
    public T remove(final int index) {
        return null;
    }

    @Override
    public int indexOf(final Object o) {
        return data.indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
        return data.lastIndexOf(o);
    }


    @Nonnull
    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }


    @Nonnull
    @Override
    public ListIterator<T> listIterator(final int index) {
        throw new UnsupportedOperationException();
    }


    @Nonnull
    @Override
    public List<T> subList(final int fromIndex, final int toIndex) {
        return this;
    }
}
