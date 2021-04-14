package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.MessageLite;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.internal.DataCenter;
import ru.yandex.yt.ytclient.proxy.internal.DiscoveryMethod;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.Manifold;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolderImpl;

/**
 *  Asynchronous YT client.
 *  <p>
 *      <b>WARNING</b> Callbacks that <b>can block</b> (e.g. they use {@link CompletableFuture#join})
 *      <b>MUST NEVER BE USED</b> with non-Async thenApply, whenComplete, etc methods
 *      called on futures returned by this client. Otherwise deadlock may appear.
 *      Always use Async versions of these methods with blocking callbacks.
 *  <p>
 *      Explanation. When using non-async thenApply callback is invoked by the thread that sets the future.
 *      In our case it is internal thread of YtClient.
 *      When all internal threads of YtClient are blocked by such callbacks
 *      YtClient becomes unable to send requests and receive responses.
 */
public class YtClient extends CompoundClient {
    private static final Object KEY = new Object();

    private final BusConnector busConnector;
    private final boolean isBusConnectorOwner;
    private final ScheduledExecutorService executor;
    private final ClientPoolProvider poolProvider;

    static public Builder builder() {
        return new Builder();
    }

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
            RpcOptions options)
    {
        this(
                new BuilderWithDefaults(
                    builder()
                            .setSharedBusConnector(connector)
                            .setClusters(clusters)
                            .setPreferredClusterName(localDataCenterName)
                            .setProxyRole(proxyRole)
                            .setRpcCredentials(credentials)
                            .setRpcCompression(compression)
                            .setRpcOptions(options)
                            // old constructors did not validate their arguments
                            .disableValidation()
                    )
        );
    }

    public YtClient(
            BusConnector connector,
            YtCluster cluster,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, Cf.list(cluster), cluster.getName(), credentials, options);
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
        if (isBusConnectorOwner) {
            busConnector.close();
        }
    }

    @SuppressWarnings("unused")
    public Map<String, List<ApiServiceClient>> getAliveDestinations() {
        return poolProvider.getAliveDestinations();
    }

    public RpcClientPool getClientPool() {
        return poolProvider.getClientPool();
    }

    @Deprecated
    List<RpcClient> selectDestinations() {
        return poolProvider.oldSelectDestinations();
    }

    /**
     * Method useful in tests. Allows to asynchronously ban client.
     *
     * @return future on number of banned proxies.
     */
    CompletableFuture<Void> banProxy(String address) {
        return poolProvider.banClient(address).thenApply((bannedCount) -> {
            if (bannedCount == 0) {
                throw new RuntimeException("Cannot ban proxy " + address + " since it is not known");
            }
            return null;
        });
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<ResponseType> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder)
    {
        return builder.invokeVia(executor, poolProvider.getClientPool());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType>
    CompletableFuture<RpcClientStreamControl> startStream(
            RpcClientRequestBuilder<RequestType, ResponseType> builder,
            RpcStreamConsumer consumer)
    {
        return builder.startStream(executor, poolProvider.getClientPool(), consumer);
    }

    private YtClient(BuilderWithDefaults builder) {
        super(builder.busConnector.executorService(), builder.builder.options);

        builder.builder.validate();

        this.busConnector = builder.busConnector;
        this.isBusConnectorOwner = builder.builder.isBusConnectorOwner;
        this.executor = busConnector.executorService();

        final RpcClientFactory rpcClientFactory = new RpcClientFactoryImpl(
                busConnector,
                builder.credentials,
                builder.builder.compression);

        if (builder.builder.options.isNewDiscoveryServiceEnabled()) {
            poolProvider = new NewClientPoolProvider(
                    busConnector,
                    builder.builder.clusters,
                    builder.builder.preferredClusterName,
                    builder.builder.proxyRole,
                    rpcClientFactory,
                    builder.credentials,
                    builder.builder.options);
        } else {
            poolProvider = new OldClientPoolProvider(
                    busConnector,
                    builder.builder.clusters,
                    builder.builder.preferredClusterName,
                    builder.builder.proxyRole,
                    rpcClientFactory,
                    builder.credentials,
                    builder.builder.compression,
                    builder.builder.options);
        }
    }

    private interface ClientPoolProvider extends AutoCloseable {
        void close();
        CompletableFuture<Void> waitProxies();
        Map<String, List<ApiServiceClient>> getAliveDestinations();
        List<RpcClient> oldSelectDestinations();
        RpcClientPool getClientPool();
        CompletableFuture<Integer> banClient(String address);
    }

    @NonNullApi
    static class NewClientPoolProvider implements ClientPoolProvider {
        final MultiDcClientPool multiDcClientPool;
        final List<ClientPoolService> dataCenterList = new ArrayList<>();
        final String localDcName;
        final RpcOptions options;

        NewClientPoolProvider(
            BusConnector connector,
            List<YtCluster> clusters,
            @Nullable String localDataCenterName,
            @Nullable String proxyRole,
            RpcClientFactory rpcClientFactory,
            RpcCredentials credentials,
            RpcOptions options)
        {
            this.options = options;
            this.localDcName = localDataCenterName;

            final EventLoopGroup eventLoopGroup = connector.eventLoopGroup();
            final Random random = new Random();

            for (YtCluster curCluster : clusters) {
                // 1. Понять http-discovery или rpc
                if (curCluster.balancerFqdn != null && !curCluster.balancerFqdn.isEmpty() && (
                        options.getPreferableDiscoveryMethod() == DiscoveryMethod.HTTP
                                || curCluster.addresses == null
                                || curCluster.addresses.isEmpty()))
                {
                    // Use http
                    dataCenterList.add(
                            ClientPoolService.httpBuilder()
                                    .setDataCenterName(curCluster.getName())
                                    .setBalancerAddress(curCluster.balancerFqdn, curCluster.httpPort)
                                    .setRole(proxyRole)
                                    .setOptions(options)
                                    .setClientFactory(rpcClientFactory)
                                    .setEventLoop(eventLoopGroup)
                                    .setRandom(random)
                                    .build()
                    );
                } else if (
                        curCluster.addresses != null && !curCluster.addresses.isEmpty() && (
                        options.getPreferableDiscoveryMethod() == DiscoveryMethod.HTTP
                        || curCluster.balancerFqdn == null || curCluster.balancerFqdn.isEmpty()))
                {
                    // use rpc
                    List<HostPort> initialProxyList =
                            curCluster.addresses.stream().map(HostPort::parse).collect(Collectors.toList());
                    dataCenterList.add(
                            ClientPoolService.rpcBuilder()
                                    .setDataCenterName(curCluster.getName())
                                    .setInitialProxyList(initialProxyList)
                                    .setRole(proxyRole)
                                    .setOptions(options)
                                    .setClientFactory(rpcClientFactory)
                                    .setEventLoop(eventLoopGroup)
                                    .setRandom(random)
                                    .build()
                    );
                } else {
                    throw new RuntimeException(String.format(
                            "Cluster %s does not have neither http balancer nor rpc proxies specified ",
                            curCluster.getName()
                    ));
                }
            }

            for (ClientPoolService clientPoolService : dataCenterList) {
                clientPoolService.start();
            }

            multiDcClientPool = MultiDcClientPool.builder()
                    .setLocalDc(localDataCenterName)
                    .addClientPools(dataCenterList)
                    .setDcMetricHolder(DataCenterMetricsHolderImpl.instance)
                    .build();
        }

        @Override
        public void close() {
            for (ClientPoolService dataCenter : dataCenterList) {
                dataCenter.close();
            }
        }

        @Override
        public CompletableFuture<Void> waitProxies() {
            CompletableFuture<Void> result = new CompletableFuture<>();
            CompletableFuture<RpcClient> client = multiDcClientPool.peekClient(result);
            client.whenComplete((c, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(null);
                }
            });
            RpcUtil.relayCancel(result, client);
            return result;
        }

        @Override
        public Map<String, List<ApiServiceClient>> getAliveDestinations() {
            Map<String, List<ApiServiceClient>> result = new HashMap<>();

            CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
            try {
                for (ClientPoolService clientPoolService : dataCenterList) {
                    RpcClient[] aliveClients = clientPoolService.getAliveClients();
                    if (aliveClients.length == 0) {
                        continue;
                    }
                    List<ApiServiceClient> clients =
                            result.computeIfAbsent(clientPoolService.getDataCenterName(), k -> new ArrayList<>());
                    for (RpcClient curClient : aliveClients) {
                        clients.add(new ApiServiceClient(curClient, options));
                    }
                }
            } finally {
                releaseFuture.complete(null);
            }
            return result;
        }

        @Override
        public List<RpcClient> oldSelectDestinations() {
            final int RESULT_SIZE = 3;
            List<RpcClient> result = new ArrayList<>();

            Consumer<ClientPoolService> processClientPoolService = (ClientPoolService service) -> {
                RpcClient[] aliveClients = service.getAliveClients();
                for (RpcClient c : aliveClients) {
                    if (result.size() >= RESULT_SIZE) {
                        break;
                    }
                    result.add(c);
                }
            };

            for (ClientPoolService clientPoolService : dataCenterList) {
                if (clientPoolService.getDataCenterName().equals(localDcName)) {
                    processClientPoolService.accept(clientPoolService);
                }
            }

            for (ClientPoolService clientPoolService : dataCenterList) {
                if (!clientPoolService.getDataCenterName().equals(localDcName)) {
                    processClientPoolService.accept(clientPoolService);
                }
            }
            return result;
        }

        @Override
        public RpcClientPool getClientPool() {
            return multiDcClientPool;
        }

        @Override
        public CompletableFuture<Integer> banClient(String address) {
            return multiDcClientPool.banClient(address);
        }
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
                @Nullable String localDataCenterName,
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
                                entry.getClusterUrl(),
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
        public List<RpcClient> oldSelectDestinations() {
            return selectDestinations();
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

        @Override
        public CompletableFuture<Integer> banClient(String address) {
            throw new RuntimeException("not implemented");
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

    @NonNullFields
    @NonNullApi
    public static class Builder {
        @Nullable BusConnector busConnector;
        boolean isBusConnectorOwner = true;
        List<YtCluster> clusters = new ArrayList<>();
        @Nullable String preferredClusterName;
        @Nullable String proxyRole;
        @Nullable RpcCredentials credentials;
        RpcCompression compression = new RpcCompression();
        RpcOptions options = new RpcOptions();
        boolean enableValidation = true;

        Builder() {
        }

        /**
         * Set BusConnector for YT client.
         *
         * <p>
         * Connector will be owned by YtClient.
         * YtClient will close it when {@link YtClient#close()} is called.
         *
         * <p>
         * If bus is never set default bus will be created
         * (default bus will be owned by YtClient so you don't need to worry about closing it).
         */
        public Builder setOwnBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = true;
            return this;
        }

        /**
         * Set BusConnector for YT client.
         *
         * <p>
         * Connector will not be owned by YtClient. It's user responsibility to close the connector.
         *
         * @see #setOwnBusConnector
         */
        public Builder setSharedBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = false;
            return this;
        }

        /**
         * Create and use default connector with specified working thread count.
         */
        public Builder setDefaultBusConnectorWithThreadCount(int threadCount) {
            setOwnBusConnector(new DefaultBusConnector(new NioEventLoopGroup(threadCount), true));
            isBusConnectorOwner = true;
            return this;
        }

        /**
         * Set YT cluster to use.
         *
         * @param cluster address of YT cluster http balancer, examples:
         *                "hahn"
         *                "arnold.yt.yandex.net"
         *                "localhost:8054"
         */
        public Builder setCluster(String cluster) {
            return setClusters(cluster);
        }

        /**
         * Set YT cluster to use.
         *
         * <p>
         * Similar to {@link #setCluster(String)} but allows to create connections to several clusters.
         * YtClient will chose cluster to send requests based on cluster availability and their ping.
         */
        public Builder setClusters(String firstCluster, String... rest) {
            List<YtCluster> ytClusters = new ArrayList<>();
            ytClusters.add(new YtCluster(YtCluster.normalizeName(firstCluster)));
            for (String clusterName : rest) {
                ytClusters.add(new YtCluster(YtCluster.normalizeName(clusterName)));
            }
            return setClusters(ytClusters);
        }

        /**
         * Set YT clusters to use.
         *
         * <p>
         * Similar to {@link #setClusters(String, String...)} but allows finer configuration.
         */
        public Builder setClusters(List<YtCluster> clusters) {
            this.clusters = clusters;
            return this;
        }

        /**
         * Set name of preferred cluster
         *
         * <p>
         * When YT is configured to use multiple clusters and preferred cluster is set
         * it will be used for all requests unless it's unavailable.
         *
         * <p>
         * If preferred cluster is not set or is set but unavailable YtClient choses
         * cluster based on network metrics.
         */
        public Builder setPreferredClusterName(@Nullable String preferredClusterName) {
            this.preferredClusterName = YtCluster.normalizeName(preferredClusterName);
            return this;
        }

        /**
         * Set proxy role to use.
         *
         * <p>
         * Projects that have dedicated proxies should use this option so YtClient will use them.
         * If no proxy role is specified default proxies will be used.
         */
        public Builder setProxyRole(@Nullable String proxyRole) {
            this.proxyRole = proxyRole;
            return this;
        }

        /**
         * Set authentication information i.e. user name and user token.
         *
         * <p>
         * When no rpc credentials is set they are loaded from environment.
         * @see RpcCredentials#loadFromEnvironment()
         */
        public Builder setRpcCredentials(RpcCredentials rpcCredentials) {
            this.credentials = rpcCredentials;
            return this;
        }

        /**
         * Set compression to be used for requests and responses.
         *
         * <p>
         * If it's not specified no compression will be used.
         */
        public Builder setRpcCompression(RpcCompression rpcCompression) {
            this.compression = rpcCompression;
            return this;
        }

        /**
         * Set miscellaneous options.
         */
        public Builder setRpcOptions(RpcOptions rpcOptions) {
            this.options = rpcOptions;
            return this;
        }

        /**
         * Finally create a client.
         */
        public YtClient build() {
            return new YtClient(new BuilderWithDefaults(this));
        }

        void validate() {
            if (!enableValidation) {
                return;
            }

            if (clusters.isEmpty()) {
                throw new IllegalArgumentException("No YT cluster specified");
            }
            {
                // Check cluster uniqueness.
                Set<String> clusterNames = new HashSet<>();
                boolean foundPreferredCluster = false;
                for (YtCluster cluster : clusters) {
                    if (clusterNames.contains(cluster.name)) {
                        throw new IllegalArgumentException(
                                String.format("Cluster %s is specified multiple times",
                                        cluster.name));
                    }
                    clusterNames.add(cluster.name);
                    if (cluster.name.equals(preferredClusterName)) {
                        foundPreferredCluster = true;
                    }
                }

                if (preferredClusterName != null && !foundPreferredCluster) {
                    throw new IllegalArgumentException(
                            String.format("Preferred cluster %s is not found among specified clusters",
                                    preferredClusterName));
                }
            }
        }

        Builder disableValidation() {
            enableValidation = false;
            return this;
        }
    }

    // Class is able to initialize nonset fields of builder with reasonable defaults. Keep in mind that:
    //   1. We cannot initialize this fields in YtClient constructor
    //   because busConnector is required to initialize superclass.
    //   2. We don't want to call initialization code if user specified explicitly values
    //   (initialization code is looking at environment and can fail and it starts threads)
    //   3. Its better not to touch and modify builder instance since it's theoretically can be used by
    //   user to initialize another YtClient.
    @NonNullFields
    private static class BuilderWithDefaults {
        final Builder builder;
        final BusConnector busConnector;
        final RpcCredentials credentials;

        BuilderWithDefaults(Builder builder) {
            this.builder = builder;

            if (builder.busConnector != null) {
                busConnector = builder.busConnector;
            } else {
                busConnector = new DefaultBusConnector();
            }

            if (builder.credentials != null) {
                credentials = builder.credentials;
            } else {
                credentials = RpcCredentials.loadFromEnvironment();
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
