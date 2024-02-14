package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.MessageLite;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.DataCenterMetricsHolderImpl;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientPool;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;


/**
 * Asynchronous YT client.
 * <p>
 * <b>WARNING</b> Callbacks that <b>can block</b> (e.g. they use {@link CompletableFuture#join})
 * <b>MUST NEVER BE USED</b> with non-Async (thenApply, whenComplete etc.) methods
 * called on futures returned by this client. Otherwise, deadlock may appear.
 * Always use Async versions of these methods with blocking callbacks.
 * <p>
 * Explanation. When using non-async thenApply callback is invoked by the thread that sets the future.
 * In our case it is internal thread of YTsaurusClient.
 * When all internal threads of YTsaurusClient are blocked by such callbacks
 * YTsaurusClient becomes unable to send requests and receive responses.
 */
public class YTsaurusClient extends CompoundClientImpl implements BaseYTsaurusClient {
    private final BusConnector busConnector;
    private final boolean isBusConnectorOwner;
    private final ScheduledExecutorService executor;
    private final ClientPoolProvider poolProvider;

    private final List<YTsaurusCluster> clusters;

    public YTsaurusClient(
            BusConnector connector,
            YTsaurusCluster cluster,
            YTsaurusClientAuth auth,
            YTsaurusClientConfig config) {
        this(new BuilderWithDefaults<>(
                        new Builder()
                                .setSharedBusConnector(connector)
                                .setClusters(List.of(cluster))
                                .setPreferredClusterName(cluster.getName())
                                .setAuth(auth)
                                .setRpcCompression(new RpcCompression())
                                .setConfig(config)
                ), DefaultSerializationResolver.getInstance()
        );
    }

    public YTsaurusClient(
            BusConnector connector,
            String clusterName,
            YTsaurusClientAuth auth,
            YTsaurusClientConfig config
    ) {
        this(connector, new YTsaurusCluster(clusterName), auth, config);
    }

    public YTsaurusClient(BusConnector connector, String clusterName, YTsaurusClientAuth auth) {
        this(
                connector,
                clusterName,
                auth,
                YTsaurusClientConfig.builder().setRpcOptions(new RpcOptions()).build()
        );
    }

    protected YTsaurusClient(BuilderWithDefaults<?, ?> builder, SerializationResolver serializationResolver) {
        super(
                builder.busConnector.executorService(), builder.builder.config, builder.builder.heavyExecutor,
                serializationResolver
        );

        builder.builder.validate();

        this.busConnector = builder.busConnector;
        this.isBusConnectorOwner = builder.builder.isBusConnectorOwner;
        this.executor = busConnector.executorService();
        this.clusters = builder.builder.clusters;

        OutageController outageController =
                builder.builder.config.getRpcOptions().getTestingOptions().getOutageController();

        final RpcClientFactory rpcClientFactory =
                outageController != null
                        ? new OutageRpcClientFactoryImpl(
                        busConnector,
                        builder.auth,
                        builder.builder.compression,
                        outageController)
                        : new RpcClientFactoryImpl(
                        busConnector,
                        builder.auth,
                        builder.builder.compression);

        this.poolProvider = new ClientPoolProvider(
                busConnector,
                builder.builder.clusters,
                builder.builder.preferredClusterName,
                builder.builder.proxyRole,
                builder.builder.proxyNetworkName,
                builder.builder.config.getUseTLS(),
                builder.builder.config.getTvmOnly(),
                builder.builder.config.getIgnoreBalancers(),
                builder.auth,
                rpcClientFactory,
                builder.builder.config.getRpcOptions(),
                builder.builder.heavyExecutor
        );
    }

    /**
     * Create client with default settings.
     */
    public static YTsaurusClient of(String cluster) {
        return builder()
                .setCluster(cluster)
                .build();
    }

    /**
     * Create builder for YTsaurusClient.
     */
    public static ClientBuilder<? extends YTsaurusClient, ?> builder() {
        return new Builder();
    }

    public List<YTsaurusCluster> getClusters() {
        return clusters;
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
        super.close();
    }

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
    protected <RequestType extends MessageLite.Builder, ResponseType extends MessageLite>
    CompletableFuture<RpcClientResponse<ResponseType>> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder
    ) {
        return builder.invokeVia(executor, poolProvider.getClientPool());
    }

    @Override
    protected <RequestType extends MessageLite.Builder, ResponseType extends MessageLite>
    CompletableFuture<RpcClientStreamControl> startStream(
            RpcClientRequestBuilder<RequestType, ResponseType> builder,
            RpcStreamConsumer consumer
    ) {
        CompletableFuture<Void> clientReleaseFuture = new CompletableFuture<>();
        RpcStreamConsumer wrappedConsumer = new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                consumer.onStartStream(control);
            }

            @Override
            public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
                consumer.onFeedback(sender, header, attachments);
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                consumer.onPayload(sender, header, attachments);
            }

            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                consumer.onResponse(sender, header, attachments);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onError(Throwable cause) {
                consumer.onError(cause);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                consumer.onCancel(cancel);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onWakeup() {
                consumer.onWakeup();
            }
        };
        CompletableFuture<RpcClient> clientFuture = getClientPool().peekClient(clientReleaseFuture);
        return clientFuture.thenApply(
                client -> client.startStream(client, builder.getRpcRequest(), wrappedConsumer, builder.getOptions())
        );
    }

    @NonNullApi
    static class ClientPoolProvider implements AutoCloseable {
        final MultiDcClientPool multiDcClientPool;
        final List<ClientPoolService> dataCenterList = new ArrayList<>();
        final String localDcName;
        final RpcOptions options;
        final Executor heavyExecutor;
        final ScheduledExecutorService executorService;

        @SuppressWarnings("checkstyle:ParameterNumber")
        ClientPoolProvider(
                BusConnector connector,
                List<YTsaurusCluster> clusters,
                @Nullable String localDataCenterName,
                @Nullable String proxyRole,
                @Nullable String proxyNetworkName,
                boolean useTLS,
                boolean tvmOnly,
                boolean ignoreBalancers,
                YTsaurusClientAuth auth,
                RpcClientFactory rpcClientFactory,
                RpcOptions options,
                Executor heavyExecutor
        ) {
            this.options = options;
            this.localDcName = localDataCenterName;
            this.heavyExecutor = heavyExecutor;
            this.executorService = connector.executorService();

            final EventLoopGroup eventLoopGroup = connector.eventLoopGroup();
            final Random random = new Random();

            for (YTsaurusCluster curCluster : clusters) {
                // Find out if we use http or rpc discovery.
                if (curCluster.balancerFqdn != null && !curCluster.balancerFqdn.isEmpty() && (
                        options.getPreferableDiscoveryMethod() == DiscoveryMethod.HTTP
                                || curCluster.addresses == null
                                || curCluster.addresses.isEmpty())
                ) {
                    // Use HTTP.
                    dataCenterList.add(
                            ClientPoolService.httpBuilder()
                                    .setDataCenterName(curCluster.getName())
                                    .setBalancerFqdn(curCluster.balancerFqdn)
                                    .setBalancerPort(curCluster.port)
                                    .setRole(proxyRole)
                                    .setProxyNetworkName(proxyNetworkName)
                                    .setUseTLS(useTLS)
                                    .setTvmOnly(tvmOnly)
                                    .setIgnoreBalancers(ignoreBalancers)
                                    .setToken(auth.getToken().orElse(null))
                                    .setOptions(options)
                                    .setClientFactory(rpcClientFactory)
                                    .setEventLoop(eventLoopGroup)
                                    .setRandom(random)
                                    .build()
                    );
                } else if (
                        curCluster.addresses != null && !curCluster.addresses.isEmpty() && (
                                options.getPreferableDiscoveryMethod() == DiscoveryMethod.RPC
                                        || curCluster.balancerFqdn == null || curCluster.balancerFqdn.isEmpty())
                ) {
                    // Use RPC.
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
                    .setDcMetricHolder(DataCenterMetricsHolderImpl.INSTANCE)
                    .build();
        }

        @Override
        public void close() {
            for (ClientPoolService dataCenter : dataCenterList) {
                dataCenter.close();
            }
        }

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
                        clients.add(new ApiServiceClientImpl(
                                curClient,
                                YTsaurusClientConfig.builder().setRpcOptions(options).build(),
                                heavyExecutor,
                                executorService,
                                DefaultSerializationResolver.getInstance()
                        ));
                    }
                }
            } finally {
                releaseFuture.complete(null);
            }
            return result;
        }

        public List<RpcClient> oldSelectDestinations() {
            final int resultSize = 3;
            List<RpcClient> result = new ArrayList<>();

            Consumer<ClientPoolService> processClientPoolService = (ClientPoolService service) -> {
                RpcClient[] aliveClients = service.getAliveClients();
                for (RpcClient c : aliveClients) {
                    if (result.size() >= resultSize) {
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

        public RpcClientPool getClientPool() {
            return new NonRepeatingClientPool(multiDcClientPool);
        }

        public CompletableFuture<Integer> banClient(String address) {
            return multiDcClientPool.banClient(address);
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BaseBuilder<
            TClient, TBuilder extends BaseBuilder<TClient, TBuilder>> {
        @Nullable
        YTsaurusClientAuth auth;
        RpcCompression compression = new RpcCompression();
        YTsaurusClientConfig config = YTsaurusClientConfig.builder()
                .setRpcOptions(new RpcOptions())
                .build();

        /**
         * Set authentication information.
         *
         * <p>
         * When no {@code YTsaurusClientAuth} is set, username and token are loaded from environment.
         *
         * @see YTsaurusClientAuth#loadUserAndTokenFromEnvironment()
         */
        public TBuilder setAuth(YTsaurusClientAuth auth) {
            this.auth = auth;
            return self();
        }

        /**
         * Set compression to be used for requests and responses.
         * <p>
         * If it's not specified then no compression is used.
         */
        public TBuilder setRpcCompression(RpcCompression rpcCompression) {
            this.compression = rpcCompression;
            return self();
        }

        /**
         * Set miscellaneous options.
         * Part of YtClientConfiguration.
         *
         * @deprecated prefer to use {@link #setConfig(YTsaurusClientConfig)} ()}
         */
        @Deprecated
        public TBuilder setRpcOptions(RpcOptions rpcOptions) {
            this.config = YTsaurusClientConfig.builder()
                    .setRpcOptions(rpcOptions)
                    .build();
            return self();
        }

        /**
         * Set settings of YtClient.
         */
        public TBuilder setConfig(YTsaurusClientConfig config) {
            this.config = config;
            return self();
        }

        protected abstract TBuilder self();

        /**
         * Finally create a client.
         */
        public abstract TClient build();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends ClientBuilder<YTsaurusClient, Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public YTsaurusClient build() {
            return new YTsaurusClient(new BuilderWithDefaults<>(this), DefaultSerializationResolver.getInstance());
        }
    }

    @NonNullFields
    @NonNullApi
    public abstract static class ClientBuilder<
            TClient,
            TBuilder extends ClientBuilder<TClient, TBuilder>>
            extends BaseBuilder<TClient, TBuilder> {
        @Nullable
        BusConnector busConnector;
        boolean isBusConnectorOwner = true;
        @Nullable
        String preferredClusterName;
        @Nullable
        String proxyRole;
        @Nullable
        String proxyNetworkName;

        List<YTsaurusCluster> clusters = new ArrayList<>();
        boolean enableValidation = true;
        Executor heavyExecutor = ForkJoinPool.commonPool();

        protected ClientBuilder() {
        }

        /**
         * Set YTsaurus clusters to use.
         *
         * <p>
         * Similar to {@link #setCluster(String)} but allows to create connections to several clusters.
         * YTsaurusClient will choose cluster to send requests based on cluster availability and their ping.
         */
        public TBuilder setClusters(String firstCluster, String... rest) {
            List<YTsaurusCluster> clustersList = new ArrayList<>();
            clustersList.add(new YTsaurusCluster(YTsaurusCluster.normalizeName(firstCluster)));
            for (String clusterName : rest) {
                clustersList.add(new YTsaurusCluster(YTsaurusCluster.normalizeName(clusterName)));
            }
            return setClusters(clustersList);
        }

        /**
         * Set YTsaurus clusters to use.
         *
         * <p>
         * Similar to {@link #setClusters(String, String...)} but allows finer configuration.
         */
        public TBuilder setClusters(List<YTsaurusCluster> clusters) {
            this.clusters = clusters;
            return self();
        }

        /**
         * Set BusConnector for YTsaurus client.
         *
         * <p>
         * Connector will be owned by YTsaurusClient.
         * YTsaurusClient will close it when {@link YTsaurusClient#close()} is called.
         *
         * <p>
         * If bus is never set default bus will be created
         * (default bus will be owned by YTsaurusClient, so you don't need to worry about closing it).
         */
        public TBuilder setOwnBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = true;
            return self();
        }

        /**
         * Set BusConnector for YT client.
         *
         * <p>
         * Connector will not be owned by YTsaurusClient. It's user responsibility to close the connector.
         *
         * @see #setOwnBusConnector
         */
        public TBuilder setSharedBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = false;
            return self();
        }

        /**
         * Set YT cluster to use.
         *
         * @param cluster address of YT cluster http balancer, examples:
         *                "hahn"
         *                "localhost:8054"
         */
        public TBuilder setCluster(String cluster) {
            setClusters(cluster);
            return self();
        }

        /**
         * Set heavy executor for YT client. This is used for deserialization of lookup/select response.
         * By default, ForkJoinPool.commonPool().
         *
         * @return self
         */
        public TBuilder setHeavyExecutor(Executor heavyExecutor) {
            this.heavyExecutor = heavyExecutor;
            return self();
        }

        /**
         * Create and use default connector with specified working thread count.
         */
        public TBuilder setDefaultBusConnectorWithThreadCount(int threadCount) {
            setOwnBusConnector(new DefaultBusConnector(new NioEventLoopGroup(threadCount), true));
            isBusConnectorOwner = true;
            return self();
        }

        /**
         * Set name of preferred cluster
         *
         * <p>
         * When YT is configured to use multiple clusters and preferred cluster is set
         * it will be used for all requests unless it is unavailable.
         *
         * <p>
         * If preferred cluster is not set or is set but unavailable YTsaurusClient chooses
         * cluster based on network metrics.
         */
        public TBuilder setPreferredClusterName(@Nullable String preferredClusterName) {
            this.preferredClusterName = YTsaurusCluster.normalizeName(preferredClusterName);
            return self();
        }

        /**
         * Set proxy role to use.
         *
         * <p>
         * Projects that have dedicated proxies should use this option so YTsaurusClient will use them.
         * If no proxy role is specified default proxies will be used.
         */
        public TBuilder setProxyRole(@Nullable String proxyRole) {
            this.proxyRole = proxyRole;
            return self();
        }

        /**
         * Set proxy network name to use.
         *
         * <p>
         * If no proxy network name is specified default network name will be used.
         */
        public TBuilder setProxyNetworkName(@Nullable String proxyNetworkName) {
            this.proxyNetworkName = proxyNetworkName;
            return self();
        }

        protected <C, B extends ClientBuilder<C, B>> ClientBuilder<C, B> copyTo(ClientBuilder<C, B> builder) {
            builder.auth = auth;
            builder.compression = compression;
            builder.config = config;

            builder.busConnector = busConnector;
            builder.isBusConnectorOwner = isBusConnectorOwner;
            builder.preferredClusterName = preferredClusterName;
            builder.proxyRole = proxyRole;
            builder.clusters = clusters;
            builder.enableValidation = enableValidation;
            builder.heavyExecutor = heavyExecutor;
            builder.proxyNetworkName = proxyNetworkName;

            return builder;
        }

        void validate() {
            if (!enableValidation) {
                return;
            }

            if (clusters.isEmpty()) {
                throw new IllegalArgumentException("No YT cluster specified");
            }

            // Check cluster uniqueness.
            Set<String> clusterNames = new HashSet<>();
            boolean foundPreferredCluster = false;
            for (YTsaurusCluster cluster : clusters) {
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

        TBuilder disableValidation() {
            enableValidation = false;
            return self();
        }
    }

    // Class is able to initialize missing fields of builder with reasonable defaults. Keep in mind that:
    //   1. We cannot initialize this fields in YTsaurusClient constructor
    //   because busConnector is required to initialize superclass.
    //   2. We don't want to call initialization code if user specified explicitly values
    //   (initialization code is looking at environment and can fail, and it starts threads)
    //   3. Its better not to touch and modify builder instance since it's theoretically can be used by
    //   user to initialize another YTsaurusClient.
    @NonNullFields
    public static class BuilderWithDefaults<
            TClient,
            TBuilder extends ClientBuilder<TClient, TBuilder>> {
        final ClientBuilder<TClient, TBuilder> builder;
        final BusConnector busConnector;
        final YTsaurusClientAuth auth;

        public BuilderWithDefaults(ClientBuilder<TClient, TBuilder> builder) {
            this.builder = builder;

            busConnector = Objects.requireNonNullElseGet(builder.busConnector, DefaultBusConnector::new);
            auth = Objects.requireNonNullElseGet(builder.auth,
                    YTsaurusClientAuth::loadUserAndTokenFromEnvironment);
        }
    }
}
