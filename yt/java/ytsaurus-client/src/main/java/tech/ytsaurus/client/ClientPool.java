package tech.ytsaurus.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.misc.SerializedExecutorService;
import tech.ytsaurus.client.rpc.DataCenterMetricsHolder;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientPool;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtFormat;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;


@NonNullApi
interface FilteringRpcClientPool extends RpcClientPool {
    /**
     * Peek client with filter.
     *
     * @param filter pool will try to return future that satisfies given filter;
     *               if none of the clients satisfies filter some client will be returned nevertheless.
     * @see #peekClient(CompletableFuture)
     */
    CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture, Predicate<RpcClient> filter);

    @Override
    default CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture) {
        return peekClient(releaseFuture, x -> true);
    }
}

@NonNullApi
interface DataCenterRpcClientPool extends FilteringRpcClientPool {
    String getDataCenterName();

    CompletableFuture<Integer> banClient(String address);
}

/**
 * Client pool that tracks returned clients and tries not to return them again.
 */
@NonNullApi
class NonRepeatingClientPool implements RpcClientPool {
    private final FilteringRpcClientPool underlying;
    private final ConcurrentSkipListSet<String> usedClients = new ConcurrentSkipListSet<>();

    NonRepeatingClientPool(FilteringRpcClientPool underlying) {
        this.underlying = underlying;
    }

    @Override
    public CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture) {
        return underlying.peekClient(releaseFuture, c -> !usedClients.contains(c.getAddressString()))
                .whenComplete((c, e) -> {
                    if (c != null) {
                        usedClients.add(c.getAddressString());
                    }
                });
    }
}

/**
 * This client pool tracks several data center pools.
 * If everything is ok it peeks clients from the local data center (or from data center with the lowest ping).
 * <p>
 * When this data center goes down, pool switches to others.
 */
@NonNullFields
@NonNullApi
class MultiDcClientPool implements FilteringRpcClientPool {
    static final Logger logger = LoggerFactory.getLogger(MultiDcClientPool.class);

    final DataCenterRpcClientPool[] clientPools;
    @Nullable
    final DataCenterRpcClientPool localDcPool;
    final DataCenterMetricsHolder dcMetricHolder;

    private MultiDcClientPool(Builder builder) {
        clientPools = builder.clientPools.toArray(new DataCenterRpcClientPool[0]);
        if (builder.localDc != null) {
            localDcPool = builder.clientPools.stream()
                    .filter((pool) -> builder.localDc.equals(YTsaurusCluster.normalizeName(pool.getDataCenterName())))
                    .findFirst().orElse(null);
            if (localDcPool == null) {
                // N.B. actually we should throw exception
                // but by historical reasons we have to work in such conditions
                // At least we can complain.
                logger.error("Cannot find local datacenter: {} among: {}",
                        builder.localDc,
                        builder.clientPools.stream()
                                .map(DataCenterRpcClientPool::getDataCenterName)
                                .collect(Collectors.toList()));
            }
        } else {
            localDcPool = null;
        }
        dcMetricHolder = Objects.requireNonNull(builder.dcMetricHolder);
    }

    static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture, Predicate<RpcClient> filter) {
        // If local dc has immediate candidates return them.
        if (localDcPool != null) {
            CompletableFuture<RpcClient> localClientFuture = localDcPool.peekClient(releaseFuture, filter);
            RpcClient localClient = getImmediateResult(localClientFuture);
            if (localClient != null) {
                return localClientFuture;
            } else {
                localClientFuture.cancel(true);
            }
        }

        // Try to find the best option among all immediate results.
        ArrayList<CompletableFuture<RpcClient>> futures = new ArrayList<>(clientPools.length);
        RpcClient resultClient = null;
        double resultPing = Double.MAX_VALUE;
        for (DataCenterRpcClientPool dc : clientPools) {
            CompletableFuture<RpcClient> f = dc.peekClient(releaseFuture, filter);
            RpcClient client = getImmediateResult(f);
            if (client != null) {
                double currentPing = dcMetricHolder.getDc99thPercentile(dc.getDataCenterName());
                if (currentPing < resultPing) {
                    resultClient = client;
                    resultPing = currentPing;
                }
            } else {
                futures.add(f);
            }
        }

        if (resultClient != null) {
            for (CompletableFuture<RpcClient> future : futures) {
                future.cancel(true);
            }
            return CompletableFuture.completedFuture(resultClient);
        }

        // If all DCs are waiting for a client, then we have to wait first available proxy.
        CompletableFuture<RpcClient> resultFuture = new CompletableFuture<>();
        AtomicInteger errorCount = new AtomicInteger(0);
        final int errorCountLimit = futures.size();
        for (CompletableFuture<RpcClient> future : futures) {
            future.whenComplete((client, error) -> {
                if (error == null) {
                    resultFuture.complete(client);
                } else if (errorCount.incrementAndGet() == errorCountLimit) {
                    resultFuture.completeExceptionally(error);
                }
            });
            resultFuture.whenComplete((client, error) -> future.cancel(true));
        }
        return resultFuture;
    }

    CompletableFuture<Integer> banClient(String address) {
        AtomicInteger total = new AtomicInteger(0);
        List<CompletableFuture<Integer>> bannedCountList = new ArrayList<>(clientPools.length);

        for (DataCenterRpcClientPool pool : clientPools) {
            bannedCountList.add(pool.banClient(address));
        }

        CompletableFuture<Void> accumulator = CompletableFuture.completedFuture(null);
        for (CompletableFuture<Integer> cur : bannedCountList) {
            accumulator = CompletableFuture.allOf(accumulator, cur.thenApply(total::addAndGet));
        }

        return accumulator.thenApply((ignored) -> total.get());
    }

    @Nullable
    private static RpcClient getImmediateResult(CompletableFuture<RpcClient> future) {
        try {
            return future.getNow(null);
        } catch (Throwable error) {
            return null;
        }
    }

    @NonNullApi
    @NonNullFields
    static class Builder {
        @Nullable
        String localDc;
        List<DataCenterRpcClientPool> clientPools = new ArrayList<>();
        @Nullable
        DataCenterMetricsHolder dcMetricHolder = null;

        Builder setLocalDc(@Nullable String localDcName) {
            localDc = localDcName;
            return this;
        }

        Builder addClientPool(DataCenterRpcClientPool clientPool) {
            clientPools.add(clientPool);
            return this;
        }

        <T extends DataCenterRpcClientPool> Builder addClientPools(Collection<T> pools) {
            clientPools.addAll(pools);
            return this;
        }

        Builder setDcMetricHolder(DataCenterMetricsHolder dcMetricHolder) {
            this.dcMetricHolder = dcMetricHolder;
            return this;
        }

        MultiDcClientPool build() {
            return new MultiDcClientPool(this);
        }
    }
}

/**
 * This pool automatically discovers rpc proxy of a given YT cluster.
 * It can use http balancer or small number of known rpc proxies for bootstrap.
 */
@NonNullApi
@NonNullFields
class ClientPoolService extends ClientPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClientPoolService.class);

    final ProxyGetter proxyGetter;
    final ScheduledExecutorService executorService;
    final long updatePeriodMs;
    final List<AutoCloseable> toClose = new ArrayList<>();

    State state = State.NOT_STARTED;
    Future<?> nextUpdate = new CompletableFuture<>();

    private ClientPoolService(HttpBuilder httpBuilder) {
        super(
                Objects.requireNonNull(httpBuilder.dataCenterName),
                Objects.requireNonNull(httpBuilder.options).getChannelPoolSize(),
                new SelfCheckingClientFactoryImpl(
                        Objects.requireNonNull(httpBuilder.clientFactory),
                        httpBuilder.options
                ),
                Objects.requireNonNull(httpBuilder.eventLoop),
                Objects.requireNonNull(httpBuilder.random),
                Objects.requireNonNull(httpBuilder.options.getRpcProxySelector())
        );

        HttpClient httpClient = HttpClient.newBuilder()
                .executor(httpBuilder.eventLoop)
                .build();

        proxyGetter = new HttpProxyGetter(
                httpClient,
                httpBuilder
        );

        executorService = httpBuilder.eventLoop;
        updatePeriodMs = httpBuilder.options.getProxyUpdateTimeout().toMillis();
    }

    private ClientPoolService(RpcBuilder rpcBuilder) {
        super(
                Objects.requireNonNull(rpcBuilder.dataCenterName),
                Objects.requireNonNull(rpcBuilder.options).getChannelPoolSize(),
                new SelfCheckingClientFactoryImpl(
                        Objects.requireNonNull(rpcBuilder.clientFactory),
                        rpcBuilder.options),
                Objects.requireNonNull(rpcBuilder.eventLoop),
                Objects.requireNonNull(rpcBuilder.random),
                Objects.requireNonNull(rpcBuilder.options.getRpcProxySelector())
        );

        proxyGetter = new RpcProxyGetter(
                Objects.requireNonNull(rpcBuilder.initialProxyList),
                this,
                rpcBuilder.role,
                rpcBuilder.dataCenterName,
                rpcBuilder.clientFactory,
                rpcBuilder.options,
                rpcBuilder.random
        );

        executorService = rpcBuilder.eventLoop;
        updatePeriodMs = rpcBuilder.options.getProxyUpdateTimeout().toMillis();

        updateClients(rpcBuilder.initialProxyList);
    }

    static HttpBuilder httpBuilder() {
        return new HttpBuilder();
    }

    static RpcBuilder rpcBuilder() {
        return new RpcBuilder();
    }

    void start() {
        synchronized (this) {
            if (state == State.NOT_STARTED) {
                state = State.RUNNING;
                setOnAllBannedCallback(() -> doUpdate(false));
                nextUpdate = executorService.submit(() -> doUpdate(true));
            } else {
                throw new IllegalArgumentException("ClientPoolService is in invalid state: " + state);
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            state = State.STOPPED;
            nextUpdate.cancel(true);
        }

        Throwable error = null;
        for (AutoCloseable closable : toClose) {
            try {
                closable.close();
            } catch (Throwable t) {
                logger.error("Error while closing client pool service", t);
                error = t;
            }
        }
        if (error != null) {
            throw new RuntimeException(error);
        }
    }

    private void doUpdate(boolean scheduleNextUpdate) {
        CompletableFuture<List<HostPort>> proxiesFuture = proxyGetter.getProxies();
        proxiesFuture.whenCompleteAsync((result, error) -> {
            if (error == null) {
                logger.debug(
                        "Successfully discovered {} rpc proxies DataCenter: {}",
                        result.size(),
                        getDataCenterName()
                );
                updateClients(result);
            } else {
                logger.warn(
                        "Failed to discover rpc proxies DataCenter: {} Error: ",
                        getDataCenterName(),
                        error
                );
                updateWithError(error);
            }

            if (scheduleNextUpdate) {
                synchronized (this) {
                    if (state == State.RUNNING) {
                        nextUpdate = executorService.schedule(
                                () -> doUpdate(true),
                                updatePeriodMs,
                                TimeUnit.MILLISECONDS);
                    } else if (state != State.STOPPED) {
                        throw new IllegalArgumentException("ClientPoolService is in unexpected state: " + state);
                    }
                }
            }
        }, executorService);
    }

    abstract static class BaseBuilder<T extends BaseBuilder<T>> {
        @Nullable
        String role;
        boolean useTLS = false;
        boolean tvmOnly = false;
        boolean ignoreBalancers = false;
        @Nullable
        String token;
        @Nullable
        String dataCenterName;
        @Nullable
        RpcOptions options;
        @Nullable
        RpcClientFactory clientFactory;
        @Nullable
        EventLoopGroup eventLoop;
        @Nullable
        Random random;
        @Nullable
        String proxyNetworkName;

        T setDataCenterName(String dataCenterName) {
            this.dataCenterName = dataCenterName;
            //noinspection unchecked
            return (T) this;
        }

        T setOptions(RpcOptions options) {
            this.options = options;
            //noinspection unchecked
            return (T) this;
        }

        T setClientFactory(RpcClientFactory clientFactory) {
            this.clientFactory = clientFactory;
            //noinspection unchecked
            return (T) this;
        }

        T setEventLoop(EventLoopGroup eventLoop) {
            this.eventLoop = eventLoop;
            //noinspection unchecked
            return (T) this;
        }

        T setRandom(Random random) {
            this.random = random;
            //noinspection unchecked
            return (T) this;
        }

        T setRole(@Nullable String role) {
            this.role = role;
            //noinspection unchecked
            return (T) this;
        }

        T setUseTLS(boolean useTLS) {
            this.useTLS = useTLS;
            //noinspection unchecked
            return (T) this;
        }

        T setTvmOnly(boolean tvmOnly) {
            this.tvmOnly = tvmOnly;
            //noinspection unchecked
            return (T) this;
        }

        T setIgnoreBalancers(boolean ignoreBalancers) {
            this.ignoreBalancers = ignoreBalancers;
            //noinspection unchecked
            return (T) this;
        }

        T setToken(@Nullable String token) {
            this.token = token;
            //noinspection unchecked
            return (T) this;
        }

        T setProxyNetworkName(@Nullable String proxyNetworkName) {
            this.proxyNetworkName = proxyNetworkName;
            //noinspection unchecked
            return (T) this;
        }
    }

    /**
     * All setters with Nullable parameter are optional.
     * Other setters are required.
     */
    static class HttpBuilder extends BaseBuilder<HttpBuilder> {
        private static final String IP_V6_REG_EX = "[0-9a-fA-F]{0,4}(:[0-9a-fA-F]{0,4}){2,7}";
        @Nullable
        String balancerFqdn;
        @Nullable
        Integer balancerPort;

        HttpBuilder setBalancerFqdn(String fqdn) {
            if (fqdn.matches(IP_V6_REG_EX)) {
                this.balancerFqdn = String.format("[%s]", fqdn);
            } else if (fqdn.matches("\\[" + IP_V6_REG_EX + "]") || !fqdn.contains(":")) {
                this.balancerFqdn = fqdn;
            } else {
                throw new IllegalArgumentException("Bad FQDN: " + fqdn);
            }
            return this;
        }

        HttpBuilder setBalancerPort(@Nullable Integer port) {
            if (port != null && (port < 0 || port > 65535)) {
                throw new IllegalArgumentException("Bad port: " + port);
            }
            this.balancerPort = port;
            return this;
        }

        ClientPoolService build() {
            return new ClientPoolService(this);
        }
    }

    static class RpcBuilder extends BaseBuilder<RpcBuilder> {
        @Nullable
        List<HostPort> initialProxyList;

        RpcBuilder setInitialProxyList(List<HostPort> initialProxyList) {
            this.initialProxyList = initialProxyList;
            return this;
        }

        ClientPoolService build() {
            return new ClientPoolService(this);
        }
    }

    private enum State {
        NOT_STARTED,
        RUNNING,
        STOPPED
    }
}

/**
 * Client pool tracks a list of RpcProxies can ban them or add new proxies.
 * It doesn't have a process that updates them automatically.
 */
@NonNullApi
@NonNullFields
class ClientPool implements DataCenterRpcClientPool {
    private static final Logger logger = LoggerFactory.getLogger(ClientPool.class);

    private final String dataCenterName;
    private final int maxSize;
    private final SelfCheckingClientFactory clientFactory;
    private final ExecutorService unsafeExecutorService;
    private final SerializedExecutorService safeExecutorService;
    private final Random random;
    private final ProxySelector proxySelector;

    // Healthy clients.
    private final Map<GUID, PooledRpcClient> activeClients = new HashMap<>();

    private CompletableFuture<Void> nextUpdate = new CompletableFuture<>();

    // Array of healthy clients that are used for optimization of peekClient.
    private volatile PooledRpcClient[] clientCache = new PooledRpcClient[0];
    @Nullable
    private volatile Runnable onAllBannedCallback = null;

    ClientPool(
            String dataCenterName,
            int maxSize,
            SelfCheckingClientFactory clientFactory,
            ExecutorService executorService,
            Random random
    ) {
        this(dataCenterName, maxSize, clientFactory, executorService, random, ProxySelector.random());
    }

    ClientPool(
            String dataCenterName,
            int maxSize,
            SelfCheckingClientFactory clientFactory,
            ExecutorService executorService,
            Random random,
            ProxySelector proxySelector
    ) {
        this.dataCenterName = dataCenterName;
        this.unsafeExecutorService = executorService;
        this.safeExecutorService = new SerializedExecutorService(executorService);
        this.random = random;
        this.maxSize = maxSize;
        this.clientFactory = clientFactory;
        this.proxySelector = proxySelector;
    }

    @Override
    public CompletableFuture<RpcClient> peekClient(CompletableFuture<?> release, Predicate<RpcClient> filter) {
        PooledRpcClient[] goodClientsRef = clientCache;
        CompletableFuture<RpcClient> result = new CompletableFuture<>();
        if (!peekClientImpl(goodClientsRef, result, release, filter)) {
            safeExecutorService.submit(() -> peekClientUnsafe(result, release, filter));
        }
        return result;
    }

    CompletableFuture<Void> updateWithError(Throwable error) {
        return safeExecutorService.submit(() -> updateWithErrorUnsafe(error));
    }

    CompletableFuture<Void> updateClients(Collection<HostPort> proxies) {
        return safeExecutorService.submit(() -> updateClientsUnsafe(new HashSet<>(proxies)));
    }

    public String getDataCenterName() {
        return dataCenterName;
    }

    void setOnAllBannedCallback(Runnable onAllBannedCallback) {
        this.onAllBannedCallback = onAllBannedCallback;
    }

    RpcClient[] getAliveClients() {
        PooledRpcClient[] tmp = this.clientCache;
        RpcClient[] result = new RpcClient[tmp.length];
        for (int i = 0; i < tmp.length; ++i) {
            result[i] = tmp[i].publicClient;
        }
        return result;
    }

    private void peekClientUnsafe(
            CompletableFuture<RpcClient> result,
            CompletableFuture<?> release,
            Predicate<RpcClient> filter
    ) {
        if (peekClientImpl(clientCache, result, release, filter)) {
            return;
        }
        nextUpdate.whenComplete((Void v, Throwable t) -> {
            if (peekClientImpl(clientCache, result, release, filter)) {
                return;
            }
            RuntimeException error = new RuntimeException("Cannot get rpc proxies; DataCenter: " + dataCenterName);
            if (t != null) {
                error.initCause(t);
            }
            result.completeExceptionally(error);
        });
    }

    private boolean peekClientImpl(
            PooledRpcClient[] clients,
            CompletableFuture<RpcClient> result,
            CompletableFuture<?> release,
            Predicate<RpcClient> filter
    ) {
        if (clients.length > 0) {
            int offset = random.nextInt(clients.length);
            PooledRpcClient pooledClient = null;

            // First we try to find client that satisfies filter.
            for (int i = 0; i < clients.length; ++i) {
                PooledRpcClient curClient = clients[(i + offset) % clients.length];
                if (!curClient.banned
                        && filter.test(curClient.publicClient)
                        && curClient.ref()
                ) {
                    pooledClient = curClient;
                    break;
                }
            }
            // If we didn't succeed we try to peek any good client.
            if (pooledClient == null) {
                for (int i = 0; i < clients.length; ++i) {
                    PooledRpcClient curClient = clients[(i + offset) % clients.length];
                    if (!curClient.banned && curClient.ref()) {
                        pooledClient = curClient;
                        break;
                    }
                }
            }
            if (pooledClient != null) {
                if (result.complete(pooledClient.publicClient)) {
                    PooledRpcClient pooledClientClosure = pooledClient;
                    release.whenComplete((o, throwable) -> pooledClientClosure.unref());
                } else {
                    pooledClient.unref();
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public CompletableFuture<Integer> banClient(String address) {
        return banErrorClient(HostPort.parse(address));
    }

    CompletableFuture<Integer> banErrorClient(HostPort hostPort) {
        return safeExecutorService.submit(
                () -> {
                    List<PooledRpcClient> toBan = new ArrayList<>();
                    for (PooledRpcClient client : activeClients.values()) {
                        if (client.hostPort.equals(hostPort)) {
                            toBan.add(client);
                        }
                    }
                    for (PooledRpcClient client : toBan) {
                        banClientUnsafe(client, true);
                    }
                    return toBan.size();
                }
        );
    }

    private void banErrorClient(PooledRpcClient client) {
        safeExecutorService.submit(() -> banClientUnsafe(client, true));
    }

    private void updateClientsUnsafe(Set<HostPort> proxies) {
        ArrayList<PooledRpcClient> toBan = new ArrayList<>();
        for (PooledRpcClient client : activeClients.values()) {
            if (proxies.contains(client.hostPort)) {
                proxies.remove(client.hostPort);
            } else {
                toBan.add(client);
            }
        }
        for (PooledRpcClient client : toBan) {
            logger.debug("Banning unknown rpc-proxy connection {}", client);
            banClientUnsafe(client, false);
        }

        ArrayList<HostPort> remainingProxies = new ArrayList<>(proxies);
        proxySelector.rank(remainingProxies);

        for (HostPort hostPort : remainingProxies) {
            if (activeClients.size() >= maxSize) {
                break;
            }

            CompletableFuture<Void> clientStatusFuture = new CompletableFuture<>();
            RpcClient rpcClient = clientFactory.create(hostPort, dataCenterName, clientStatusFuture);
            GUID clientGuid = GUID.create();
            PooledRpcClient pooledClient = new PooledRpcClient(hostPort, rpcClient, clientGuid, clientStatusFuture);
            clientStatusFuture.whenComplete((result, error) -> {
                if (error != null) {
                    logger.debug("Banning {} because of error: ", pooledClient, error);
                    banErrorClient(pooledClient);
                }
            });
            logger.debug("Opened new rpc-proxy connection: {}", pooledClient);
            activeClients.put(clientGuid, pooledClient);
        }
        updateGoodClientsCacheUnsafe();
        CompletableFuture<Void> oldNextUpdate = nextUpdate;
        nextUpdate = new CompletableFuture<>();

        oldNextUpdate.complete(null);
    }

    private void updateWithErrorUnsafe(Throwable error) {
        CompletableFuture<Void> oldNextUpdate = nextUpdate;
        nextUpdate = new CompletableFuture<>();

        oldNextUpdate.completeExceptionally(error);
    }

    private void updateGoodClientsCacheUnsafe() {
        PooledRpcClient[] newCache = new PooledRpcClient[activeClients.size()];
        clientCache = activeClients.values().toArray(newCache);
        logger.debug("Updated client cache; {} clients available", clientCache.length);
    }

    private void banClientUnsafe(PooledRpcClient client, boolean updateClientCache) {
        PooledRpcClient pooledClient = activeClients.get(client.guid);
        if (pooledClient == null || pooledClient.banned) {
            return;
        }
        pooledClient.banned = true;
        pooledClient.unref();
        activeClients.remove(client.guid);

        if (updateClientCache) {
            updateGoodClientsCacheUnsafe();
            Runnable cb = onAllBannedCallback;
            if (activeClients.isEmpty() && cb != null) {
                unsafeExecutorService.execute(cb);
            }
        }
    }

    @NonNullFields
    @NonNullApi
    static class PooledRpcClient {
        final HostPort hostPort;
        final RpcClient publicClient;
        final GUID guid;
        final CompletableFuture<Void> statusFuture;

        volatile boolean banned = false;

        private final AtomicInteger referenceCounter = new AtomicInteger(1);

        PooledRpcClient(HostPort hostPort, RpcClient client, GUID guid, CompletableFuture<Void> clientStatusFuture) {
            this.hostPort = hostPort;
            this.publicClient = client;
            this.guid = guid;
            this.statusFuture = clientStatusFuture;
        }

        boolean ref() {
            int old = referenceCounter.getAndUpdate(x -> x == 0 ? 0 : ++x);
            return old > 0;
        }

        void unref() {
            int ref = referenceCounter.decrementAndGet();
            if (ref == 0) {
                logger.debug("Releasing rpc-proxy connection {}", this);
                publicClient.unref();
                statusFuture.complete(null);
            }
        }

        @Override
        public String toString() {
            return String.format("[%s/%s]", guid, hostPort);
        }
    }
}

interface ProxyGetter {
    CompletableFuture<List<HostPort>> getProxies();
}

@NonNullApi
@NonNullFields
class HttpProxyGetter implements ProxyGetter {
    private static final int HTTP_PROXY_PORT = 80;
    private static final int HTTPS_PROXY_PORT = 443;

    private static final int TVM_ONLY_HTTP_PROXY_PORT = 9026;
    private static final int TVM_ONLY_HTTPS_PROXY_PORT = 9443;

    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    private final HttpClient httpClient;
    private final String balancerFqdn;
    @Nullable
    private final Integer balancerPort;
    @Nullable
    private final String role;
    private final boolean useTLS;
    private final boolean tvmOnly;
    private final boolean ignoreBalancers;
    @Nullable
    private final String token;
    @Nullable
    String proxyNetworkName;

    HttpProxyGetter(HttpClient httpClient, ClientPoolService.HttpBuilder httpBuilder) {
        this.httpClient = httpClient;
        this.balancerFqdn = Objects.requireNonNull(httpBuilder.balancerFqdn);
        this.balancerPort = httpBuilder.balancerPort;
        this.role = httpBuilder.role;
        this.useTLS = httpBuilder.useTLS;
        this.tvmOnly = httpBuilder.tvmOnly;
        this.ignoreBalancers = httpBuilder.ignoreBalancers;
        this.token = httpBuilder.token;
        this.proxyNetworkName = httpBuilder.proxyNetworkName;
    }

    @Override
    public CompletableFuture<List<HostPort>> getProxies() {
        var discoverProxiesUrl = String.format(
                "%s://%s/api/v4/discover_proxies?type=rpc", createScheme(), createFqdnWithPort()
        );
        if (role != null) {
            discoverProxiesUrl += "&role=" + role;
        }
        if (proxyNetworkName != null) {
            discoverProxiesUrl += "&network_name=" + proxyNetworkName;
        }
        if (tvmOnly) {
            discoverProxiesUrl += "&address_type=tvm_only_internal_rpc";
        }
        if (ignoreBalancers) {
            discoverProxiesUrl += "&ignore_balancers=true";
        }
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(discoverProxiesUrl))
                .setHeader("X-YT-Header-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                .setHeader("X-YT-Output-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT));
        if (token != null) {
            requestBuilder.setHeader("Authorization", String.format("OAuth %s", token));
        }
        CompletableFuture<HttpResponse<InputStream>> responseFuture =
                httpClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());

        CompletableFuture<List<HostPort>> resultFuture = responseFuture.thenApply((response) -> {
            // TODO: this should use common library of raw requests.
            if (response.statusCode() != 200) {
                StringBuilder builder = new StringBuilder();
                builder.append("Error: ");
                builder.append(response.statusCode());
                builder.append("\n");

                for (Map.Entry<String, List<String>> entry : response.headers().map().entrySet()) {
                    builder.append(entry.getKey());
                    builder.append("=");
                    builder.append(entry.getValue());
                    builder.append("\n");
                }

                try (var responseBody = response.body()) {
                    builder.append(new String(responseBody.readAllBytes()));
                    builder.append("\n");
                } catch (IOException ignored) {
                }

                throw new RuntimeException(builder.toString());
            }

            YTreeNode node = YTreeTextSerializer.deserialize(response.body());
            return node
                    .mapNode()
                    .getOrThrow("proxies")
                    .asList()
                    .stream()
                    .map(YTreeNode::stringValue)
                    .map(HostPort::parse)
                    .collect(Collectors.toList());
        });

        String finalDiscoverProxiesUrl = discoverProxiesUrl;
        return resultFuture.handle((result, error) -> {
            if (error != null) {
                throw new RuntimeException("Failed to get proxies from " + finalDiscoverProxiesUrl, error);
            }
            return result;
        });
    }

    private String createScheme() {
        if (useTLS) {
            return HTTPS_SCHEME;
        }
        return HTTP_SCHEME;
    }

    private String createFqdnWithPort() {
        if (balancerPort != null) {
            return balancerFqdn + ":" + balancerPort;
        }
        int port;
        if (tvmOnly) {
            port = useTLS
                    ? TVM_ONLY_HTTPS_PROXY_PORT
                    : TVM_ONLY_HTTP_PROXY_PORT;
        } else {
            port = useTLS
                    ? HTTPS_PROXY_PORT
                    : HTTP_PROXY_PORT;
        }
        return String.format("%s:%s", balancerFqdn, port);
    }
}

@NonNullFields
@NonNullApi
class RpcProxyGetter implements ProxyGetter {
    final List<HostPort> initialProxyList;
    final @Nullable
    RpcClientPool clientPool;
    final @Nullable
    String role;
    final String dataCenterName;
    final RpcClientFactory clientFactory;
    final RpcOptions options;
    final Random random;

    RpcProxyGetter(
            List<HostPort> initialProxyList,
            @Nullable RpcClientPool clientPool,
            @Nullable String role,
            String dataCenterName,
            RpcClientFactory clientFactory,
            RpcOptions options,
            Random random
    ) {
        this.initialProxyList = Collections.unmodifiableList(initialProxyList);
        this.clientPool = clientPool;
        this.role = role;
        this.dataCenterName = dataCenterName;
        this.clientFactory = clientFactory;
        this.options = options;
        this.random = random;
    }

    @Override
    public CompletableFuture<List<HostPort>> getProxies() {
        CompletableFuture<Void> releaseClientFuture = new CompletableFuture<>();
        RpcClient rpcClient = null;
        if (clientPool != null) {
            CompletableFuture<RpcClient> clientFuture = clientPool.peekClient(releaseClientFuture);
            if (clientFuture.isDone() && !clientFuture.isCompletedExceptionally()) {
                rpcClient = clientFuture.join();
            }
        }
        if (rpcClient == null) {
            HostPort address = initialProxyList.get(random.nextInt(initialProxyList.size()));
            rpcClient = clientFactory.create(address, dataCenterName);
        }

        DiscoveryServiceClient client = new DiscoveryServiceClient(rpcClient, options);

        CompletableFuture<List<String>> requestResult = client.discoverProxies(role);
        CompletableFuture<List<HostPort>> resultFuture = requestResult
                .thenApply(result -> result.stream().map(HostPort::parse).collect(Collectors.toList()));

        resultFuture.whenComplete((result, error) -> releaseClientFuture.complete(null));
        return resultFuture;
    }
}
