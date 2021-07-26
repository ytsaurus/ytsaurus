package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.YtFormat;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.DiscoveryMethod;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.asynchttpclient.Dsl.asyncHttpClient;

@NonNullApi
@NonNullFields
public class PeriodicDiscovery implements AutoCloseable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(PeriodicDiscovery.class);

    private final BusConnector connector;
    private final String datacenterName;
    private final Set<HostPort> proxies;
    private final Duration updatePeriod;
    private final RpcOptions options;
    private final Random rnd;
    private final List<HostPort> initialAddresses;
    private final @Nullable String proxyRole;
    private final @Nullable String clusterUrl;
    private final @Nullable String discoverProxiesUrl;
    private final RpcCredentials credentials;
    private final RpcCompression compression;
    private final @Nullable PeriodicDiscoveryListener listener;
    private final AsyncHttpClient httpClient;

    private volatile boolean running = true;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public PeriodicDiscovery(
            String datacenterName,
            List<String> initialAddresses,
            @Nullable String proxyRole,
            @Nullable String clusterUrl,
            BusConnector connector,
            RpcOptions options,
            RpcCredentials credentials,
            RpcCompression compression,
            @Nullable PeriodicDiscoveryListener listener
    ) {
        this.connector = Objects.requireNonNull(connector);
        this.datacenterName = Objects.requireNonNull(datacenterName);
        this.updatePeriod = options.getProxyUpdateTimeout();
        this.options = Objects.requireNonNull(options);
        this.rnd = new Random();
        this.proxyRole = proxyRole;
        this.clusterUrl = clusterUrl;
        this.proxies = new HashSet<>();
        this.credentials = Objects.requireNonNull(credentials);
        this.compression = Objects.requireNonNull(compression);
        this.listener = listener;
        this.httpClient = asyncHttpClient(
                new DefaultAsyncHttpClientConfig.Builder()
                    .setThreadPoolName("YtClient-PeriodicDiscovery")
                    .setThreadFactory(options.getDiscoveryThreadFactory())
                    .setEventLoopGroup(connector.eventLoopGroup())
                    .setHttpClientCodecMaxHeaderSize(65536)
                    .build()
        );

        if (clusterUrl != null) {
            this.discoverProxiesUrl = this.proxyRole != null
                    ? String.format("http://%s/api/v4/discover_proxies?type=rpc&role=%s", clusterUrl, this.proxyRole)
                    : String.format("http://%s/api/v4/discover_proxies?type=rpc", clusterUrl);
        } else {
            this.discoverProxiesUrl = null;
        }

        try {
            this.initialAddresses = initialAddresses.stream().map(HostPort::parse).collect(Collectors.toList());
            setProxies(this.initialAddresses);
        } catch (Throwable e) {
            logger.error("[{}] Error on construction periodic discovery", datacenterName, e);
            IoUtils.closeQuietly(this);
            throw e;
        }
    }

    public void start() {
        updateProxies();
    }

    public Set<String> getAddresses() {
        return proxies.stream().map(HostPort::toString).collect(Collectors.toSet());
    }

    private void setProxies(Collection<HostPort> list) {
        logger.debug("[{}] New proxy list added", datacenterName);
        proxies.clear();
        proxies.addAll(list);
        if (listener != null) {
            try {
                listener.onProxiesSet(proxies);
            } catch (Throwable e) {
                listener.onError(e);
                logger.error("[{}] Error on proxy set {}", datacenterName, list, e);
            }
        }
    }

    private DiscoveryServiceClient createDiscoveryServiceClient(HostPort addr) {
        RpcClientFactory factory = new RpcClientFactoryImpl(connector, credentials, compression);
        RpcClient rpcClient = factory.create(addr, datacenterName);
        return new DiscoveryServiceClient(rpcClient, options);
    }

    DiscoveryMethod selectDiscoveryMethod() {
        boolean preferHttp = options.getPreferableDiscoveryMethod() == DiscoveryMethod.HTTP;
        boolean isRpcAvailable = !proxies.isEmpty();
        if ((preferHttp || !isRpcAvailable) && clusterUrl != null) {
            return DiscoveryMethod.HTTP;
        } else {
            return DiscoveryMethod.RPC;
        }
    }

    private void updateProxies() {
        if (!running) {
            return; // ---
        }
        DiscoveryMethod discoveryMethod = selectDiscoveryMethod();
        if (discoveryMethod == DiscoveryMethod.HTTP) {
            updateProxiesFromHttp();
        } else {
            updateProxiesFromRpc();
        }
    }

    private void updateProxiesFromHttp() {
        ListenableFuture<Response> responseFuture = httpClient.executeRequest(
                new RequestBuilder()
                    .setUrl(discoverProxiesUrl)
                    .setHeader("X-YT-Header-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .setHeader("X-YT-Output-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .setHeader("Authorization", String.format("OAuth %s", credentials.getToken()))
                    .build());

        responseFuture.addListener(() -> {
            try {
                if (!running) {
                    return; // ---
                }

                Response response = responseFuture.get();

                if (response.getStatusCode() != 200) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("Error: ");
                    builder.append(response.getStatusCode());
                    builder.append("\n");

                    for (Map.Entry<String, String> entry : response.getHeaders()) {
                        builder.append(entry.getKey());
                        builder.append("=");
                        builder.append(entry.getValue());
                        builder.append("\n");
                    }

                    builder.append(new String(response.getResponseBodyAsBytes()));
                    builder.append("\n");

                    throw new RuntimeException(builder.toString());
                }

                YTreeNode node = YTreeTextSerializer.deserialize(response.getResponseBodyAsStream());
                List<HostPort> curProxies = node
                        .mapNode()
                        .getOrThrow("proxies")
                        .asList().stream()
                        .map(YTreeNode::stringValue)
                        .map(HostPort::parse)
                        .collect(Collectors.toList());

                processProxies(new HashSet<>(curProxies));
            } catch (Throwable e) {
                if (listener != null) {
                    listener.onError(e);
                }

                logger.error("[{}] Error on process proxies", datacenterName, e);
            } finally {
                if (running) {
                    scheduleUpdate();
                }
            }
        }, connector.eventLoopGroup());
    }

    private void updateProxiesFromRpc() {
        List<HostPort> clients = new ArrayList<>(proxies);
        HostPort clientAddr = clients.get(rnd.nextInt(clients.size()));
        DiscoveryServiceClient client = createDiscoveryServiceClient(clientAddr);
        client.discoverProxies(proxyRole).whenComplete((result, error) -> {
            if (!running) {
                return; // ---
            }
            try {
                if (error != null) {
                    logger.error("[{}] Error on update proxies", datacenterName, error);
                } else {
                    processProxies(new HashSet<>(result.stream().map(HostPort::parse).collect(Collectors.toList())));
                }
            } catch (Throwable e) {
                logger.error("[{}] Error on process proxies", datacenterName, e);
            }

            if (running) {
                scheduleUpdate();
            }
        });
    }

    private void processProxies(Set<HostPort> list) {
        setProxies(list);

        if (proxies.isEmpty()) {
            if (clusterUrl != null) {
                logger.warn("[{}] Empty proxies list. Bootstrapping from the initial list: {}",
                        datacenterName, clusterUrl);
            } else {
                logger.warn("[{}] Empty proxies list. Bootstrapping from the initial list: {}",
                        datacenterName, initialAddresses);
                setProxies(initialAddresses);
            }
        }
    }

    private void scheduleUpdate() {
        connector.eventLoopGroup().schedule(this::updateProxies, updatePeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        logger.debug("[{}] Stopping periodic discovery", datacenterName);
        running = false;
        IoUtils.closeQuietly(httpClient);
    }
}

interface ProxyGetter {
    CompletableFuture<List<HostPort>> getProxies();
}

@NonNullApi
@NonNullFields
class HttpProxyGetter implements ProxyGetter {
    AsyncHttpClient httpClient;
    String balancerHost;
    @Nullable String role;
    @Nullable String token;

    HttpProxyGetter(AsyncHttpClient httpClient, String balancerHost, @Nullable String role, @Nullable String token) {
        this.httpClient = httpClient;
        this.balancerHost = balancerHost;
        this.role = role;
        this.token = token;
    }

    @Override
    public CompletableFuture<List<HostPort>> getProxies() {
        String discoverProxiesUrl = String.format("http://%s/api/v4/discover_proxies?type=rpc", balancerHost);
        if (role != null) {
            discoverProxiesUrl += "&role=" + role;
        }
        RequestBuilder requestBuilder = new RequestBuilder()
                        .setUrl(discoverProxiesUrl)
                        .setHeader("X-YT-Header-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                        .setHeader("X-YT-Output-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT));
        if (token != null) {
            // NB. token should be set https://st.yandex-team.ru/YTADMINREQ-25316#60c516ddf76e1021d1143001
            requestBuilder.setHeader("Authorization", String.format("OAuth %s", token));
        }
        CompletableFuture<Response> responseFuture =
                httpClient.executeRequest(requestBuilder.build()).toCompletableFuture();

        CompletableFuture<List<HostPort>> resultFuture = responseFuture.thenApply((response) -> {
            // TODO: this should use common library of raw requests.
            if (response.getStatusCode() != 200) {
                StringBuilder builder = new StringBuilder();
                builder.append("Error: ");
                builder.append(response.getStatusCode());
                builder.append("\n");

                for (Map.Entry<String, String> entry : response.getHeaders()) {
                    builder.append(entry.getKey());
                    builder.append("=");
                    builder.append(entry.getValue());
                    builder.append("\n");
                }

                builder.append(new String(response.getResponseBodyAsBytes()));
                builder.append("\n");

                throw new RuntimeException(builder.toString());
            }

            YTreeNode node = YTreeTextSerializer.deserialize(response.getResponseBodyAsStream());
            return node
                    .mapNode()
                    .getOrThrow("proxies")
                    .asList()
                    .stream()
                    .map(YTreeNode::stringValue)
                    .map(HostPort::parse)
                    .collect(Collectors.toList());
        });
        resultFuture.whenComplete((result, error) -> responseFuture.cancel(true));
        return resultFuture;
    }
}

@NonNullFields
@NonNullApi
class RpcProxyGetter implements ProxyGetter {
    final List<HostPort> initialProxyList;
    final @Nullable RpcClientPool clientPool;
    final @Nullable String role;
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
