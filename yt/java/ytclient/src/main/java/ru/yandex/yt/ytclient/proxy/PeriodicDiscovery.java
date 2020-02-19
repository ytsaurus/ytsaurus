package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.inside.yt.kosher.common.YtFormat;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.io.IoUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactoryImpl;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class PeriodicDiscovery implements AutoCloseable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(PeriodicDiscovery.class);

    private final BusConnector connector;
    private final String datacenterName;
    private final Set<HostPort> proxies;
    private final Duration updatePeriod;
    private final RpcOptions options;
    private final Random rnd;
    private final List<HostPort> initialAddresses;
    private final Option<String> proxyRole;
    private final Option<String> clusterUrl;
    private final String discoverProxiesUrl;
    private final RpcCredentials credentials;
    private final RpcCompression compression;
    private final Option<PeriodicDiscoveryListener> listenerOpt;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AsyncHttpClient httpClient;

    public PeriodicDiscovery(
            String datacenterName,
            List<String> initialAddresses,
            String proxyRole,
            String clusterUrl,
            BusConnector connector,
            RpcOptions options,
            RpcCredentials credentials,
            RpcCompression compression,
            PeriodicDiscoveryListener listener) {
        this.connector = Objects.requireNonNull(connector);
        this.datacenterName = Objects.requireNonNull(datacenterName);
        this.updatePeriod = options.getProxyUpdateTimeout();
        this.options = Objects.requireNonNull(options);
        this.rnd = new Random();
        this.proxyRole = Option.ofNullable(proxyRole);
        this.clusterUrl = Option.ofNullable(clusterUrl);
        this.proxies = new HashSet<>();
        this.credentials = Objects.requireNonNull(credentials);
        this.compression = Objects.requireNonNull(compression);
        this.listenerOpt = Option.ofNullable(listener);
        this.httpClient = asyncHttpClient(
                new DefaultAsyncHttpClientConfig.Builder()
                    .setThreadPoolName("YtClient-PeriodicDiscovery")
                    .setEventLoopGroup(connector.eventLoopGroup())
                    .setHttpClientCodecMaxHeaderSize(65536)
                    .build()
        );

        if (clusterUrl != null) {
            this.discoverProxiesUrl = this.proxyRole.map(x ->
                    String.format("http://%s/api/v4/discover_proxies?type=rpc&role=%s", clusterUrl, x)
            ).getOrElse(() -> String.format("http://%s/api/v4/discover_proxies?type=rpc", clusterUrl));
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
        logger.info("[{}] New proxy list added", datacenterName);
        proxies.clear();
        proxies.addAll(list);
        for (PeriodicDiscoveryListener listener : listenerOpt) {
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

    private void updateProxies() {
        if (!running.get()) {
            return; // ---
        }
        if (proxies.isEmpty() && clusterUrl.isPresent()) {
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
                if (!running.get()) {
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
                List<HostPort> proxies = node
                        .asMap()
                        .getOrThrow("proxies")
                        .asList()
                        .map(YTreeNode::stringValue)
                        .map(HostPort::parse);

                processProxies(new HashSet<>(proxies));
            } catch (Throwable e) {
                for (PeriodicDiscoveryListener listener : listenerOpt) {
                    listener.onError(e);
                }

                logger.error("[{}] Error on process proxies", datacenterName, e);
            } finally {
                if (running.get()) {
                    scheduleUpdate();
                }
            }
        }, connector.eventLoopGroup());
    }

    private void updateProxiesFromRpc() {
        List<HostPort> clients = new ArrayList<>(proxies);
        HostPort clientAddr = clients.get(rnd.nextInt(clients.size()));
        DiscoveryServiceClient client = createDiscoveryServiceClient(clientAddr);
        client.discoverProxies(proxyRole.getOrNull()).whenComplete((result, error) -> {
            if (!running.get()) {
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

            if (running.get()) {
                scheduleUpdate();
            }
        });
    }

    private void processProxies(Set<HostPort> list) {
        setProxies(list);

        if (proxies.isEmpty()) {
            if (clusterUrl.isPresent()) {
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
        running.set(false);
        IoUtils.closeQuietly(httpClient);
    }
}
