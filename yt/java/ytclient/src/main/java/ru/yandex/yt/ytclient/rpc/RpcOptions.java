package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nullable;

import ru.yandex.yt.ytclient.proxy.internal.DiscoveryMethod;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolderImpl;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolderImpl;

/**
 * Опции для создания rpc клиентов
 */
public class RpcOptions {
    private boolean defaultRequestAck = true;
    private boolean useClientsCache = false; // for backward compatibility
    private int clientsCacheSize = 10000; // will be used only when useClientsCache is true
    private Duration clientCacheExpiration = Duration.ofMillis(1); // will be used only when useClientsCache is true

    /**
     * @see #setAcknowledgementTimeout
     */
    private @Nullable Duration acknowledgementTimeout = Duration.ofSeconds(15);

    private Duration globalTimeout = Duration.ofMillis(60000);  // fails request after this timeout

    // sends fallback request to other proxy after this timeout
    private Duration failoverTimeout = Duration.ofMillis(30000);
    private Duration proxyUpdateTimeout = Duration.ofMillis(60000);
    private Duration pingTimeout = Duration.ofMillis(5000); // marks proxy as dead/live after this timeout
    private Duration channelPoolRebalanceInterval = Duration.ofMinutes(10);
    private Duration rpcClientSelectionTimeout = Duration.ofSeconds(30);
    private int channelPoolSize = 3;

    private Duration minBackoffTime = Duration.ofSeconds(3);
    private Duration maxBackoffTime = Duration.ofSeconds(30);

    // steaming options
    private Duration readTimeout = Duration.ofMillis(60000);
    private Duration writeTimeout = Duration.ofMillis(60000);
    private int windowSize = 32 * 1024 * 1024;

    private RpcFailoverPolicy failoverPolicy = new DefaultRpcFailoverPolicy();
    private BalancingResponseHandlerMetricsHolder responseMetricsHolder =
            new BalancingResponseHandlerMetricsHolderImpl();
    private DataCenterMetricsHolder dataCenterMetricsHolder = DataCenterMetricsHolderImpl.INSTANCE;
    private BalancingDestinationMetricsHolder destinationMetricsHolder = new BalancingDestinationMetricsHolderImpl();

    private boolean traceEnabled = false;
    private boolean traceSampled = false;
    private boolean traceDebug = false;

    private DiscoveryMethod preferableDiscoveryMethod = DiscoveryMethod.RPC;

    private boolean newDiscoveryServiceEnabled = false;

    private ThreadFactory discoveryThreadFactory;

    public RpcOptions() {
        // nothing
    }

    public boolean getTrace() {
        return traceEnabled;
    }

    public RpcOptions setTrace(boolean traceEnabled) {
        this.traceEnabled = traceEnabled;
        return this;
    }

    public boolean getTraceSampled() {
        return traceSampled;
    }

    public RpcOptions setTraceSampled(boolean traceSampled) {
        this.traceSampled = traceSampled;
        return this;
    }

    public boolean getTraceDebug() {
        return traceDebug;
    }

    public RpcOptions setTraceDebug(boolean traceDebug) {
        this.traceDebug = traceDebug;
        return this;
    }

    @Deprecated
    public Duration getDefaultTimeout() {
        return getGlobalTimeout();
    }

    @Deprecated
    public RpcOptions setDefaultTimeout(Duration defaultTimeout) {
        return setGlobalTimeout(defaultTimeout);
    }

    public boolean getDefaultRequestAck() {
        return defaultRequestAck;
    }

    public RpcOptions setDefaultRequestAck(boolean defaultRequestAck) {
        this.defaultRequestAck = defaultRequestAck;
        return this;
    }

    public boolean getUseClientsCache() {
        return useClientsCache;
    }

    public RpcOptions setUseClientsCache(boolean useClientsCache) {
        this.useClientsCache = useClientsCache;
        return this;
    }

    public int getClientsCacheSize() {
        return clientsCacheSize;
    }

    public void setClientsCacheSize(int clientsCacheSize) {
        this.clientsCacheSize = clientsCacheSize;
    }

    public Duration getClientCacheExpiration() {
        return clientCacheExpiration;
    }

    public void setClientCacheExpiration(Duration clientCacheExpiration) {
        this.clientCacheExpiration = clientCacheExpiration;
    }

    public Duration getFailoverTimeout() {
        return failoverTimeout;
    }

    public RpcOptions setFailoverTimeout(Duration failoverTimeout) {
        this.failoverTimeout = failoverTimeout;
        return this;
    }

    public Duration getGlobalTimeout() {
        return globalTimeout;
    }

    public RpcOptions setGlobalTimeout(Duration globalTimeout) {
        this.globalTimeout = globalTimeout;
        return this;
    }

    public Duration getProxyUpdateTimeout() {
        return proxyUpdateTimeout;
    }

    public RpcOptions setProxyUpdateTimeout(Duration timeout) {
        this.proxyUpdateTimeout = timeout;
        return this;
    }

    public Duration getPingTimeout() {
        return pingTimeout;
    }

    public RpcOptions setPingTimeout(Duration timeout) {
        this.pingTimeout = timeout;
        return this;
    }

    public RpcOptions setStreamingWindowSize(int bytes) {
        this.windowSize = bytes;
        return this;
    }

    public int getStreamingWindowSize() {
        return this.windowSize;
    }

    public RpcOptions  setStreamingWriteTimeout(Duration timeout) {
        this.writeTimeout = timeout;
        return this;
    }

    public Duration getStreamingWriteTimeout() {
        return this.writeTimeout;
    }

    public RpcOptions setStreamingReadTimeout(Duration timeout) {
        this.readTimeout = timeout;
        return this;
    }

    public Duration getStreamingReadTimeout() {
        return this.readTimeout;
    }

    public RpcFailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    public RpcOptions setFailoverPolicy(RpcFailoverPolicy failoverPolicy) {
        this.failoverPolicy = failoverPolicy;
        return this;
    }

    public RpcOptions setResponseMetricsHolder(BalancingResponseHandlerMetricsHolder responseMetricsHolder) {
        this.responseMetricsHolder = responseMetricsHolder;
        return this;
    }

    public BalancingResponseHandlerMetricsHolder getResponseMetricsHolder() {
        return responseMetricsHolder;
    }

    public RpcOptions setDataCenterMetricsHolder(DataCenterMetricsHolder holder) {
        this.dataCenterMetricsHolder = holder;
        return this;
    }

    public DataCenterMetricsHolder getDataCenterMetricsHolder() {
        return dataCenterMetricsHolder;
    }

    public RpcOptions setDestinationMetricsHolder(BalancingDestinationMetricsHolder holder) {
        this.destinationMetricsHolder = holder;
        return this;
    }

    public BalancingDestinationMetricsHolder getDestinationMetricsHolder() {
        return destinationMetricsHolder;
    }

    public Duration getChannelPoolRebalanceInterval() {
        return channelPoolRebalanceInterval;
    }

    public RpcOptions setChannelPoolRebalanceInterval(Duration channelPoolRebalanceInterval) {
        this.channelPoolRebalanceInterval = channelPoolRebalanceInterval;
        return this;
    }

    public int getChannelPoolSize() {
        return channelPoolSize;
    }

    public RpcOptions setChannelPoolSize(int channelPoolSize) {
        this.channelPoolSize = channelPoolSize;
        return this;
    }

    public Duration getRpcClientSelectionTimeout() {
        return rpcClientSelectionTimeout;
    }

    public void setRpcClientSelectionTimeout(Duration rpcClientSelectionTimeout) {
        this.rpcClientSelectionTimeout = rpcClientSelectionTimeout;
    }

    public DiscoveryMethod getPreferableDiscoveryMethod() {
        return preferableDiscoveryMethod;
    }

    public void setPreferableDiscoveryMethod(DiscoveryMethod preferableDiscoveryMethod) {
        this.preferableDiscoveryMethod = preferableDiscoveryMethod;
    }

    /**
     * @see #setNewDiscoveryServiceEnabled
     */
    public boolean isNewDiscoveryServiceEnabled() {
        return newDiscoveryServiceEnabled;
    }

    /**
     * Whether or not to use new discovery service.
     *
     * <p> This option is temporary and we are going to move all clients to new discovery service.
     * Once we are sure our new service is pretty stable this option will be removed.
     */
    public RpcOptions setNewDiscoveryServiceEnabled(boolean newDiscoveryServiceEnabled) {
        this.newDiscoveryServiceEnabled = newDiscoveryServiceEnabled;
        return this;
    }

    /**
     * @see #setAcknowledgementTimeout
     */
    public @Nullable Duration getAcknowledgementTimeout() {
        return isNewDiscoveryServiceEnabled() ? acknowledgementTimeout : null;
    }

    /**
     * Set acknowledgement timeout.
     *
     * Client will fail request if acknowledgement is not received within this timeout.
     * Works only with newDiscoveryServiceEnabled
     *
     * @see #setNewDiscoveryServiceEnabled
     */
    public RpcOptions setAcknowledgementTimeout(@Nullable Duration acknowledgementTimeout) {
        this.acknowledgementTimeout = acknowledgementTimeout;
        return this;
    }

    /**
     * Set minimal backoff time.
     * <p>
     *     When retrying request ytclient might wait for some time before making next attempt.
     *     This time lies in interval [minBackoffTime, maxBackoffTime].
     *     Exact value is unspecified. It might depend on:
     *       - error that is being retried (e.g RequestQueueSizeLimitExceeded is retried with increasing backoff time)
     *       - version of the ytclient library (we might tune backoff times)
     * </p>
     * @see #setMaxBackoffTime
     */
    public RpcOptions setMinBackoffTime(Duration minBackoffTime) {
        this.minBackoffTime = Objects.requireNonNull(minBackoffTime);
        return this;
    }

    /**
     * Set maximum backoff time.
     *
     * @see #setMinBackoffTime for explanation.
     */
    public RpcOptions setMaxBackoffTime(Duration maxBackoffTime) {
        this.maxBackoffTime = Objects.requireNonNull(maxBackoffTime);
        return this;
    }

    /**
     * Get minimum backoff time.
     *
     * @see #setMinBackoffTime for explanation.
     */
    public Duration getMinBackoffTime() {
        return minBackoffTime;
    }

    /**
     * Get maximum backoff time.
     *
     * @see #setMinBackoffTime for explanation.
     * @see #setMaxBackoffTime
     */
    public Duration getMaxBackoffTime() {
        return maxBackoffTime;
    }

    public ThreadFactory getDiscoveryThreadFactory() {
        return discoveryThreadFactory;
    }

    public RpcOptions setDiscoveryThreadFactory(ThreadFactory discoveryThreadFactory) {
        this.discoveryThreadFactory = discoveryThreadFactory;
        return this;
    }
}
