package tech.ytsaurus.client.rpc;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import tech.ytsaurus.client.DiscoveryMethod;
import tech.ytsaurus.client.ProxySelector;
import tech.ytsaurus.client.RetryPolicy;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Options for creating RPC clients.
 */
@NonNullApi
@NonNullFields
public class RpcOptions {
    /**
     * @see #setDefaultRequestAck
     */
    private boolean defaultRequestAck = true;

    /**
     * @see #setAcknowledgementTimeout
     */
    @Nullable
    private Duration acknowledgementTimeout = Duration.ofSeconds(15);

    /**
     * @see #setGlobalTimeout
     */
    private Duration globalTimeout = Duration.ofMillis(60000);

    /**
     * @see #setFailoverTimeout
     */
    private Duration failoverTimeout = Duration.ofMillis(30000);

    /**
     * @see #setProxyUpdateTimeout
     */
    private Duration proxyUpdateTimeout = Duration.ofMillis(60000);

    /**
     * @see #setChannelPoolSize
     */
    private int channelPoolSize = 3;

    /**
     * @see #setTestingOptions(TestingOptions)
     */
    private TestingOptions testingOptions = new TestingOptions();

    /**
     * @see #setMinBackoffTime
     */
    private Duration minBackoffTime = Duration.ofSeconds(3);

    /**
     * @see #setMaxBackoffTime
     */
    private Duration maxBackoffTime = Duration.ofSeconds(30);

    // Streaming options.
    private int windowSize = 32 * 1024 * 1024;
    @Nullable
    private Duration writeTimeout = Duration.ofMillis(60000);
    @Nullable
    private Duration readTimeout = Duration.ofMillis(60000);

    /**
     * @see #setRetryPolicyFactory(Supplier)
     */
    private Supplier<RetryPolicy> retryPolicyFactory =
            () -> RetryPolicy.attemptLimited(3, RetryPolicy.fromRpcFailoverPolicy(new DefaultRpcFailoverPolicy()));

    /**
     * @see #setResponseMetricsHolder
     */
    private BalancingResponseHandlerMetricsHolder responseMetricsHolder =
            new BalancingResponseHandlerMetricsHolderImpl();

    private boolean traceEnabled = false;
    private boolean traceSampled = false;
    private boolean traceDebug = false;

    private DiscoveryMethod preferableDiscoveryMethod = DiscoveryMethod.RPC;

    /**
     * @see #setRpcProxySelector
     */
    private ProxySelector rpcProxySelector = ProxySelector.random();

    public RpcOptions() {
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

    /**
     * If notifications about message delivery are needed.
     * @see tech.ytsaurus.client.bus.BusDeliveryTracking
     */
    public RpcOptions setDefaultRequestAck(boolean defaultRequestAck) {
        this.defaultRequestAck = defaultRequestAck;
        return this;
    }

    /**
     * @see #setTestingOptions(TestingOptions)
     */
    public TestingOptions getTestingOptions() {
        return this.testingOptions;
    }

    /**
     * Allows affecting requests and responses for testing purposes.
     * For example, to emulate errors from the server and to get sent requests in tests.
     * @return self
     */
    public RpcOptions setTestingOptions(TestingOptions testingOptions) {
        this.testingOptions = testingOptions;
        return this;
    }

    /**
     * @see #setFailoverTimeout
     */
    public Duration getFailoverTimeout() {
        return failoverTimeout;
    }

    /**
     * Sends fallback request to other proxy after this timeout.
     */
    public RpcOptions setFailoverTimeout(Duration failoverTimeout) {
        this.failoverTimeout = failoverTimeout;
        return this;
    }

    /**
     * @see #setGlobalTimeout
     */
    public Duration getGlobalTimeout() {
        return globalTimeout;
    }


    /**
     * Fails request after this timeout.
     * @return self
     */
    public RpcOptions setGlobalTimeout(Duration globalTimeout) {
        this.globalTimeout = globalTimeout;
        return this;
    }

    /**
     * @see #setProxyUpdateTimeout
     */
    public Duration getProxyUpdateTimeout() {
        return proxyUpdateTimeout;
    }

    /**
     * How often new rpc proxies will be discovered.
     * @return self
     */
    public RpcOptions setProxyUpdateTimeout(Duration timeout) {
        this.proxyUpdateTimeout = timeout;
        return this;
    }

    public RpcOptions setStreamingWindowSize(int bytes) {
        this.windowSize = bytes;
        return this;
    }

    public int getStreamingWindowSize() {
        return this.windowSize;
    }

    public RpcOptions setStreamingWriteTimeout(Duration timeout) {
        this.writeTimeout = timeout;
        return this;
    }

    public Optional<Duration> getStreamingWriteTimeout() {
        return Optional.ofNullable(this.writeTimeout);
    }

    public RpcOptions setStreamingReadTimeout(@Nullable Duration timeout) {
        this.readTimeout = timeout;
        return this;
    }

    public Optional<Duration> getStreamingReadTimeout() {
        return Optional.ofNullable(this.readTimeout);
    }

    /**
     * @deprecated Use {@link #setRetryPolicyFactory(Supplier)} instead.
     */
    @Deprecated
    public RpcOptions setFailoverPolicy(RpcFailoverPolicy failoverPolicy) {
        this.retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                3, RetryPolicy.fromRpcFailoverPolicy(failoverPolicy));
        return this;
    }

    /**
     * @return retry policy factory
     */
    public Supplier<RetryPolicy> getRetryPolicyFactory() {
        return this.retryPolicyFactory;
    }

    /**
     * Allow setting custom factory of retry policies.
     * @return self
     */
    public RpcOptions setRetryPolicyFactory(Supplier<RetryPolicy> retryPolicyFactory) {
        this.retryPolicyFactory = retryPolicyFactory;
        return this;
    }

    /**
     * Metrics holder, calculate request metrics
     * @see BalancingResponseHandlerMetricsHolderImpl
     */
    public RpcOptions setResponseMetricsHolder(BalancingResponseHandlerMetricsHolder responseMetricsHolder) {
        this.responseMetricsHolder = responseMetricsHolder;
        return this;
    }

    /**
     * @see #setResponseMetricsHolder
     */
    public BalancingResponseHandlerMetricsHolder getResponseMetricsHolder() {
        return responseMetricsHolder;
    }

    /**
     * @see #setChannelPoolSize
     */
    public int getChannelPoolSize() {
        return channelPoolSize;
    }

    /**
     * Set maximum amount of opened rpc-proxy connections.
     * @return self
     */
    public RpcOptions setChannelPoolSize(int channelPoolSize) {
        this.channelPoolSize = channelPoolSize;
        return this;
    }

    public DiscoveryMethod getPreferableDiscoveryMethod() {
        return preferableDiscoveryMethod;
    }

    public void setPreferableDiscoveryMethod(DiscoveryMethod preferableDiscoveryMethod) {
        this.preferableDiscoveryMethod = preferableDiscoveryMethod;
    }

    /**
     * @see #setAcknowledgementTimeout
     */
    public Optional<Duration> getAcknowledgementTimeout() {
        return Optional.ofNullable(acknowledgementTimeout);
    }

    /**
     * Set acknowledgement timeout.
     * <p>
     * Client will fail request if acknowledgement is not received within this timeout.
     */
    public RpcOptions setAcknowledgementTimeout(@Nullable Duration acknowledgementTimeout) {
        this.acknowledgementTimeout = acknowledgementTimeout;
        return this;
    }

    /**
     * Set minimal backoff time.
     * <p>
     * When retrying request ytclient might wait for some time before making next attempt.
     * This time lies in interval [minBackoffTime, maxBackoffTime].
     * Exact value is unspecified. It might depend on:
     * - error that is being retried (e.g RequestQueueSizeLimitExceeded is retried with increasing backoff time)
     * - version of the ytclient library (we might tune backoff times)
     * </p>
     *
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

    /**
     * Get current {@link ProxySelector}
     *
     * @see ProxySelector
     */
    public ProxySelector getRpcProxySelector() {
        return rpcProxySelector;
    }

    /**
     * Set {@link ProxySelector} for ranking of a proxy list.
     *
     * @see ProxySelector for a list of available implementations
     */
    public RpcOptions setRpcProxySelector(@Nullable ProxySelector rpcProxySelector) {
        if (rpcProxySelector == null) {
            this.rpcProxySelector = ProxySelector.random();
            return this;
        }
        this.rpcProxySelector = rpcProxySelector;
        return this;
    }
}
