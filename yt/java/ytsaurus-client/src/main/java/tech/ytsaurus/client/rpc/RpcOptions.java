package tech.ytsaurus.client.rpc;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.DiscoveryMethod;
import tech.ytsaurus.client.ProxySelector;
import tech.ytsaurus.client.RetryPolicy;

/**
 * Options for creating RPC clients.
 */
public class RpcOptions {
    private boolean defaultRequestAck = true;

    /**
     * @see #setAcknowledgementTimeout
     */
    private @Nullable
    Duration acknowledgementTimeout = Duration.ofSeconds(15);

    // Fails request after this timeout.
    private Duration globalTimeout = Duration.ofMillis(60000);

    // Sends fallback request to other proxy after this timeout.
    private Duration failoverTimeout = Duration.ofMillis(30000);
    private Duration proxyUpdateTimeout = Duration.ofMillis(60000);
    private int channelPoolSize = 3;

    private TestingOptions testingOptions = new TestingOptions();

    private Duration minBackoffTime = Duration.ofSeconds(3);
    private Duration maxBackoffTime = Duration.ofSeconds(30);

    // Streaming options.
    private Duration readTimeout = Duration.ofMillis(60000);
    private Duration writeTimeout = Duration.ofMillis(60000);
    private int windowSize = 32 * 1024 * 1024;

    @Nonnull
    private Supplier<RetryPolicy> retryPolicyFactory =
            () -> RetryPolicy.attemptLimited(3, RetryPolicy.fromRpcFailoverPolicy(new DefaultRpcFailoverPolicy()));

    private BalancingResponseHandlerMetricsHolder responseMetricsHolder =
            new BalancingResponseHandlerMetricsHolderImpl();

    private boolean traceEnabled = false;
    private boolean traceSampled = false;
    private boolean traceDebug = false;

    private DiscoveryMethod preferableDiscoveryMethod = DiscoveryMethod.RPC;

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

    public RpcOptions setDefaultRequestAck(boolean defaultRequestAck) {
        this.defaultRequestAck = defaultRequestAck;
        return this;
    }

    public TestingOptions getTestingOptions() {
        return this.testingOptions;
    }

    public RpcOptions setTestingOptions(TestingOptions testingOptions) {
        this.testingOptions = testingOptions;
        return this;
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
     * Allow setting custom factory of retry policies
     *
     * @return self
     */
    public RpcOptions setRetryPolicyFactory(Supplier<RetryPolicy> retryPolicyFactory) {
        this.retryPolicyFactory = retryPolicyFactory;
        return this;
    }

    public RpcOptions setResponseMetricsHolder(BalancingResponseHandlerMetricsHolder responseMetricsHolder) {
        this.responseMetricsHolder = responseMetricsHolder;
        return this;
    }

    public BalancingResponseHandlerMetricsHolder getResponseMetricsHolder() {
        return responseMetricsHolder;
    }

    public int getChannelPoolSize() {
        return channelPoolSize;
    }

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
    public @Nullable
    Duration getAcknowledgementTimeout() {
        return acknowledgementTimeout;
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
     * Set {@link ProxySelector} for ranking of a proxy list
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
