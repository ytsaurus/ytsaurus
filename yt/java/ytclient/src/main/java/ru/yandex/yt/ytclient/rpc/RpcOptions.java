package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;

import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolderImpl;

/**
 * Опции для создания rpc клиентов
 */
public class RpcOptions {
    private String serviceName = null;
    private int protocolVersion = 0;
    private Duration defaultTimeout = null;
    private boolean defaultRequestAck = true;

    private Duration globalTimeout = Duration.ofMillis(60000);
    private Duration failoverTimeout = Duration.ofMillis(30000);
    private RpcFailoverPolicy failoverPolicy = new DefaultRpcFailoverPolicy();
    private BalancingResponseHandlerMetricsHolder metricsHolder = new BalancingResponseHandlerMetricsHolderImpl();

    public RpcOptions() {
        // nothing
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public boolean getDefaultRequestAck() {
        return defaultRequestAck;
    }

    public RpcOptions setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public RpcOptions setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
        return this;
    }

    public RpcOptions setDefaultTimeout(Duration defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
        return this;
    }

    public RpcOptions setDefaultRequestAck(boolean defaultRequestAck) {
        this.defaultRequestAck = defaultRequestAck;
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

    public RpcFailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    public RpcOptions setFailoverPolicy(RpcFailoverPolicy failoverPolicy) {
        this.failoverPolicy = failoverPolicy;
        return this;
    }

    public RpcOptions setMetricsHolder(BalancingResponseHandlerMetricsHolder metricsHolder) {
        this.metricsHolder = metricsHolder;
        return this;
    }

    public BalancingResponseHandlerMetricsHolder getMetricsHolder() {
        return metricsHolder;
    }
}
