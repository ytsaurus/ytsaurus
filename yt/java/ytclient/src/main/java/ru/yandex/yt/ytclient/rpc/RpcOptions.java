package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;

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
    private String serviceName = null;
    private int protocolVersion = 0;
    private Duration defaultTimeout = null;
    private boolean defaultRequestAck = true;

    private Duration globalTimeout = Duration.ofMillis(60000);  // fails request after this timeout
    private Duration failoverTimeout = Duration.ofMillis(30000); // sends fallback request to other proxy after this timeout
    private Duration proxyUpdateTimeout = Duration.ofMillis(60000);
    private Duration pingTimeout = Duration.ofMillis(5000); // marks proxy as dead/live after this timeout

    private RpcFailoverPolicy failoverPolicy = new DefaultRpcFailoverPolicy();
    private BalancingResponseHandlerMetricsHolder responseMetricsHolder = new BalancingResponseHandlerMetricsHolderImpl();
    private DataCenterMetricsHolder dataCenterMetricsHolder = DataCenterMetricsHolderImpl.instance;
    private BalancingDestinationMetricsHolder destinationMetricsHolder = new BalancingDestinationMetricsHolderImpl();

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
}
