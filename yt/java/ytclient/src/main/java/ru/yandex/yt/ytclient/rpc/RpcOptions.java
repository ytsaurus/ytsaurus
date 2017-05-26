package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;

/**
 * Опции для создания rpc клиентов
 */
public class RpcOptions {
    private String serviceName = null;
    private int protocolVersion = 0;
    private Duration defaultTimeout = null;
    private boolean defaultRequestAck = true;

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
}
