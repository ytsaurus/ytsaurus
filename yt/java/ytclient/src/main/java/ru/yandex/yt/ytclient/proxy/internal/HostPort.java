package ru.yandex.yt.ytclient.proxy.internal;

import java.util.Objects;

public class HostPort
{
    final String host;
    final int port;

    private HostPort(String host, int port)
    {
        this.host = Objects.requireNonNull(host);
        this.port = port;
    }

    private HostPort(String host)
    {
        this(host, 9013);
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public static HostPort parse(String hostPort) {
        String[] parts = hostPort.split(":");
        if (parts.length > 1) {
            return new HostPort(parts[0], Integer.parseInt(parts[1]));
        } else {
            return new HostPort(parts[0]);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostPort hostPort = (HostPort) o;

        return host.equals(hostPort.getHost()) && port == hostPort.getPort();
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }
}
