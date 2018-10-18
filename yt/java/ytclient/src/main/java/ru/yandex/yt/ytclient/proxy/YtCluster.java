package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class YtCluster {
    final String balancerFqdn;
    int httpPort;
    final String name;
    final List<String> addresses;
    final Optional<String> proxyRole;

    public YtCluster(String name, String balancerFqdn, int httpPort, List<String> addresses, Optional<String> proxyRole) {
        this.name = name;
        this.balancerFqdn = balancerFqdn;
        this.httpPort = httpPort;
        this.addresses = addresses;
        this.proxyRole = proxyRole;
    }

    public YtCluster(String name, String balancerFqdn, int httpPort, List<String> addresses) {
        this(name, balancerFqdn, httpPort, addresses, Optional.empty());
    }

    public YtCluster(String name, String balancerFqdn, int httpPort) {
        this(name, balancerFqdn, httpPort, new ArrayList<>());
    }

    public YtCluster(String name) {
        this(name, name + ".yt.yandex.net", 80, new ArrayList<>());
    }

    public String getName() {
        return name;
    }
}
