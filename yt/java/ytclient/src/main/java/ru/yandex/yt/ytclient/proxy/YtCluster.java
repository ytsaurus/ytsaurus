package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;

public class YtCluster {
    final String balancerFqdn;
    int httpPort;
    final String name;
    final List<String> initialProxies;

    public YtCluster(String name, String balancerFqdn, int httpPort, List<String> initialProxies) {
        this.name = name;
        this.balancerFqdn = balancerFqdn;
        this.httpPort = httpPort;
        this.initialProxies = initialProxies;
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
