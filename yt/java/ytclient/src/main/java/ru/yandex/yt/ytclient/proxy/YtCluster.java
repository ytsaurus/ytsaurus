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
        this(name, getFqdn(name), getPort(name), new ArrayList<>());
    }

    public String getName() {
        return name;
    }

    static String getFqdn(String name) {
        int index = name.indexOf(":");
        if (index < 0) {
            if (name.contains(".")) {
                return name;
            } else {
                return name + ".yt.yandex.net";
            }
        } else {
            return name.substring(0, index);
        }
    }

    static int getPort(String name) {
        int index = name.lastIndexOf(":");
        if (index < 0) {
            return 80;
        } else {
            return Integer.parseInt(name.substring(index+1));
        }
    }
}
