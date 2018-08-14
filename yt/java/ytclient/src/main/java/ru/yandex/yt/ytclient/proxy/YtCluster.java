package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;

public class YtCluster {
    final String clusterUrl;
    final String name;
    final List<String> initialProxies;

    public YtCluster(String name, String clusterUrl, List<String> initialProxies) {
        this.name = name;
        this.clusterUrl = clusterUrl;
        this.initialProxies = initialProxies;
    }

    public YtCluster(String name) {
        this(name, "http://" + name + ".yt.yandex.net", new ArrayList<>());
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public String getName() {
        return name;
    }
}
