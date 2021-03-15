package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

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
        if(name.startsWith("[")) {
            return getFqdnFromBracketed(name);
        } else {
            return getFqdnFromUnbracketed(name);
        }
    }

    private static String getFqdnFromBracketed(String name) {
        int index = name.indexOf("]");
        return name.substring(1, index);
    }

    private static String getFqdnFromUnbracketed(String name) {
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

    String getNormalizedName() {
        return normalizeName(name);
    }

    static @Nullable
    String normalizeName(@Nullable String name) {
        if (name == null) {
            return null;
        }
        if (normalizationLowersHostName) {
            name = name.toLowerCase();
        }
        if (name.equals("")) {
            return null;
        } else if (name.contains(":")) {
            return name;
        } else if (name.contains(".")) {
            return name;
        } else if (name.equals("localhost")) {
            return name;
        } else {
            return name + ".yt.yandex.net";
        }
    }

    public String getClusterUrl() {
        // "[]:12345" requires 8 extra bytes.
        StringBuilder builder = new StringBuilder(balancerFqdn.length() + 8);
        if (balancerFqdn.indexOf(':') >= 0) {
            builder.append('[').append(balancerFqdn).append(']');
        } else {
            builder.append(balancerFqdn);
        }
        builder.append(':').append(httpPort);
        return builder.toString();
    }

    // This option is set to true only in tests.
    static boolean normalizationLowersHostName = false;
}
