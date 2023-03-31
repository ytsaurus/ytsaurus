package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class YTsaurusCluster {
    // This option is set to true only in tests.
    static boolean normalizationLowersHostName = false;
    static List<String> httpPrefixes = Arrays.asList("http://", "https://");

    final String balancerFqdn;
    int httpPort;
    final String name;
    final List<String> addresses;
    @Nullable
    final String proxyRole;

    public YTsaurusCluster(
            String name,
            String balancerFqdn,
            int httpPort,
            List<String> addresses,
            @Nullable String proxyRole
    ) {
        this.name = removeHttp(name);
        this.balancerFqdn = balancerFqdn;
        this.httpPort = httpPort;
        this.addresses = addresses;
        this.proxyRole = proxyRole;
    }

    public YTsaurusCluster(String name, String balancerFqdn, int httpPort, List<String> addresses) {
        this(name, balancerFqdn, httpPort, addresses, null);
    }

    public YTsaurusCluster(String name, String balancerFqdn, int httpPort) {
        this(name, balancerFqdn, httpPort, new ArrayList<>());
    }

    public YTsaurusCluster(String name) {
        this(name, getFqdn(name), getPort(name), new ArrayList<>());
    }

    public String getName() {
        return name;
    }

    static String getFqdn(String name) {
        if (name.startsWith("[")) {
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
        name = removeHttp(name);
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
        name = removeHttp(name);
        int index = name.lastIndexOf(":");
        if (index < 0) {
            return 80;
        } else {
            return Integer.parseInt(name.substring(index + 1));
        }
    }

    private static String removeHttp(String name) {
        for (String prefix : httpPrefixes) {
            if (name.startsWith(prefix)) {
                return name.substring(prefix.length());
            }
        }
        return name;
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
}
