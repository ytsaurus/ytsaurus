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
    @Nullable
    final Integer port;
    final String name;
    final List<String> addresses;
    @Nullable
    final String proxyRole;
    final boolean useTLS;

    public YTsaurusCluster(
            String name,
            String balancerFqdn,
            int port,
            List<String> addresses,
            @Nullable String proxyRole
    ) {
        this.name = cleanupName(name);
        this.balancerFqdn = balancerFqdn;
        this.port = port;
        this.addresses = addresses;
        this.proxyRole = proxyRole;
        this.useTLS = useTLS(name);
    }

    public YTsaurusCluster(String name, String balancerFqdn, int port, List<String> addresses) {
        this(name, balancerFqdn, port, addresses, null);
    }

    public YTsaurusCluster(String name, String balancerFqdn, int port) {
        this(name, balancerFqdn, port, new ArrayList<>());
    }

    public YTsaurusCluster(String name) {
        this.name = cleanupName(name);
        this.balancerFqdn = getFqdn(name);
        this.port = getPort(name);
        this.addresses = new ArrayList<>();
        this.proxyRole = null;
        this.useTLS = useTLS(name);
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
        name = cleanupName(name);
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

    @Nullable
    private static Integer getPort(String name) {
        name = cleanupName(name);
        int index = name.lastIndexOf(":");
        if (index < 0) {
            return null;
        }
        return Integer.parseInt(name.substring(index + 1));
    }

    private static String cleanupName(String name) {
        name = name.trim();
        for (String prefix : httpPrefixes) {
            if (name.startsWith(prefix)) {
                name = name.substring(prefix.length());
            }
        }
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        return name;
    }

    private static boolean useTLS(String name) {
        return name.startsWith("https://");
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
