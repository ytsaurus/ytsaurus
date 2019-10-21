package ru.yandex.yt.jdbc;

import java.util.Objects;
import java.util.Properties;

import ru.yandex.misc.io.InputStreamSourceUtils2;
import ru.yandex.yt.ytclient.rpc.internal.Compression;

import static ru.yandex.yt.jdbc.YtClientParameters.COMPRESSION;
import static ru.yandex.yt.jdbc.YtClientParameters.DEBUG_OUTPUT;
import static ru.yandex.yt.jdbc.YtClientParameters.HOME;
import static ru.yandex.yt.jdbc.YtClientParameters.MAX_INPUT_LIMIT;
import static ru.yandex.yt.jdbc.YtClientParameters.SCAN_RECURSIVE;
import static ru.yandex.yt.jdbc.YtClientParameters.TOKEN;
import static ru.yandex.yt.jdbc.YtClientParameters.USERNAME;

public class YtClientProperties {

    private final String proxy;
    private final String username;
    private final String token;
    private final Compression compression;
    private final boolean debugOutput;
    private final int maxInputLimit;
    private final String home;
    private final boolean scanRecursive;

    YtClientProperties(String proxy, Properties properties) {
        this.proxy = Objects.requireNonNull(proxy, "'proxy' is mandatory");
        this.username = Objects.requireNonNull(USERNAME.readValue(properties), "'username' is mandatory");
        this.token = readToken(Objects.requireNonNull(TOKEN.readValue(properties), "'token' is mandatory"));
        this.compression = Compression.valueOf(COMPRESSION.readValue(properties));
        this.debugOutput = Boolean.parseBoolean(DEBUG_OUTPUT.readValue(properties));
        this.maxInputLimit = Integer.parseInt(MAX_INPUT_LIMIT.readValue(properties));
        this.home = HOME.readValue(properties);
        this.scanRecursive = Boolean.parseBoolean(SCAN_RECURSIVE.readValue(properties));
    }

    String getProxy() {
        return proxy;
    }

    String getUsername() {
        return username;
    }

    String getToken() {
        return token;
    }

    Compression getCompression() {
        return compression;
    }

    boolean isDebugOutput() {
        return debugOutput;
    }

    int getMaxInputLimit() {
        return maxInputLimit;
    }

    String getHome() {
        return home;
    }

    boolean isScanRecursive() {
        return scanRecursive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YtClientProperties)) {
            return false;
        }
        YtClientProperties that = (YtClientProperties) o;
        return debugOutput == that.debugOutput &&
                maxInputLimit == that.maxInputLimit &&
                scanRecursive == that.scanRecursive &&
                Objects.equals(proxy, that.proxy) &&
                Objects.equals(username, that.username) &&
                Objects.equals(token, that.token) &&
                compression == that.compression &&
                Objects.equals(home, that.home);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxy, username, token, compression, debugOutput, maxInputLimit, home, scanRecursive);
    }

    @Override
    public String toString() {
        return "YtClientProperties{" +
                "proxy='" + proxy + '\'' +
                ", username='" + username + '\'' +
                ", compression=" + compression +
                ", debugOutput=" + debugOutput +
                ", maxInputLimit=" + maxInputLimit +
                ", home='" + home + '\'' +
                ", scanRecursive=" + scanRecursive +
                '}';
    }

    private static String readToken(String token) {
        if (token.startsWith("file:")) {
            return InputStreamSourceUtils2.valueOf(token).readText().trim();
        } else {
            return token;
        }
    }
}
