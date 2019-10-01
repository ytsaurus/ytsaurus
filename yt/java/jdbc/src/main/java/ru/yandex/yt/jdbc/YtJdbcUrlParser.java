package ru.yandex.yt.jdbc;

import java.net.URISyntaxException;
import java.util.Properties;

import ru.yandex.misc.lang.StringUtils;

class YtJdbcUrlParser {
    private static final String JDBC_PREFIX = "jdbc:";
    static final String JDBC_YT_PREFIX = JDBC_PREFIX + "yt:";

    private YtJdbcUrlParser() {
    }

    static YtClientProperties parse(String jdbcUrl, Properties properties) {
        try {
            if (!jdbcUrl.startsWith(JDBC_YT_PREFIX)) {
                throw new URISyntaxException(jdbcUrl, "'" + JDBC_YT_PREFIX + "' prefix is mandatory");
            }
            return parseYtUrl(jdbcUrl.substring(JDBC_YT_PREFIX.length()), properties);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to parse YT properties", e);
        }
    }

    private static YtClientProperties parseYtUrl(String proxy, Properties properties) throws URISyntaxException {
        return new YtClientProperties(StringUtils.stripStart(proxy, "/"), properties);

    }
}
