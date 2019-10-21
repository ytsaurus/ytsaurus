package ru.yandex.yt.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;

public class YtDriver implements Driver {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(YtDriver.class);

    static {
        final YtDriver ytDriver = new YtDriver();
        try {
            DriverManager.registerDriver(ytDriver);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to register YT JDBC Driver", e);
        }
    }

    private static final Map<YtClientProperties, WrapperWithCounter> WRAPPERS = new ConcurrentHashMap<>();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }

        final YtClientProperties properties = YtJdbcUrlParser.parse(url, info);
        final WrapperWithCounter wrapper = WRAPPERS.computeIfAbsent(properties, props -> new WrapperWithCounter());

        synchronized (wrapper) {
            if (wrapper.impl == null) {
                wrapper.impl = new YtClientWrapper(properties);
                LOGGER.info("Created new connection for {}", properties);
            }
            wrapper.count++;

            final YtConnection connection = new YtConnection(url, wrapper.impl, () -> {
                synchronized (wrapper) {
                    wrapper.count--;
                    if (wrapper.count == 0) {
                        wrapper.impl.close();
                        LOGGER.info("Closed connection for {}", properties);
                        wrapper.impl = null;
                    }
                }
            });

            return wrapper.impl.wrap(Connection.class, connection);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(YtJdbcUrlParser.JDBC_YT_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final YtClientParameters[] params = YtClientParameters.values();

        final Collection<DriverPropertyInfo> result = new ArrayList<>(params.length);
        for (YtClientParameters param : params) {
            result.add(param.apply(info));
        }

        return result.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private static class WrapperWithCounter {
        private YtClientWrapper impl;
        private int count;

    }
}
