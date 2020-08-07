package ru.yandex.yt.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.misc.lang.StringUtils;
import ru.yandex.yt.TError;
import ru.yandex.yt.ytclient.proxy.SelectRowsRequest;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public class YtStatement extends AbstractWrapper implements Statement {

    private static final Logger LOGGER = LoggerFactory.getLogger(YtStatement.class);

    private final YtConnection connection;
    private final YtClient client;
    private final int inputLimit;
    private final boolean allowJoinWithoutIndex;
    private final String udfRegistryPath;

    private boolean closed;
    private int maxRows = 10_000;
    private int queryTimeout;
    private int fetchSize;

    private volatile CompletableFuture<UnversionedRowset> future;
    private YtResultSet resultSet;

    YtStatement(YtConnection connection) {
        this.connection = connection;

        final YtClientWrapper wrapper = connection.getWrapper();
        this.client = wrapper.getClient();

        final YtClientProperties props = wrapper.getProperties();
        this.inputLimit = props.getMaxInputLimit();
        this.allowJoinWithoutIndex = props.isAllowJoinWithoutIndex();
        this.udfRegistryPath = props.getUdfRegistryPath();
    }

    YtConnection getYtConnection() {
        return this.connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (StringUtils.isEmpty(sql)) {
            throw new SQLException("sql cannot be empty");
        }
        sql = sql.trim();
        if (StringUtils.startsWithIgnoreCase(sql, "select ")) {
            sql = sql.substring("select ".length());
        }
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        // maxRows может быть аггрессивно задана в IDE (и быть меньше, чем limit при постраничной выборке)
        final SelectRowsRequest request = SelectRowsRequest.of(sql)
                .setInputRowsLimit(inputLimit)
                .setOutputRowsLimit(maxRows)
                .setFailOnIncompleteResult(false)
                .setAllowJoinWithoutIndex(allowJoinWithoutIndex)
                .setUdfRegistryPath(udfRegistryPath);

        this.future = client.selectRows(request);
        final UnversionedRowset rowSet;
        try {
            rowSet = this.future.join();
        } catch (CompletionException e) {
            LOGGER.error("Error when executing query", e);
            if (e.getCause() instanceof RpcError) {
                final RpcError rpcError = (RpcError) e.getCause();
                final TError error = rpcError.getError();
                if (error != null && error.getInnerErrorsList() != null && !error.getInnerErrorsList().isEmpty()) {
                    throw new SQLException(error.getInnerErrorsList().stream()
                            .map(TError::getMessage)
                            .collect(Collectors.joining("\n")), e);
                }
            }
            throw e;
        } finally {
            this.future = null;
        }
        this.resultSet = new YtResultSet(this, rowSet.getSchema(), rowSet.getYTreeRows());
        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        //
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        //
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        final CompletableFuture<UnversionedRowset> currentFuture = this.future;
        if (currentFuture != null) {
            currentFuture.cancel(true);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        this.executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // do nothing
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        //
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        //
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }


}
