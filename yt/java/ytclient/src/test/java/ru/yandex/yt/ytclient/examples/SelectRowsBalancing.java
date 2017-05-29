package ru.yandex.yt.ytclient.examples;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

/**
 * Created by aozeritsky on 24.05.2017.
 */
public class SelectRowsBalancing {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsExample.class);

    public static void main(String[] args) throws Exception {
        BusConnector connector = ExamplesUtil.createConnector();
        String user = ExamplesUtil.getUser();
        String token = ExamplesUtil.getToken();

        RpcClient rpcClient = new BalancingRpcClient(
            Duration.ofMillis(60),
            ExamplesUtil.createRpcClient(connector, user, token),
            ExamplesUtil.createRpcClient(connector, user, token),
            ExamplesUtil.createRpcClient(connector, user, token),
            ExamplesUtil.createRpcClient(connector, user, token)
            );

        ApiServiceClient client = new ApiServiceClient(rpcClient,
            new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));

        try {
            long t0 = System.nanoTime();
            UnversionedRowset rowset = client.selectRows(
                "OrderID, UpdateTime, ClientID, Shows, Clicks FROM [//yabs/GPStat3.dynamic] LIMIT 10")
                .join();
            long t1 = System.nanoTime();
            logger.info("Request time: {}", (t1 - t0) / 1000000.0);

            logger.info("Result schema:");

            for (ColumnSchema column : rowset.getSchema().getColumns()) {
                logger.info("    {}", column.getName());
            }
            for (UnversionedRow row : rowset.getRows()) {
                logger.info("Row:");
                for (UnversionedValue value : row.getValues()) {
                    logger.info("    value: {}", value);
                }
            }
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }

        } catch (Throwable e) {
            logger.error("error `{}`", e.toString());
        }
    }
}
