package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ColumnarStatistics;
import tech.ytsaurus.client.request.GetColumnarStatistics;
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientTestStubs;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpcproxy.TColumnarStatistics;
import tech.ytsaurus.rpcproxy.TReqGetColumnarStatistics;
import tech.ytsaurus.rpcproxy.TRspGetColumnarStatistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GetColumnarStatisticsClientTest {
    // Compact row [int64 -5]: version, count, id, type, ZigZag(-5).
    private static final byte[] MIN_ROW = {
            0, 1, 0, (byte) ColumnValueType.INT64.getValue(), 9
    };
    // Compact row [int64 7]; ZigZag(7) = 14.
    private static final byte[] MAX_ROW = {
            0, 1, 0, (byte) ColumnValueType.INT64.getValue(), 14
    };

    @Parameterized.Parameter
    public boolean transactional;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    @Parameterized.Parameters(name = "transactional={0}")
    public static Object[][] params() {
        return new Object[][]{{false}, {true}};
    }

    @After
    public void shutdown() {
        executor.shutdownNow();
    }

    @Test
    public void sendsRequestAndConvertsResponse() throws Exception {
        TRspGetColumnarStatistics response = TRspGetColumnarStatistics.newBuilder()
                .addStatistics(TColumnarStatistics.newBuilder()
                        .addColumnDataWeights(80)
                        .setColumnMinValues(ByteString.copyFrom(MIN_ROW))
                        .setColumnMaxValues(ByteString.copyFrom(MAX_ROW))
                        .addColumnNonNullValueCounts(10))
                .addStatistics(TColumnarStatistics.newBuilder()
                        .addColumnDataWeights(7))
                .build();
        RespondingRpcClient rpcClient = new RespondingRpcClient(response);
        YPath first = YPath.simple("//tmp/first").withColumns("key").withRange(2, 10);
        YPath second = YPath.simple("//tmp/second").withColumns("key");

        ApiServiceClientImpl client = new ApiServiceClientImpl(
                rpcClient, new RpcOptions(), executor, executor, DefaultSerializationResolver.getInstance());
        GUID transactionId = GUID.create();
        TransactionalClient requestClient = transactional
                ? new ApiServiceTransaction(client, transactionId, YtTimestamp.MIN,
                        false, false, false, Duration.ofSeconds(30), executor)
                : client;
        List<ColumnarStatistics> result;
        try {
            result = requestClient.getColumnarStatistics(GetColumnarStatistics.builder()
                    .setPaths(first, second)
                    .setAdditionalData(TReqGetColumnarStatistics.newBuilder().setEnableReadSizeEstimation(true).build())
                    .build()).get(30, TimeUnit.SECONDS);
        } finally {
            client.close();
        }

        TReqGetColumnarStatistics body = (TReqGetColumnarStatistics) rpcClient.request.body;
        assertEquals(List.of(ByteString.copyFrom(first.toTree().toBinary()),
                ByteString.copyFrom(second.toTree().toBinary())), body.getPathsList());
        assertTrue(body.getEnableReadSizeEstimation());
        if (transactional) {
            assertEquals(new TransactionalOptions(transactionId).toProto(), body.getTransactionalOptions());
        } else {
            assertFalse(body.hasTransactionalOptions());
        }

        assertEquals(2, result.size());
        ColumnarStatistics firstStatistics = result.get(0);
        assertEquals(List.of(80L), firstStatistics.getColumnDataWeights());
        assertEquals(-5L, firstStatistics.getColumnMinValues().get(0).longValue());
        assertEquals(7L, firstStatistics.getColumnMaxValues().get(0).longValue());
        assertEquals(List.of(10L), firstStatistics.getColumnNonNullValueCounts());
        ColumnarStatistics secondStatistics = result.get(1);
        assertEquals(List.of(7L), secondStatistics.getColumnDataWeights());
        assertEquals(List.of(), secondStatistics.getColumnMinValues());
        assertEquals(List.of(), secondStatistics.getColumnMaxValues());
    }

    private static class RespondingRpcClient extends RpcClientTestStubs.RpcClientStub {
        private final TRspGetColumnarStatistics response;
        private volatile RpcRequest<?> request;

        RespondingRpcClient(TRspGetColumnarStatistics response) {
            super("responding");
            this.response = response;
        }

        @Override
        public RpcClientRequestControl send(
                RpcClient sender,
                RpcRequest<?> request,
                RpcClientResponseHandler handler,
                RpcOptions options
        ) {
            this.request = request;
            TResponseHeader header = TResponseHeader.newBuilder()
                    .setCodec(Compression.None.getValue())
                    .build();
            handler.onResponse(this, header, List.of(response.toByteArray()));
            return () -> false;
        }
    }
}
