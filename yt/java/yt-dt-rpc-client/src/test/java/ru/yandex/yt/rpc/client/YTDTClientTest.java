package ru.yandex.yt.rpc.client;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import protocol.ApiService;

import ru.yandex.yt.rpc.channel.ClientChannelPool;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.protocol.RpcRequestMessage;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.proto.ProtobufHelpers;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspLookupRows;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author valri
 */
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({YtDtClient.class, ProtobufHelpers.class})
public class YTDTClientTest {
    private ClientChannelPool mockChannelPool;
    private YtDtClient mockClient;
    private int numberOfTries = 5;
    private int requestTimeout = 100;
    private int channelReadTimeoutSeconds = 5;
    private LookupReqInfo lookupInfo;

    @Before
    public void setUp() throws Exception {
        mockChannelPool = mock(ClientChannelPool.class);
        PowerMockito.whenNew(ClientChannelPool.class).withAnyArguments().thenReturn(mockChannelPool);
        mockClient = new YtDtClient(new InetSocketAddress("1.1.1.1", 8080), "token", "user", "domain", 123,
                numberOfTries, requestTimeout, channelReadTimeoutSeconds);

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema((short) 0, "BannerHash", ValueType.INT_64));
        columns.add(new ColumnSchema((short) 5, "Attr", ValueType.STRING));
        columns.add(new ColumnSchema((short) 6, "UpdateTime", ValueType.INT_64));
        columns.add(new ColumnSchema((short) 1, "BannerID", ValueType.INT_64));
        columns.add(new ColumnSchema((short) 3, "ResourcePart", ValueType.INT_64));
        columns.add(new ColumnSchema((short) 2, "ResourceNo", ValueType.INT_64));
        columns.add(new ColumnSchema((short) 4, "Display", ValueType.STRING));

        TableSchema schema = new TableSchema("//yabs/Resource", columns);
        List<Map<String, Object>> filter = new ArrayList<>();
        Map<String, Object> firstFilter = new HashMap<>();
        firstFilter.put("BannerID", 1459262910);
        firstFilter.put("ResourceNo", 2);
        firstFilter.put("ResourcePart", 0);
        Map<String, Object> secondFilter = new HashMap<>();
        secondFilter.put("BannerID", 2813512494L);
        secondFilter.put("ResourceNo", 3);
        secondFilter.put("ResourcePart", 0);
        filter.add(firstFilter);
        filter.add(secondFilter);
        this.lookupInfo = new LookupReqInfo(schema, filter, 1, UUID.randomUUID());
    }

    @Test
    public void retryRequest() throws Exception {
        BusPackage returned = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, new ArrayList<>());
        CompletableFuture<BusPackage> future = CompletableFuture.completedFuture(returned);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        Method retryRequest = YtDtClient.class.getDeclaredMethod("retryRequest", BusPackage.class,
                RpcRequestMessage.class);
        retryRequest.setAccessible(true);
        RpcRequestMessage pack = mock(RpcRequestMessage.class);
        assertEquals(returned, retryRequest.invoke(mockClient, returned, pack));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void retryRequestCancelled() throws Exception {
        CompletableFuture<BusPackage> future = (CompletableFuture<BusPackage>) mock(CompletableFuture.class);
        when(future.get(requestTimeout, TimeUnit.MILLISECONDS)).thenThrow(CancellationException.class);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);

        Method retryRequest = YtDtClient.class.getDeclaredMethod("retryRequest", BusPackage.class,
                RpcRequestMessage.class);
        retryRequest.setAccessible(true);
        retryRequest.invoke(mockClient, mock(BusPackage.class),  mock(RpcRequestMessage.class));
        verify(future, times(numberOfTries)).get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void retryRequestTimedOut() throws Exception {
        CompletableFuture<BusPackage> future = (CompletableFuture<BusPackage>) mock(CompletableFuture.class);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        when(future.get(requestTimeout, TimeUnit.MILLISECONDS)).thenThrow(TimeoutException.class);

        Method retryRequest = YtDtClient.class.getDeclaredMethod("retryRequest", BusPackage.class,
                RpcRequestMessage.class);
        retryRequest.setAccessible(true);
        retryRequest.invoke(mockClient, mock(BusPackage.class),  mock(RpcRequestMessage.class));
        verify(future, times(1)).get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void getNode() throws Exception {
        BusPackage returned = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, new ArrayList<>());
        CompletableFuture<BusPackage> future = CompletableFuture.completedFuture(returned);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        PowerMockito.mockStatic(ProtobufHelpers.class);
        when(ProtobufHelpers.validateHeader(any(), any())).thenReturn(true);
        String theAnswer = "Some info about Node";
        ApiService.TRspGetNode response = protocol.ApiService.TRspGetNode.newBuilder().setData(
                ByteString.copyFrom((theAnswer.getBytes()))).build();
        when(ProtobufHelpers.getProtoFromRsp(any(), any(), any())).thenReturn(response);
        assertEquals(mockClient.getNode("//@", UUID.randomUUID()), theAnswer);
    }

    @Test
    public void getNodeNotParsedProto() throws Exception {
        BusPackage returned = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, new ArrayList<>());
        CompletableFuture<BusPackage> future = CompletableFuture.completedFuture(returned);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        PowerMockito.mockStatic(ProtobufHelpers.class);
        when(ProtobufHelpers.validateHeader(any(), any())).thenReturn(true);
        when(ProtobufHelpers.getProtoFromRsp(any(), any(), any())).thenReturn(null);
        assertEquals(mockClient.getNode("//@", UUID.randomUUID()), "");
    }

    @Test
    public void getNodeEmptyFututreResponse() throws Exception {
        CompletableFuture<BusPackage> future = CompletableFuture.completedFuture(null);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        assertEquals(mockClient.getNode("//@", UUID.randomUUID()), "");
    }

    @Test
    public void lookupRows() throws Exception {
        BusPackage returned = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, new ArrayList<>());
        CompletableFuture<BusPackage> future = CompletableFuture.completedFuture(returned);
        PowerMockito.when(mockChannelPool.sendRequest(any())).thenReturn(future);
        PowerMockito.mockStatic(ProtobufHelpers.class);
        when(ProtobufHelpers.validateHeader(any(), any())).thenReturn(true);
        ApiService.TRspLookupRows response = protocol.ApiService.TRspLookupRows.newBuilder()
                .setRowsetDescriptor(ApiService.TRowsetDescriptor
                        .newBuilder()
                        .setWireFormatVersion(1)
                        .build())
                .build();
        when(ProtobufHelpers.getProtoFromRsp(any(), any(), any())).thenReturn(response);

        RpcRspLookupRows rspLookupRows = mock(RpcRspLookupRows.class);
        PowerMockito.whenNew(RpcRspLookupRows.class).withAnyArguments().thenReturn(rspLookupRows);
        Map<String, Object> mapa = new HashMap<>();
        mapa.put("BannerID", 123456789L);
        List<Map<String, Object>> r = new ArrayList<>();
        r.add(mapa);
        when(rspLookupRows.parseRpcResponse(any())).thenReturn(r);
        assertEquals(r, mockClient.lookupRows(lookupInfo, false, false));
    }
}
