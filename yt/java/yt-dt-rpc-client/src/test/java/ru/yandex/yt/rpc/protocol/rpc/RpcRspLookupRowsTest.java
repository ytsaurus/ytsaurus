package ru.yandex.yt.rpc.protocol.rpc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspLookupRows;

import static org.junit.Assert.assertEquals;

/**
 * @author valri
 */
public class RpcRspLookupRowsTest {
    TableSchema schema;

    @Before
    public void setUp() throws Exception {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema((short) 0, "columnId", ValueType.INT_64));
        this.schema = new TableSchema("//yabs/Resource", columns);
    }

    @Test
    public void parseRpcResponse() throws Exception {
        RpcRspLookupRows parser = new RpcRspLookupRows(schema);

        final ByteBuffer out = ByteBuffer.allocate(32);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(1L);
        out.putLong(1L);
        out.putShort((short) 0);
        out.putShort(ValueType.INT_64.getValue());
        out.putInt(0);
        out.putLong(123456789L);

        List<List<Byte>> parts = new ArrayList<>();
        List<Byte> internal = Bytes.asList(out.array());
        parts.add(internal);

        Map<String, Object> mapa = new HashMap<>();
        mapa.put("columnId", 123456789L);
        List<Map<String, Object>> r = new ArrayList<>();
        r.add(mapa);
        assertEquals(r, parser.parseRpcResponse(parts));
    }


    @Test
    public void parseRpcResponseNullType() throws Exception {
        RpcRspLookupRows parser = new RpcRspLookupRows(schema);

        final ByteBuffer out = ByteBuffer.allocate(32);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(1L);
        out.putLong(1L);
        out.putShort((short) 0);
        out.putShort(ValueType.NULL.getValue());
        out.putInt(0);
        out.putLong(123456789L);

        List<List<Byte>> parts = new ArrayList<>();
        List<Byte> internal = Bytes.asList(out.array());
        parts.add(internal);

        Map<String, Object> mapa = new HashMap<>();
        mapa.put("columnId", null);
        List<Map<String, Object>> r = new ArrayList<>();
        r.add(mapa);
        assertEquals(r, parser.parseRpcResponse(parts));
    }


    @Test
    public void parseRpcResponseStringType() throws Exception {
        RpcRspLookupRows parser = new RpcRspLookupRows(schema);
        final ByteBuffer out = ByteBuffer.allocate(38);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(1L);
        out.putLong(1L);
        out.putShort((short) 0);
        out.putShort(ValueType.STRING.getValue());
        out.putInt("Buffalo".getBytes().length);
        out.put("Buffalo".getBytes());

        List<List<Byte>> parts = new ArrayList<>();
        List<Byte> internal = Bytes.asList(out.array());
        parts.add(internal);

        Map<String, Object> mapa = new HashMap<>();
        mapa.put("columnId", "Buffalo");
        List<Map<String, Object>> r = new ArrayList<>();
        r.add(mapa);
        assertEquals(r, parser.parseRpcResponse(parts));
    }
}
