package ru.yandex.yt.rpc.handlers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import protocol.ApiService;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspLookupRows;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspVersionedLookupRows;

import static ru.yandex.yt.rpc.utils.Utility.byteArrayFromList;

/**
 * @author valri
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 15, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class Benchmarking {
    @Benchmark
    public Object checkDecoder() {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final ByteBuf out = ByteBufUtil.threadLocalDirectBuffer();
        final UUID uuid = UUID.randomUUID();
        final byte[] attachBytes = "iam a cute attachment".getBytes();
        final int numOfParts = 1;
        final List<Byte> headerPart = Bytes.asList(attachBytes);
        out.writeIntLE(BusPackage.DEFAULT_SIGNATURE);
        out.writeShortLE(BusPackage.PacketType.MESSAGE.getValue());
        out.writeShortLE(BusPackage.PacketFlags.REQUEST_ACK.getValue());
        out.writeLongLE(uuid.getMostSignificantBits());
        out.writeLongLE(uuid.getLeastSignificantBits());
        out.writeIntLE(numOfParts);
        out.writeLongLE(BusPackage.NULL_CHECKSUM);
        out.writeIntLE(attachBytes.length);
        out.writeLongLE(BusPackage.NULL_CHECKSUM);
        out.writeLongLE(BusPackage.NULL_CHECKSUM);
        out.writeBytes(byteArrayFromList(headerPart));

        ch.writeInbound(out);
        ch.writeInbound(ch.releaseOutbound());
        return ch.readInbound();
    }

    @Benchmark
    public Object checkEncoder() {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final UUID uuid = UUID.randomUUID();
        final byte[] attachBytes = "iam a cute attachment".getBytes();
        final List<Byte> headerPart = Bytes.asList(attachBytes);
        final List<List<Byte>> pack = new ArrayList<>();
        pack.add(headerPart);
        pack.add(Bytes.asList((
                "I AM A LOOOOOOOOOONG ATTTACHMENT I AM A LOOOOOOOOOONG ATTTACHMENT").getBytes()));
        String attach = String.join("", Collections.nCopies(1000,
                "I AM A LOOOOOOOOOONG ATTTACHMENT I AM A LOOOOOOOOOONG ATTTACHMENT"));
        pack.add(Bytes.asList((attach).getBytes()));

        BusPackage request = new BusPackage(BusPackage.PacketType.MESSAGE, BusPackage.PacketFlags.NONE,
                uuid, false, pack);

        ch.writeOutbound(request);
        ch.writeOutbound(ch.releaseInbound());

        return ch.readOutbound();
    }

    @Benchmark
    public Object checkParseRpcRespLookup() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema((short) 0, "columnId", ValueType.STRING));
        columns.add(new ColumnSchema((short) 5, "name", ValueType.STRING));
        columns.add(new ColumnSchema((short) 6, "city", ValueType.STRING));
        TableSchema schema = new TableSchema("//some/path/to/table", columns);
        RpcRspLookupRows parser = new RpcRspLookupRows(schema);

        final ByteBuffer out = ByteBuffer.allocate(48);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(1L);
        out.putLong(1L);
        out.putShort((short) 0);
        out.putShort(ValueType.STRING.getValue());
        String theString = "lalalalala";
        out.putInt(theString.getBytes().length);
        out.put(theString.getBytes());

        final ByteBuffer out2 = ByteBuffer.allocate(150);
        out2.order(ByteOrder.LITTLE_ENDIAN);
        out2.putLong(1L);
        out2.putLong(1L);
        out2.putShort((short) 5);
        out2.putShort(ValueType.STRING.getValue());
        String theString2 = "AM A LOOOOOOOOOONG ATTTACHMENT I AM A LOOOOOOOOOONG ATTTACHMEN";
        out2.putInt(theString2.getBytes().length);
        out2.put(theString2.getBytes());

        final ByteBuffer out3 = ByteBuffer.allocate(130000);
        out3.order(ByteOrder.LITTLE_ENDIAN);
        out3.putLong(1L);
        out3.putLong(1L);
        out3.putShort((short) 5);
        out3.putShort(ValueType.STRING.getValue());
        String attach = String.join("", Collections.nCopies(1000,
                "I AM A LOOOOOOOOOONG ATTTACHMENT I AM A LOOOOOOOOOONG ATTTACHMENT"));
        out3.putInt(attach.getBytes().length);
        out3.put(attach.getBytes());

        List<List<Byte>> parts = new ArrayList<>();
        parts.add(Bytes.asList(out.array()));
        parts.add(Bytes.asList(out2.array()));
        parts.add(Bytes.asList(out3.array()));
        return parser.parseRpcResponse(parts);
    }

    @Benchmark
    public Object checkParseRpcRespVersionedLookup() {
        protocol.ApiService.TRowsetDescriptor desc = ApiService.TRowsetDescriptor
                .newBuilder()
                .addColumns(ApiService.TRowsetDescriptor.TColumnDescriptor
                        .newBuilder()
                        .setName("columnId")
                        .setType(ValueType.STRING.getValue())
                        .build())
                .addColumns(ApiService.TRowsetDescriptor.TColumnDescriptor
                        .newBuilder()
                        .setName("name")
                        .setType(ValueType.STRING.getValue())
                        .build())
                .addColumns(ApiService.TRowsetDescriptor.TColumnDescriptor
                        .newBuilder()
                        .setName("city")
                        .setType(ValueType.STRING.getValue())
                        .build())
                .build();
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema((short) 0, "columnId", ValueType.STRING));
        columns.add(new ColumnSchema((short) 1, "name", ValueType.STRING));
        columns.add(new ColumnSchema((short) 2, "city", ValueType.STRING));
        TableSchema schema = new TableSchema("//some/path/to/table", columns);
        RpcRspVersionedLookupRows parser = new RpcRspVersionedLookupRows(desc, schema);

        final ByteBuffer out = ByteBuffer.allocate(100000);
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(1L);
        out.putInt(1);
        out.putInt(1);
        out.putInt(-1);
        out.putInt(-1);
        out.putLong(0L);
        String theString = "lalalala";
        out.putLong(theString.getBytes().length);
        out.put(theString.getBytes());

        out.putShort((short) 1);
        out.putShort(ValueType.STRING.getValue());
        String theString2 = String.join("", Collections.nCopies(1000,
                "I AM A LOOOOOOOOOONG ATTTACHMENT I AM A LOOOOOOOOOONG ATTTACHMENT"));
        out.putInt(theString2.getBytes().length);
        out.put(theString2.getBytes());
        out.putLong(1L);
        List<List<Byte>> parts = new ArrayList<>();
        parts.add(Bytes.asList(out.array()));
        return parser.parseRpcResponse(parts);
    }
}
