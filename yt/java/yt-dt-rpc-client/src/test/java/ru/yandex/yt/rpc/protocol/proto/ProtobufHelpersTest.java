package ru.yandex.yt.rpc.protocol.proto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBufUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import protocol.Error;
import protocol.Guid;
import protocol.Rpc;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.rpc.RpcMessageType;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author valri
 */
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(ProtobufHelpers.class)
public class ProtobufHelpersTest {
    @Test
    public void getUuidFromHeader() throws Exception {
        Guid.TGuid uid = Guid.TGuid.newBuilder().setFirst(1L).setSecond(2L).build();
        Rpc.TResponseHeader responseHeader = Rpc.TResponseHeader.newBuilder().setRequestId(uid).build();
        assertEquals(new UUID(1L, 2L), ProtobufHelpers.getUuidFromHeader(responseHeader));
    }

    @Test
    public void validateHeader() throws Exception {
        List<List<Byte>> llist = new ArrayList<>();
        List<Byte> header = new ArrayList<>();
        header.addAll(Bytes.asList(ByteBuffer.allocate(Integer.BYTES).putInt(ByteBufUtil.swapInt(
                RpcMessageType.RESPONSE.getValue())).array()));
        llist.add(header);
        BusPackage busPackage = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, llist);
        Guid.TGuid uid = Guid.TGuid.newBuilder().setFirst(1L).setSecond(2L).build();
        Rpc.TResponseHeader responseHeader = Rpc.TResponseHeader.newBuilder().setRequestId(uid).build();
        assertTrue(ProtobufHelpers.validateHeader(busPackage, responseHeader));
    }

    @Test
    public void validateHeaderNotResp() throws Exception {
        List<List<Byte>> llist = new ArrayList<>();
        List<Byte> header = new ArrayList<>();
        header.addAll(Bytes.asList(ByteBuffer.allocate(Integer.BYTES).putInt(ByteBufUtil.swapInt(
                RpcMessageType.UNKNOWN.getValue())).array()));
        llist.add(header);
        BusPackage busPackage = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, llist);
        Guid.TGuid uid = Guid.TGuid.newBuilder().setFirst(1L).setSecond(2L).build();
        Rpc.TResponseHeader responseHeader = Rpc.TResponseHeader.newBuilder().setRequestId(uid).build();
        assertFalse(ProtobufHelpers.validateHeader(busPackage, responseHeader));
    }

    @Test
    public void validateHeaderPlusError() throws Exception {
        List<List<Byte>> llist = new ArrayList<>();
        List<Byte> header = new ArrayList<>();
        header.addAll(Bytes.asList(ByteBuffer.allocate(Integer.BYTES).putInt(ByteBufUtil.swapInt(
                RpcMessageType.UNKNOWN.getValue())).array()));
        llist.add(header);
        BusPackage busPackage = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, llist);
        Guid.TGuid uid = Guid.TGuid.newBuilder().setFirst(1L).setSecond(2L).build();
        Error.TError err = Error.TError.newBuilder().setCode(500).build();
        Rpc.TResponseHeader responseHeader = Rpc.TResponseHeader.newBuilder().setRequestId(uid).setError(err).build();
        assertFalse(ProtobufHelpers.validateHeader(busPackage, responseHeader));
    }


    @Test
    public void parseResponseHeader() throws Exception {
        Guid.TGuid uid = Guid.TGuid.newBuilder().setFirst(1L).setSecond(2L).build();
        protocol.Rpc.TResponseHeader header = protocol.Rpc.TResponseHeader.newBuilder().setRequestId(uid).build();
        List<Byte> res = new ArrayList<>();
        res.addAll(Bytes.asList(ByteBuffer.allocate(4).putInt(1).array()));
        res.addAll(Bytes.asList(header.toByteArray()));
        List<List<Byte>> llist = new ArrayList<>();
        llist.add(res);
        BusPackage busPackage = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, llist);
        assertEquals(header, ProtobufHelpers.parseResponseHeader(busPackage));
    }

    @Test
    public void getProtoFromRsp() throws Exception {
        protocol.Test.TestProto testProto = protocol.Test.TestProto.newBuilder().setId(1).setName("killboy").build();
        List<Byte> res = new ArrayList<>();
        res.addAll(Bytes.asList(ByteBuffer.allocate(4).putInt(1).array()));
        res.addAll(Bytes.asList(ByteBuffer.allocate(4).putInt(2).array()));
        res.addAll(Bytes.asList(testProto.toByteArray()));
        List<List<Byte>> llist = new ArrayList<>();
        llist.add(new ArrayList<>());
        llist.add(res);
        BusPackage busPackage = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, UUID.randomUUID(), false, llist);

        assertEquals(testProto, ProtobufHelpers.getProtoFromRsp(busPackage, protocol.Test.TestProto.parser(),
                protocol.Test.TestProto.class));
    }
}
