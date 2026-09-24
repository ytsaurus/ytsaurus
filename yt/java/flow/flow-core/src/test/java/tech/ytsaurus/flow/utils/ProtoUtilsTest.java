package tech.ytsaurus.flow.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.test.TTestMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtoUtilsTest {

    @Test
    void guidFromProtoMatchesCanonicalTextForm() {
        // Canonical YT GUID text form is "Parts32[3]-[2]-[1]-[0]"; the proto TGuid carries
        // first = (Parts32[1] << 32) | Parts32[0] and second = (Parts32[3] << 32) | Parts32[2].
        var protoGuid = TGuid.newBuilder()
                .setFirst((3L << 32) | 4L)
                .setSecond((1L << 32) | 2L)
                .build();
        assertEquals(GUID.valueOf("1-2-3-4"), ProtoUtils.fromProto(protoGuid));
        assertEquals("1-2-3-4", ProtoUtils.fromProto(protoGuid).toString());
    }

    @Test
    void parseBytes() {
        var message = TTestMessage.newBuilder()
                .setId("1")
                .setTime(1714215000L)
                .setCount(1)
                .setValue(1.1)
                .setIsValue(true)
                .build();
        ByteString binMessage = message.toByteString();
        var parsedMessage = ProtoUtils.parseBytes(binMessage, TTestMessage.class);
        var parsedMessageByArray = ProtoUtils.parseBytes(binMessage.toByteArray(), TTestMessage.class);
        assertEquals(message, parsedMessage);
        assertEquals(message, parsedMessageByArray);
    }

    @Test
    void parseBytesWrapsMalformedInput() {
        // Tag 0x08 = field 1, varint wire type, but no value follows -> truncated message.
        byte[] malformed = {0x08};
        var fromArray = assertThrows(
                RuntimeException.class,
                () -> ProtoUtils.parseBytes(malformed, TTestMessage.class)
        );
        assertInstanceOf(InvalidProtocolBufferException.class, fromArray.getCause());

        var fromByteString = assertThrows(
                RuntimeException.class,
                () -> ProtoUtils.parseBytes(ByteString.copyFrom(malformed), TTestMessage.class)
        );
        assertInstanceOf(InvalidProtocolBufferException.class, fromByteString.getCause());
    }

    @Test
    void newBuilder() {
        var message = TTestMessage.newBuilder()
                .setId("1")
                .setTime(1714215000L)
                .setCount(1)
                .setValue(1.1)
                .setIsValue(true)
                .build();
        var message2 = ProtoUtils.<TTestMessage.Builder>newBuilder(TTestMessage.class)
                .setId("1")
                .setTime(1714215000L)
                .setCount(1)
                .setValue(1.1)
                .setIsValue(true)
                .build();
        assertEquals(message, message2);
    }
}
