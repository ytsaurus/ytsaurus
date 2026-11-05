package tech.ytsaurus.flow.row.codec;

import java.lang.reflect.Type;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.rows.YTreeSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteStringYsonCodecTest {
    @Test
    void decodesOnlyTheSelectedRangeAcrossBufferBoundaries() {
        String value = "Строка 🌍".repeat(1000);
        var codec = DefaultYsonCodec.INSTANCE;
        var wire = ByteString.copyFrom(codec.encode(value));
        byte[] padded = new byte[wire.size() + 31];
        wire.copyTo(padded, 0, 17, wire.size());
        var slice = ByteString.copyFrom(padded).substring(17, 17 + wire.size());
        var rope = wire.substring(0, 997).concat(wire.substring(997));
        assertTrue(rope.asReadOnlyByteBufferList().size() > 1);
        var serializer = codec.getOrCreateSerializer(String.class);
        for (var bytes : List.of(wire, slice, rope)) {
            assertEquals(value, codec.decode(bytes, String.class));
            assertEquals(value, codec.decodeWithSerializer(serializer, bytes, String.class));
            assertThrows(RuntimeException.class,
                    () -> codec.decode(bytes.substring(0, bytes.size() - 1), String.class));
        }
    }

    @Test
    void defaultByteStringMethodsDelegateToByteArrayMethods() {
        String value = "value";
        YsonCodec codec = new ByteArrayOnlyCodec();
        var wire = ByteString.copyFrom(codec.encode(value));
        byte[] padded = new byte[wire.size() + 11];
        wire.copyTo(padded, 0, 5, wire.size());
        var slice = ByteString.copyFrom(padded).substring(5, 5 + wire.size());
        var serializer = codec.getOrCreateSerializer(String.class);

        assertEquals(value, codec.decode(slice, String.class));
        assertEquals(value, codec.decodeWithSerializer(serializer, slice, String.class));
    }

    /**
     * A codec written before the ByteString overloads existed.
     */
    private static final class ByteArrayOnlyCodec implements YsonCodec {
        @Override
        public YTreeSerializer<?> getOrCreateSerializer(Type type) {
            return DefaultYsonCodec.INSTANCE.getOrCreateSerializer(type);
        }

        @Override
        public byte[] encodeWithSerializer(YTreeSerializer<?> serializer, Object value) {
            return DefaultYsonCodec.INSTANCE.encodeWithSerializer(serializer, value);
        }

        @Override
        public Object decodeWithSerializer(YTreeSerializer<?> serializer, byte[] bytes, Class<?> fieldType) {
            return DefaultYsonCodec.INSTANCE.decodeWithSerializer(serializer, bytes, fieldType);
        }

        @Override
        public byte[] encode(Object value) {
            return DefaultYsonCodec.INSTANCE.encode(value);
        }

        @Override
        public Object decode(byte[] bytes, Class<?> fieldType) {
            return DefaultYsonCodec.INSTANCE.decode(bytes, fieldType);
        }
    }
}
