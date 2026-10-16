package tech.ytsaurus.flow.row.codec;

import java.util.List;
import java.util.Objects;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.utils.YsonUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link YsonByteArrayCodec} backed by {@link DefaultYsonCodec} round-trips both
 * {@code YTreeSerializerFactory}-supported values and {@code @Entity} classes through binary YSON.
 */
class YsonByteArrayCodecTest {

    @Entity
    static class State {
        String name;
        Long count;
        List<String> tags;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            State that = (State) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(count, that.count)
                    && Objects.equals(tags, that.tags);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, count, tags);
        }
    }

    private static State sample() {
        var state = new State();
        state.name = "flow";
        state.count = 42L;
        state.tags = List.of("a", "b");
        return state;
    }

    @Test
    void roundTripsEntity() {
        var codec = new YsonByteArrayCodec<>(State.class, DefaultYsonCodec.INSTANCE);
        State decoded = codec.decode(codec.encode(sample()));
        assertEquals(sample(), decoded);
    }

    @Test
    void roundTripsPrimitive() {
        var codec = new YsonByteArrayCodec<>(Long.class, DefaultYsonCodec.INSTANCE);
        assertEquals(42L, codec.decode(codec.encode(42L)));
    }

    @Test
    void encodesEntityAsBinaryYson() {
        var codec = new YsonByteArrayCodec<>(State.class, DefaultYsonCodec.INSTANCE);
        byte[] encoded = codec.encode(sample());
        // The encoded bytes are the binary YSON produced by EntityYsonSerializer.
        byte[] expected = new EntityYsonSerializer<>(State.class).serialize(sample());
        assertEquals(YsonUtils.yTreeFromBinary(expected), YsonUtils.yTreeFromBinary(encoded));
    }
}
