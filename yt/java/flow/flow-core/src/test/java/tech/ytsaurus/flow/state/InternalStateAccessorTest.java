package tech.ytsaurus.flow.state;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.DefaultYsonCodec;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;
import tech.ytsaurus.flow.row.codec.YsonByteArrayCodec;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class InternalStateAccessorTest {

    private static final String STATE = "state";
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();
    private static final ByteArrayCodec<Long> YSON = new YsonByteArrayCodec<>(Long.class, DefaultYsonCodec.INSTANCE);

    /**
     * Visibly non-identity value codec: flips every byte, so a missing or one-sided application
     * shows up as garbage.
     */
    private static final InternalStateValueCodec FLIP = new InternalStateValueCodec() {
        @Override
        public byte[] decode(ByteString wire) {
            return flip(wire.toByteArray());
        }

        @Override
        public ByteString encode(byte[] value) {
            return ByteString.copyFrom(flip(value));
        }
    };

    private static byte[] flip(byte[] bytes) {
        byte[] out = bytes.clone();
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ~out[i];
        }
        return out;
    }

    private static Payload key() {
        return new PayloadBuilder(KEY_SCHEMA).set("k", "k1").finish();
    }

    private static InternalStateAccessor<Long> accessor(StatesHolder holder) {
        return new InternalStateAccessor<>(key(), StateDescriptors.yson(STATE, Long.class), holder, FLIP);
    }

    @Test
    void setEncodesWithTheValueCodecAfterTheByteCodec() {
        var holder = new StatesHolder(STATE, KEY_SCHEMA);
        accessor(holder).set(42L);
        assertEquals(ByteString.copyFrom(flip(YSON.encode(42L))), holder.get(key().getRow()).getBytes());
    }

    @Test
    void getDecodesWithTheValueCodecBeforeTheByteCodec() {
        var holder = new StatesHolder(STATE, KEY_SCHEMA);
        holder.load(key().getRow(), new State(ByteString.copyFrom(flip(YSON.encode(42L)))));
        assertEquals(42L, accessor(holder).get());
    }

    @Test
    void getAfterSetRoundTrips() {
        var holder = new StatesHolder(STATE, KEY_SCHEMA);
        var acc = accessor(holder);
        acc.set(42L);
        assertEquals(42L, acc.get());
    }

    @Test
    void getReturnsTheSameValueOnEveryCall() {
        // A value outside the Long cache, so two decodes would yield two objects.
        var holder = new StatesHolder(STATE, KEY_SCHEMA);
        holder.load(key().getRow(), new State(ByteString.copyFrom(flip(YSON.encode(1_000_000L)))));
        assertSame(accessor(holder).get(), accessor(holder).get());
    }
}
