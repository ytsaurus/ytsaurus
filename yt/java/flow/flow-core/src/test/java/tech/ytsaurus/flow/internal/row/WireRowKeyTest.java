package tech.ytsaurus.flow.internal.row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WireRowKeyTest {
    @Test
    void usesOnlyDeclaredFullHashes() {
        assertEquals(WireRowKey.HashMode.LEADING_UINT64,
                WireRowKey.hashModeFor(keySchema("farm_hash(data)", "data")));
        for (String expression : List.of(" farm_hash( business_id , shop_sku ) ",
                "farm_hash(shop_sku,business_id)")) {
            assertEquals(WireRowKey.HashMode.LEADING_UINT64,
                    WireRowKey.hashModeFor(keySchema(expression, "business_id", "shop_sku")));
        }
        for (String expression : List.of("farm_hash(data) % 16", "farm_hash(data) % abs(partition_count)",
                "data", "farm_hash(data) + 1", "0")) {
            assertEquals(WireRowKey.HashMode.CONTENT, WireRowKey.hashModeFor(keySchema(expression)));
            assertEquals(WireRowKey.HashMode.CONTENT, WireRowKey.hashModeFor(keySchema(expression, "data")));
        }
        assertEquals(WireRowKey.HashMode.CONTENT, WireRowKey.hashModeFor(null));
        assertEquals(WireRowKey.HashMode.CONTENT, WireRowKey.hashModeFor(TableSchema.builder().build()));
        assertEquals(WireRowKey.HashMode.CONTENT,
                WireRowKey.hashModeFor(TableSchema.builder().addValue("hash", ColumnValueType.UINT64).build()));
    }

    @Test
    void usesContentHashingWhenHashCoversOnlyPartOfKey() {
        // The group_by_schema of kafka_wait_click_join: keys that share hit_id share the hash column.
        var schema = keySchema("farm_hash(hit_id)", "hit_id", "hit_time");
        var mode = WireRowKey.hashModeFor(schema);
        assertEquals(WireRowKey.HashMode.CONTENT, mode);
        assertEquals(WireRowKey.HashMode.CONTENT,
                WireRowKey.hashModeFor(keySchema("farm_hash(business_id)", "business_id", "shop_sku")));
        assertEquals(WireRowKey.HashMode.CONTENT,
                WireRowKey.hashModeFor(keySchema("farm_hash(other)", "data")));

        var hashes = new HashSet<Integer>();
        var leadingHashes = new HashSet<Integer>();
        for (long hitTime = 0; hitTime < 1000; hitTime++) {
            var writer = new ProtoWireRowWriter(3);
            writer.writeUint64Bits(0, 0x9e3779b97f4a7c15L);
            writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("hit-id"));
            writer.writeUint64Bits(2, hitTime);
            var wire = writer.finish();
            hashes.add(WireRowKey.of(wire, mode).hashCode());
            leadingHashes.add(WireRowKey.of(wire, WireRowKey.HashMode.LEADING_UINT64).hashCode());
        }
        assertEquals(1, leadingHashes.size());
        assertTrue(hashes.size() > 990, "distinct hashes: " + hashes.size());
    }

    @Test
    void hashesAndComparesIndependentOfBufferBoundaries() {
        byte[] payload = new byte[16384];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31);
        }
        var wire = key(Long.MIN_VALUE, payload);
        var variants = List.of(wire, slice(wire), rope(wire, 97), rope(wire, 1307), slicedRope(wire, 4099));
        for (var mode : WireRowKey.HashMode.values()) {
            var expected = WireRowKey.of(wire, mode);
            for (var a : variants) {
                var left = WireRowKey.of(a, mode);
                assertSame(a, left.wireBytes());
                assertEquals(expected.hashCode(), left.hashCode());
                for (var b : variants) {
                    var right = WireRowKey.of(b, mode);
                    for (int repeat = 0; repeat < 3; repeat++) {
                        assertEquals(left, right);
                        assertEquals(right, left);
                    }
                }
            }
        }
    }

    @Test
    void comparesCompleteBytesDespiteHashCollisions() {
        byte[] payload = new byte[8192];
        var original = key(17, payload);
        var expected = WireRowKey.of(original, WireRowKey.HashMode.LEADING_UINT64);
        var map = new HashMap<WireRowKey, Integer>();
        map.put(expected, 0);
        int number = 1;
        for (int position : new int[] {0, 90, 1024, 8191}) {
            byte[] changed = payload.clone();
            changed[position] = 1;
            var wire = key(17, changed);
            var actual = WireRowKey.of(rope(wire, 117), WireRowKey.HashMode.LEADING_UINT64);
            assertEquals(expected.hashCode(), actual.hashCode());
            assertNotEquals(expected, actual);
            assertNotEquals(actual, expected);
            map.put(actual, number++);
        }
        assertEquals(5, map.size());
        assertEquals(0, map.get(WireRowKey.of(rope(original, 891), WireRowKey.HashMode.LEADING_UINT64)));
    }

    @Test
    void supportsEmptyAndShortContentKeysAroundTheBufferThreshold() {
        // Keys up to 64 bytes compare ByteStrings; longer keys compare ByteBuffers.
        for (int length = 0; length <= 70; length++) {
            byte[] data = new byte[length];
            Arrays.fill(data, (byte) length);
            var wire = ByteString.copyFrom(data);
            assertEquals(WireRowKey.of(wire, WireRowKey.HashMode.CONTENT),
                    WireRowKey.of(slice(wire), WireRowKey.HashMode.CONTENT));
        }
    }

    @Test
    void distinctModesDoNotViolateHashEqualityContract() {
        var wire = key(1, new byte[] {2, 3});
        assertNotEquals(WireRowKey.of(wire, WireRowKey.HashMode.CONTENT),
                WireRowKey.of(wire, WireRowKey.HashMode.LEADING_UINT64));
    }

    @Test
    void hashesNullLeadingColumnWithoutAssumingItContainsANumber() {
        var wire = ByteString.copyFrom(new byte[] {0, 1, 0, 2});
        var key = WireRowKey.of(wire, WireRowKey.HashMode.LEADING_UINT64);
        assertEquals(key, WireRowKey.of(slice(wire), WireRowKey.HashMode.LEADING_UINT64));
    }

    @Test
    void validatesLeadingHashAndAcceptsNoncanonicalVarints() {
        for (int size = 1; size <= 10; size++) {
            byte[] data = new byte[4 + size];
            data[1] = 1;
            data[3] = (byte) ColumnValueType.UINT64.getValue();
            Arrays.fill(data, 4, data.length - 1, (byte) 0x80);
            assertEquals(Long.hashCode(0),
                    WireRowKey.of(ByteString.copyFrom(data), WireRowKey.HashMode.LEADING_UINT64).hashCode());
            data[data.length - 1] = (byte) 0x80;
            assertThrows(IllegalArgumentException.class,
                    () -> WireRowKey.of(ByteString.copyFrom(data), WireRowKey.HashMode.LEADING_UINT64));
        }
        assertInvalidKey(InvalidRowException.class, "does not support versions", new byte[] {1, 1, 0, 4, 1});
        assertInvalidKey(InvalidRowException.class, "Key has no leading hash column", new byte[] {0, 0});
        assertInvalidKey(InvalidRowException.class, "Leading hash column must have id zero",
                new byte[] {0, 1, 1, 4, 1});
        assertInvalidKey(MalformedRowException.class, "truncated value", new byte[] {0, 1, 0, 4, (byte) 0x80});
        byte[] overflow = new byte[14];
        overflow[1] = 1;
        overflow[3] = (byte) ColumnValueType.UINT64.getValue();
        Arrays.fill(overflow, 4, 13, (byte) 0xff);
        overflow[13] = 2;
        assertThrows(IllegalArgumentException.class,
                () -> WireRowKey.of(ByteString.copyFrom(overflow), WireRowKey.HashMode.LEADING_UINT64));
    }

    private static void assertInvalidKey(Class<? extends RuntimeException> type, String message, byte[] data) {
        var error = assertThrows(type,
                () -> WireRowKey.of(ByteString.copyFrom(data), WireRowKey.HashMode.LEADING_UINT64));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    /**
     * Returns a key schema whose first column is a UINT64 computed by {@code expression}.
     */
    private static TableSchema keySchema(String expression, String... otherColumns) {
        var builder = TableSchema.builder().add(ColumnSchema.builder("hash", ColumnValueType.UINT64)
                .setExpression(expression).build());
        for (String name : otherColumns) {
            builder.addValue(name, ColumnValueType.STRING);
        }
        return builder.build();
    }

    private static ByteString key(long hash, byte[] payload) {
        var writer = new ProtoWireRowWriter(2);
        writer.writeUint64Bits(0, hash);
        writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFrom(payload));
        return writer.finish();
    }

    /**
     * Returns a flat copy of {@code wire} whose buffer starts at a non-zero array offset.
     */
    private static ByteString slice(ByteString wire) {
        byte[] padded = new byte[wire.size() + 17];
        wire.copyTo(padded, 17);
        var result = ByteString.copyFrom(padded).substring(17);
        if (!wire.isEmpty()) {
            assertEquals(1, result.asReadOnlyByteBufferList().size());
            assertEquals(17, result.asReadOnlyByteBuffer().position());
        }
        return result;
    }

    /**
     * Returns a rope whose two leaves both start at non-zero array offsets.
     */
    private static ByteString slicedRope(ByteString wire, int split) {
        var result = slice(wire.substring(0, split)).concat(slice(wire.substring(split)));
        var buffers = result.asReadOnlyByteBufferList();
        assertEquals(2, buffers.size());
        assertTrue(buffers.get(0).position() != 0 && buffers.get(1).position() != 0);
        return result;
    }

    private static ByteString rope(ByteString wire, int split) {
        var result = ByteString.copyFrom(wire.substring(0, split).toByteArray())
                .concat(ByteString.copyFrom(wire.substring(split).toByteArray()));
        assertTrue(result.asReadOnlyByteBufferList().size() > 1);
        return result;
    }
}
