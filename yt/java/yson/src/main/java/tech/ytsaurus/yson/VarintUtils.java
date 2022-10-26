package tech.ytsaurus.yson;

class VarintUtils {
    static final int MAX_VARINT32_SIZE = 5;
    static final int MAX_VARINT64_SIZE = 10;

    private VarintUtils() {
    }

    static int encodeZigZag32(final int value) {
        return (value << 1) ^ (value >> 31);
    }

    static long encodeZigZag64(final long value) {
        return (value << 1) ^ (value >> 63);
    }

    static long decodeZigZag64(final long value) {
        return (value >>> 1) ^ -(value & 1);
    }
}
