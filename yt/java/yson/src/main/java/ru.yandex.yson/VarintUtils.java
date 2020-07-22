package ru.yandex.yson;

class VarintUtils {
    static int MAX_VARINT32_SIZE = 5;
    static int MAX_VARINT64_SIZE = 10;

    static int encodeZigZag32(final int value) {
        return (value << 1) ^ (value >> 31);
    }

    static long encodeZigZag64(final long value) {
        return (value << 1) ^ (value >> 63);
    }
}
