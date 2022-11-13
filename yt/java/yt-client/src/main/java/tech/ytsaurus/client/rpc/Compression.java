package tech.ytsaurus.client.rpc;

import java.util.HashMap;
import java.util.Map;

public enum Compression {
    None(0),

    Lz4(4),
    Lz4HighCompression(5),

    Zlib_1(19),
    Zlib_2(20),
    Zlib_3(21),
    Zlib_4(22),
    Zlib_5(23),
    Zlib_6(2),
    Zlib_7(24),
    Zlib_8(25),
    Zlib_9(3);

    private static final Map<Integer, Compression> INDEX = new HashMap<>();

    private final int value;

    Compression(int value) {
        this.value = value;
    }

    public static Compression fromValue(int value) {
        Compression compression = INDEX.get(value);
        if (compression == null) {
            throw new IllegalArgumentException("Unsupported compression " + value);
        }
        return compression;
    }

    public int getValue() {
        return value;
    }

    static {
        for (Compression entity : Compression.values()) {
            INDEX.put(entity.getValue(), entity);
        }
    }
}
