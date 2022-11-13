package tech.ytsaurus.core;

public enum DataSizeUnit {
    BYTES(0, "", ""),
    KILOBYTES(10, "K", "KB"),
    MEGABYTES(20, "M", "MB"),
    GIGABYTES(30, "G", "GB"),
    TERABYTES(40, "T", "TB"),
    PETABYTES(50, "P", "PB"),
    EXABYTES(60, "E", "EB");

    private final long multiplier;
    private final int bits;
    private final String shortName;
    private final String name;

    DataSizeUnit(int bits, String shortName, String name) {
        this.bits = bits;
        this.multiplier = 1L << bits;
        this.shortName = shortName;
        this.name = name;
    }

    static {
        for (int i = 1; i < values().length; ++i) {
            DataSizeUnit prev = values()[i - 1];
            DataSizeUnit curr = values()[i];
            if (prev.getMultiplier() >= curr.getMultiplier()) {
                throw new AssertionError("must be sorted");
            }
        }
    }

    public int getBits() {
        return bits;
    }

    public long getMultiplier() {
        return multiplier;
    }

    public String getShortName() {
        return shortName;
    }

    public String getName() {
        return name;
    }

    public DataSize dataSize(long count) {
        return DataSize.fromBytes(count << bits);
    }

    public static DataSizeUnit parseShortName(String name0) {
        String name = name0.replaceFirst("[bB]$", "");
        for (DataSizeUnit u : values()) {
            if (u.shortName.equalsIgnoreCase(name)) {
                return u;
            }
        }
        throw new IllegalArgumentException("cannot parse '" + name0 + "' as DataSizeUnit");
    }

    public static DataSizeUnit largest() {
        return values()[values().length - 1];
    }

    public static DataSizeUnit unitFor(long count) {
        if (count == Long.MIN_VALUE) {
            return EXABYTES;
        }

        if (count < 0) {
            count = -count;
        }

        int bits = 64 - Long.numberOfLeadingZeros(count);
        DataSizeUnit result = BYTES;
        for (DataSizeUnit u : DataSizeUnit.values()) {
            if (u.bits >= bits) {
                break;
            }
            result = u;
        }
        return result;
    }

}
