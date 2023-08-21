package tech.ytsaurus.core;

public class DataSize {
    public static final DataSize ZERO = new DataSize(0);
    public static final DataSize MEGABYTE = fromMegaBytes(1);

    private final long bytes;

    private DataSize(long bytes) {
        this.bytes = bytes;
    }

    public static DataSize fromBytes(long bytes) {
        return new DataSize(bytes);
    }

    public static DataSize fromKiloBytes(long kiloBytes) {
        return DataSizeUnit.KILOBYTES.dataSize(kiloBytes);
    }

    public static DataSize fromMegaBytes(long megaBytes) {
        return DataSizeUnit.MEGABYTES.dataSize(megaBytes);
    }

    public static DataSize fromGigaBytes(long gigaBytes) {
        return DataSizeUnit.GIGABYTES.dataSize(gigaBytes);
    }

    public static DataSize fromTeraBytes(long teraBytes) {
        return DataSizeUnit.TERABYTES.dataSize(teraBytes);
    }

    public static DataSize fromPetaBytes(long petaBytes) {
        return DataSizeUnit.PETABYTES.dataSize(petaBytes);
    }

    public static DataSize fromExaBytes(long exaBytes) {
        if (exaBytes > 7L || exaBytes <= -8L) {
            throw new IllegalArgumentException("Invalid exaBytes");
        }
        return DataSizeUnit.EXABYTES.dataSize(exaBytes);
    }

    public long toBytes() {
        return bytes;
    }

    public long toKiloBytes() {
        return to(DataSizeUnit.KILOBYTES);
    }

    public long toMegaBytes() {
        return to(DataSizeUnit.MEGABYTES);
    }

    public long toGigaBytes() {
        return to(DataSizeUnit.GIGABYTES);
    }

    public long toTeraBytes() {
        return to(DataSizeUnit.TERABYTES);
    }

    public long toPetaBytes() {
        return to(DataSizeUnit.PETABYTES);
    }

    public long toExaBytes() {
        return to(DataSizeUnit.EXABYTES);
    }

    public long to(DataSizeUnit u) {
        return to(bytes, u);
    }

    public static long to(long bytes, DataSizeUnit u) {
        if (bytes == Long.MIN_VALUE) {
            return bytes >> u.getBits();
        }
        long absResult = (bytes < 0 ? -bytes : bytes) >> u.getBits();
        return bytes < 0 ? -absResult : absResult;
    }

    public String toString(DataSizeUnit unit) {
        return string(bytes, unit);
    }

    @Override
    public String toString() {
        return String.valueOf(toBytes());
    }

    public String toStringShort() {
        return shortString(bytes);
    }

    public static String shortString(long bytes) {
        return shortString(bytes, DataSizeUnit.unitFor(bytes));
    }

    public static String shortString(long bytes, DataSizeUnit unit) {
        return bytes == 0 ? "0" : string(bytes, unit);
    }

    private static String string(long bytes, DataSizeUnit unit) {
        return to(bytes, unit) + unit.getShortName();
    }

    /**
     * Convert to human-readable form. It may contain fractional part of the unit and uses full unit name,
     * for example: 56 bytes, 1 KB, 4.9 MB, 1.4 GB
     */
    public String toPrettyString() {
        return prettyString(bytes);
    }

    public static String prettyString(long bytes) {
        return prettyString(bytes, 10);
    }

    public String toPrettyString(int roundingThreshold) {
        return prettyString(bytes, roundingThreshold);
    }

    public static String prettyString(long bytes, int roundingThreshold) {
        if (bytes == Long.MIN_VALUE) {
            return prettyStringInternal(bytes, roundingThreshold);
        }
        return bytes < 0
                ? '-' + prettyStringInternal(-bytes, roundingThreshold)
                : prettyStringInternal(bytes, roundingThreshold);
    }

    private static String prettyStringInternal(long bytes, int roundingThreshold) {
        if (bytes == 0) {
            return "0";
        }

        DataSizeUnit unit = DataSizeUnit.unitFor(bytes);
        if (unit == DataSizeUnit.BYTES) {
            return Long.toString(bytes) + " bytes";
        }

        long sizeInUnits = to(bytes, unit);
        long diff = bytes - (sizeInUnits << unit.getBits());
        long tenths = (10 * diff) >> unit.getBits();
        long tenthMultiplier = unit.getMultiplier() / 10;
        if (diff - tenths * tenthMultiplier > (tenthMultiplier >> 1)) { // not exactly precise
            ++tenths;
            if (tenths == 10) {
                tenths = 0;
                ++sizeInUnits;
            }
        }

        if (tenths > 0 && sizeInUnits < roundingThreshold) {
            return sizeInUnits + "." + tenths + " " + unit.getName();
        } else {
            return (tenths < 5 ? sizeInUnits : sizeInUnits + 1) + " " + unit.getName();
        }
    }

    public String toStringKiloBytes() {
        return toKiloBytes() + DataSizeUnit.KILOBYTES.getShortName();
    }

    public String toStringMegaBytes() {
        return toMegaBytes() + DataSizeUnit.MEGABYTES.getShortName();
    }

    public String toStringGigaBytes() {
        return toGigaBytes() + DataSizeUnit.GIGABYTES.getShortName();
    }

    public String toStringTeraBytes() {
        return toTeraBytes() + DataSizeUnit.TERABYTES.getShortName();
    }

    public String toStringPetaBytes() {
        return toPetaBytes() + DataSizeUnit.PETABYTES.getShortName();
    }

    public String toStringExaBytes() {
        return toExaBytes() + DataSizeUnit.EXABYTES.getShortName();
    }

    public DataSize max(DataSize that) {
        if (bytes > that.bytes) {
            return this;
        }
        return that;
    }

    public DataSize min(DataSize that) {
        if (bytes > that.bytes) {
            return that;
        }
        return this;
    }

    public DataSize div(float d) {
        return fromBytes((long) (bytes / d));
    }

    public DataSize mul(float d) {
        return fromBytes((long) (bytes * d));
    }

    public DataSize plus(DataSize that) {
        return DataSize.fromBytes(toBytes() + that.toBytes());
    }

    public DataSize minus(DataSize that) {
        return DataSize.fromBytes(toBytes() - that.toBytes());
    }
}
