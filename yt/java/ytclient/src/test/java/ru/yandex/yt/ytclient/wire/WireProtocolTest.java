package ru.yandex.yt.ytclient.wire;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import ru.yandex.yt.ytclient.tables.ColumnValueType;

/**
 * Общие утилиты для тестов wire протокола
 * <p>
 * Основано на данных из yt/unittests/wire_protocol_ut.cpp
 */
public class WireProtocolTest {
    private static final ColumnValueType[] VALUE_TYPES = new ColumnValueType[]{
            ColumnValueType.NULL,
            ColumnValueType.INT64,
            ColumnValueType.UINT64,
            ColumnValueType.DOUBLE,
            ColumnValueType.BOOLEAN,
            ColumnValueType.STRING,
            ColumnValueType.ANY,
    };

    private static final boolean[] AGGREGATE_TYPES = new boolean[]{true, false};

    public static byte[] makeByteArray(int... bytes) {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }

    public static byte[] mergeChunks(List<byte[]> chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] result = new byte[length];
        int index = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, index, chunk.length);
            index += chunk.length;
        }
        return result;
    }

    public static UnversionedValue makeValueSample(int id, ColumnValueType type, boolean aggregate) {
        Object value;
        switch (type) {
            case INT64:
                value = Long.parseLong("0123456789ABCDEF", 16);
                break;
            case UINT64:
                value = Long.parseUnsignedLong("FEDCBA9876543210", 16);
                break;
            case DOUBLE:
                value = 3.141592653589793;
                break;
            case BOOLEAN:
                value = false;
                break;
            case STRING:
                value = new byte[]{'s'};
                break;
            case ANY:
                value = new byte[]{'{', '}'};
                break;
            default:
                value = null;
                break;
        }
        return new UnversionedValue(id, type, aggregate, value);
    }

    public static UnversionedRow makeUnversionedRowSample() {
        List<UnversionedValue> values = new ArrayList<>();
        int id = 0;
        for (ColumnValueType type : VALUE_TYPES) {
            for (boolean aggregate : AGGREGATE_TYPES) {
                values.add(makeValueSample(id, type, aggregate));
            }
        }
        return new UnversionedRow(values);
    }

    public static byte[] makeUnversionedRowCanonicalBlob() {
        return makeByteArray(
                // value count
                0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // one value per row
                0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x00, 0x00, 0x05, 0x01, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x00, 0x00, 0x06, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x10, 0x01, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x00, 0x00, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x00, 0x00, 0x11, 0x01, 0x02, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x00, 0x00, 0x11, 0x00, 0x02, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf);
    }

    public static UnversionedRow makeSchemafulRowSample() {
        List<UnversionedValue> values = new ArrayList<>();
        int id = 0;
        for (ColumnValueType type : VALUE_TYPES) {
            // no aggregates
            values.add(makeValueSample(id, type, false));
        }
        return new UnversionedRow(values);
    }

    public static byte[] makeSchemafulRowCanonicalBlob() {
        return makeByteArray(
                // value count
                0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // null bitmap
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // one value per row
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x7d, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf);
    }

    public static List<WireColumnSchema> extractSchemaData(UnversionedRow row, ColumnValueType nullType) {
        List<WireColumnSchema> result = new ArrayList<>();
        for (UnversionedValue value : row.getValues()) {
            result.add(new WireColumnSchema(
                    value.getId(),
                    value.getType() != ColumnValueType.NULL ? value.getType() : nullType,
                    value.isAggregate()));
        }
        return result;
    }

    private static final char[] HEX_NIBBLES = "0123456789abcdef".toCharArray();

    public static String hexString(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (int i = 0; i < data.length; i++) {
            int value = data[i] & 0xff;
            sb.append(HEX_NIBBLES[value >> 4]);
            sb.append(HEX_NIBBLES[value & 15]);
        }
        return sb.toString();
    }

    public static Matcher<byte[]> dataEquals(byte[] canonical) {
        return new TypeSafeMatcher<byte[]>() {
            @Override
            protected boolean matchesSafely(byte[] item) {
                if (item.length != canonical.length) {
                    return false;
                }
                for (int i = 0; i < canonical.length; i++) {
                    // 0xcf marks garbage due to alignment
                    if (canonical[i] != (byte) 0xcf) {
                        if (item[i] != canonical[i]) {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is ").appendText(hexString(canonical));
            }

            @Override
            protected void describeMismatchSafely(byte[] item, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendText(hexString(item));
            }
        };
    }
}
