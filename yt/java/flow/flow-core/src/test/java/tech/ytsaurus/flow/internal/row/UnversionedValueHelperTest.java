package tech.ytsaurus.flow.internal.row;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link UnversionedValueHelper}, focused on UINT64 -> int narrowing
 * (unsigned-aware, unlike the signed {@link Math#toIntExact(long)} previously used).
 */
@DisplayName("UnversionedValueHelper Tests")
class UnversionedValueHelperTest {

    private static UnversionedValue uint64(long value) {
        return new UnversionedValue(0, ColumnValueType.UINT64, false, value);
    }

    private static UnversionedValue int64(long value) {
        return new UnversionedValue(0, ColumnValueType.INT64, false, value);
    }

    @Nested
    @DisplayName("uint64ToInt")
    class Uint64ToInt {

        @Test
        @DisplayName("Narrows a high-bit unsigned value (out of signed-int range) to its 32-bit pattern")
        void narrowsHighBitValue() {
            // 0x80000000 = 2_147_483_648: a valid unsigned value above Integer.MAX_VALUE.
            assertEquals(Integer.MIN_VALUE, UnversionedValueHelper.uint64ToInt(0x80000000L));
        }

        @Test
        @DisplayName("Narrows the maximum unsigned 32-bit value to -1")
        void narrowsMaxUnsigned32() {
            assertEquals(-1, UnversionedValueHelper.uint64ToInt(0xFFFFFFFFL));
        }

        @Test
        @DisplayName("Keeps small in-range values unchanged")
        void keepsSmallValues() {
            assertEquals(42, UnversionedValueHelper.uint64ToInt(42L));
            assertEquals(0, UnversionedValueHelper.uint64ToInt(0L));
        }

        @Test
        @DisplayName("Rejects a value that does not fit in unsigned 32 bits")
        void rejectsValueAboveUnsigned32() {
            assertThrows(ArithmeticException.class, () -> UnversionedValueHelper.uint64ToInt(0x100000000L));
        }

        @Test
        @DisplayName("Rejects a negative long (value above 2^63)")
        void rejectsNegativeLong() {
            assertThrows(ArithmeticException.class, () -> UnversionedValueHelper.uint64ToInt(-1_000_000_000_000L));
        }
    }

    @Nested
    @DisplayName("convertValueFrom UINT64 -> Integer")
    class ConvertUint64ToInteger {

        @Test
        @DisplayName("Decodes a high-bit UINT64 into an int field (regression for Math.toIntExact)")
        void decodesHighBitUint64IntoIntegerField() {
            assertEquals(Integer.MIN_VALUE,
                    UnversionedValueHelper.convertValueFrom(uint64(0x80000000L), Integer.class));
            assertEquals(Integer.MIN_VALUE,
                    UnversionedValueHelper.convertValueFrom(uint64(0x80000000L), int.class));
        }

        @Test
        @DisplayName("Decodes the maximum unsigned 32-bit UINT64 into an int field as -1")
        void decodesMaxUnsigned32IntoIntegerField() {
            assertEquals(-1, UnversionedValueHelper.convertValueFrom(uint64(0xFFFFFFFFL), Integer.class));
        }

        @Test
        @DisplayName("Rejects a UINT64 above the unsigned 32-bit range when narrowing to int")
        void rejectsUint64AboveUnsigned32() {
            assertThrows(
                    ArithmeticException.class,
                    () -> UnversionedValueHelper.convertValueFrom(uint64(0x100000000L), Integer.class)
            );
        }

        @Test
        @DisplayName("Keeps a high-bit UINT64 intact when target is a long field")
        void decodesUint64IntoLongField() {
            assertEquals(0x80000000L, UnversionedValueHelper.convertValueFrom(uint64(0x80000000L), Long.class));
        }
    }

    @Nested
    @DisplayName("convertValueFrom INT64 -> Integer (unchanged, signed)")
    class ConvertInt64ToInteger {

        @Test
        @DisplayName("Decodes a negative INT64 into an int field")
        void decodesNegativeInt64IntoIntegerField() {
            assertEquals(-42, UnversionedValueHelper.convertValueFrom(int64(-42L), Integer.class));
        }

        @Test
        @DisplayName("Still rejects an INT64 outside signed-int range")
        void rejectsInt64OutOfIntRange() {
            assertThrows(
                    ArithmeticException.class,
                    () -> UnversionedValueHelper.convertValueFrom(int64(Long.MAX_VALUE), Integer.class)
            );
        }
    }
}
