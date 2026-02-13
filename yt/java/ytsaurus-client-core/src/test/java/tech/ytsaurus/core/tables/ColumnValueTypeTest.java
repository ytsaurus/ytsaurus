package tech.ytsaurus.core.tables;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnValueTypeTest {

    public static Stream<String> invalidNames() {
        return Stream.of("invalid", "", null, "1");
    }

    @ParameterizedTest
    @CsvSource({
            "0x00, MIN",
            "0x01, THE_BOTTOM",
            "0x02, NULL",
            "0x03, INT64",
            "0x04, UINT64",
            "0x05, DOUBLE",
            "0x06, BOOLEAN",
            "0x10, STRING",
            "0x11, ANY",
            "0x12, COMPOSITE",
            "0xef, MAX"
    })
    public void testFromValueValid(int value, ColumnValueType expected) {
        assertEquals(expected, ColumnValueType.fromValue(value));
    }

    @ParameterizedTest
    @ValueSource(ints = {-0x01, 0xF0, 0xFF, 0x100})
    public void testFromValueInvalid(int value) {
        assertThrows(IllegalArgumentException.class, () -> ColumnValueType.fromValue(value));
    }

    @ParameterizedTest
    @CsvSource({
            "min, MIN",
            "the_bottom, THE_BOTTOM",
            "null, NULL",
            "int64, INT64",
            "uint64, UINT64",
            "double, DOUBLE",
            "boolean, BOOLEAN",
            "string, STRING",
            "any, ANY",
            "composite, COMPOSITE",
            "max, MAX"
    })
    public void testFromNameValid(String name, ColumnValueType expected) {
        assertEquals(expected, ColumnValueType.fromName(name));
    }

    @ParameterizedTest
    @MethodSource("invalidNames")
    public void testFromNameInvalid(String name) {
        assertThrows(IllegalArgumentException.class, () -> ColumnValueType.fromName(name));
    }

    @ParameterizedTest
    @EnumSource(
            value = ColumnValueType.class,
            names = {"INT64", "UINT64", "DOUBLE", "BOOLEAN"}
    )
    public void testIsValueType(ColumnValueType type) {
        assertTrue(type.isValueType());
    }

    @ParameterizedTest
    @EnumSource(
            value = ColumnValueType.class,
            names = {"MIN", "THE_BOTTOM", "NULL", "STRING", "ANY", "COMPOSITE", "MAX"}
    )
    public void testIsNotValueType(ColumnValueType type) {
        assertFalse(type.isValueType());
    }

    @ParameterizedTest
    @EnumSource(
            value = ColumnValueType.class,
            names = {"STRING", "ANY", "COMPOSITE"}
    )
    public void testIsStringLikeType(ColumnValueType type) {
        assertTrue(type.isStringLikeType());
    }

    @ParameterizedTest
    @EnumSource(
            value = ColumnValueType.class,
            names = {"MIN", "THE_BOTTOM", "NULL", "INT64", "UINT64", "DOUBLE", "BOOLEAN", "MAX"}
    )
    public void testIsNotStringLikeType(ColumnValueType type) {
        assertFalse(type.isStringLikeType());
    }
}
