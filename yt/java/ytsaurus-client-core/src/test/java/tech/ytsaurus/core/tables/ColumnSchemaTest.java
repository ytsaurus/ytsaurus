package tech.ytsaurus.core.tables;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnSchemaTest {

    public static Stream<Arguments> supportedTypes() {
        return Stream.of(
                Arguments.of(ColumnValueType.NULL, ColumnValueType.NULL),
                Arguments.of(ColumnValueType.INT64, ColumnValueType.INT64),
                Arguments.of(ColumnValueType.UINT64, ColumnValueType.UINT64),
                Arguments.of(ColumnValueType.DOUBLE, ColumnValueType.DOUBLE),
                Arguments.of(ColumnValueType.BOOLEAN, ColumnValueType.BOOLEAN),
                Arguments.of(ColumnValueType.STRING, ColumnValueType.STRING),
                Arguments.of(ColumnValueType.ANY, ColumnValueType.ANY),
                // Not invertible type conversion!
                // It might be a bug, but some clients are dependent on it.
                Arguments.of(ColumnValueType.COMPOSITE, ColumnValueType.ANY)
        );
    }

    public static Stream<Arguments> unsupportedTypes() {
        return Stream.of(
                Arguments.of(ColumnValueType.MIN, ColumnValueType.NULL),
                Arguments.of(ColumnValueType.THE_BOTTOM, ColumnValueType.NULL),
                Arguments.of(ColumnValueType.MAX, ColumnValueType.NULL)
        );
    }

    @MethodSource("supportedTypes")
    @ParameterizedTest
    public void testPrecomputeTypes(ColumnValueType inputType, ColumnValueType expectedType) {
        assertEquals(
                expectedType,
                new ColumnSchema("name", inputType).getType()
        );
    }

    @MethodSource("unsupportedTypes")
    @ParameterizedTest
    public void testPrecomputeTypes(ColumnValueType unsupportedType) {
        assertThrows(IllegalStateException.class, () -> new ColumnSchema("name", unsupportedType));
    }

}
