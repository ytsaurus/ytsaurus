package tech.ytsaurus.flow.internal.row;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.persistence.Column;
import javax.persistence.Entity;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.flow.test.TTestMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for TypeAwareProtoWireProtocolReader and TypeAwareProtoWireProtocolWriter.
 * Tests are organized by functionality and data type for better maintainability.
 */
@DisplayName("TypeAwareProtoWireProtocol Reader/Writer Tests")
class TypeAwareProtoWireProtocolReaderWriterTest {

    private <T, V> void testRoundTrip(Class<T> entityClass, BiConsumer<T, V> setter,
                                      Function<T, V> getter, V value) {
        var writer = new TypeAwareProtoWireProtocolWriter<>(entityClass);
        var reader = new TypeAwareProtoWireProtocolReader<>(entityClass);

        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            setter.accept(entity, value);

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertEquals(value, getter.apply(deserialized));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generic round-trip test helper with custom assertion
     */
    private <T, V> void testRoundTripWithAssertion(Class<T> entityClass, BiConsumer<T, V> setter,
                                                   Function<T, V> getter, V value,
                                                   BiConsumer<V, V> assertion) {
        var writer = new TypeAwareProtoWireProtocolWriter<>(entityClass);
        var reader = new TypeAwareProtoWireProtocolReader<>(entityClass);

        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            setter.accept(entity, value);

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertion.accept(value, getter.apply(deserialized));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generic null value test helper
     */
    private <T> void testNullValue(Class<T> entityClass, BiConsumer<T, Object> setter,
                                   Function<T, Object> getter) {
        var writer = new TypeAwareProtoWireProtocolWriter<>(entityClass);
        var reader = new TypeAwareProtoWireProtocolReader<>(entityClass);

        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            setter.accept(entity, null);

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertNull(getter.apply(deserialized));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Entity
    static class OneStringField {
        private String datum;
    }

    @Entity
    static class OneLongField {
        private Long datum;
    }

    @Entity
    static class OneDoubleField {
        private Double datum;
    }

    @Entity
    static class OneBooleanField {
        private Boolean datum;
    }

    @Entity
    static class OneIntegerField {
        private Integer datum;
    }

    @Entity
    static class OneUint64Field {
        @Column(columnDefinition = "uint64")
        private Long datum;
    }

    @Entity
    static class TwoFields {
        private String stringDatum;
        @Column(columnDefinition = "uint64")
        private Long longDatum;
    }

    @Entity
    static class MultipleTypesEntity {
        private String stringField;
        private Long longField;
        private Double doubleField;
        private Boolean booleanField;
    }

    @Entity
    static class ByteArrayField {
        private byte[] data;
    }

    // Primitive type fields
    @Entity
    static class PrimitiveIntField {
        private int datum;
    }

    @Entity
    static class PrimitiveLongField {
        private long datum;
    }

    @Entity
    static class PrimitiveDoubleField {
        private double datum;
    }

    @Entity
    static class PrimitiveBooleanField {
        private boolean datum;
    }

    @Entity
    static class PrimitiveShortField {
        private short datum;
    }

    @Entity
    static class PrimitiveByteField {
        private byte datum;
    }

    @Entity
    static class MapNonStringKey {
        private Map<Long, String> data;
    }

    @Entity
    static class ProtobufEntity {
        private TTestMessage datum;
    }

    @Nested
    @DisplayName("String Like Type Tests")
    class StringTypeTests {

        @Test
        @DisplayName("Should read and write byte array")
        void shouldReadWriteByteArray() {
            testRoundTripWithAssertion(ByteArrayField.class,
                    (e, v) -> e.data = v,
                    e -> e.data,
                    new byte[]{0x01, 0x02, 0x03},
                    Assertions::assertArrayEquals);
        }

        @ParameterizedTest
        @ValueSource(strings = {"test", "", "Hello 世界 🌍", "Line1\nLine2\tTabbed\r\nWindows"})
        @DisplayName("Should handle various string values")
        void shouldHandleVariousStringValues(String value) {
            testRoundTrip(OneStringField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @Test
        @DisplayName("Should handle long string (100 characters)")
        void shouldHandleLongString() {
            testRoundTrip(OneStringField.class, (e, v) -> e.datum = v, e -> e.datum, "a".repeat(100));
        }

        @Test
        @DisplayName("Should read string from predefined byte array")
        void shouldReadStringFromByteArray() {
            byte[] chunkArray = {
                    0x00, // version
                    0x01, // number of values in row
                    0x00, // value id
                    0x10, // ColumnValueType.STRING
                    0x02, // 2 bytes length
                    0x74, // t
                    0x70  // p
            };
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneStringField.class);
            var entity = reader.readChunk(chunk);

            assertEquals("tp", entity.datum);
        }

        @Test
        @DisplayName("Should write string to expected byte array")
        void shouldWriteStringToByteArray() {
            byte[] expectedArr = {
                    0x00, // version
                    0x01, // number of values in row
                    0x00, // value id
                    0x10, // ColumnValueType.STRING
                    0x02, // 2 bytes length
                    0x74, // t
                    0x70  // p
            };
            var expected = ByteString.copyFrom(expectedArr);
            var writer = new TypeAwareProtoWireProtocolWriter<>(OneStringField.class);
            var entity = new OneStringField();
            entity.datum = "tp";

            assertEquals(expected, writer.writeEntity(entity));
        }
    }

    @Nested
    @DisplayName("INT64 Type Tests")
    class Int64TypeTests {

        @ParameterizedTest
        @ValueSource(longs = {0L, 1L, -1L, 42L, -42L, Long.MAX_VALUE, Long.MIN_VALUE})
        @DisplayName("Should handle various INT64 values")
        void shouldHandleVariousInt64Values(long value) {
            testRoundTrip(OneLongField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @Test
        @DisplayName("Should read positive INT64 from byte array")
        void shouldReadPositiveInt64() {
            byte[] chunkArray = {0x00, 0x01, 0x00, 0x03, 0x06};
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneLongField.class);
            assertEquals(3L, reader.readChunk(chunk).datum);
        }

        @Test
        @DisplayName("Should read negative INT64 from byte array")
        void shouldReadNegativeInt64() {
            byte[] chunkArray = {0x00, 0x01, 0x00, 0x03, 0x01};
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneLongField.class);
            assertEquals(-1L, reader.readChunk(chunk).datum);
        }

        @Test
        @DisplayName("Should convert INT64 to Integer field")
        void shouldConvertInt64ToInteger() {
            byte[] chunkArray = {0x00, 0x01, 0x00, 0x03, 0x54};
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneIntegerField.class);
            assertEquals(42, reader.readChunk(chunk).datum);
        }
    }

    @Nested
    @DisplayName("UINT64 Type Tests")
    class Uint64TypeTests {

        @ParameterizedTest
        @ValueSource(longs = {0L, 1L, 100L, 1000L, Long.MAX_VALUE})
        @DisplayName("Should handle various UINT64 values")
        void shouldHandleVariousUint64Values(long value) {
            testRoundTrip(OneUint64Field.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @Test
        @DisplayName("Should write UINT64 to expected byte array")
        void shouldWriteUint64ToByteArray() {
            byte[] expectedArray = {0x00, 0x01, 0x00, 0x04, 0x64};
            var expected = ByteString.copyFrom(expectedArray);
            var writer = new TypeAwareProtoWireProtocolWriter<>(OneUint64Field.class);
            var entity = new OneUint64Field();
            entity.datum = 100L;
            assertEquals(expected, writer.writeEntity(entity));
        }

        @Test
        @DisplayName("Should round-trip UINT64 value with high bit set")
        void shouldRoundTripUint64WithHighBitSet() {
            // -1L is the unsigned value 0xFFFFFFFFFFFFFFFF.
            testRoundTrip(OneUint64Field.class, (e, v) -> e.datum = v, e -> e.datum, -1L);
        }
    }

    @Nested
    @DisplayName("Double Type Tests")
    class DoubleTypeTests {

        static Stream<Arguments> provideDoubleValues() {
            return Stream.of(
                    Arguments.of(1.0), Arguments.of(-1.0), Arguments.of(3.14159), Arguments.of(-3.14159),
                    Arguments.of(Double.MAX_VALUE), Arguments.of(Double.NaN),
                    Arguments.of(Double.POSITIVE_INFINITY), Arguments.of(Double.NEGATIVE_INFINITY)
            );
        }

        @ParameterizedTest
        @MethodSource("provideDoubleValues")
        @DisplayName("Should handle various double values")
        void shouldHandleVariousDoubleValues(double value) {
            testRoundTripWithAssertion(OneDoubleField.class,
                    (e, v) -> e.datum = v,
                    e -> e.datum,
                    value,
                    (expected, actual) -> {
                        if (Double.isNaN(expected)) {
                            assertTrue(Double.isNaN(actual));
                        } else {
                            assertEquals(expected, actual, 0.0001);
                        }
                    });
        }

        @Test
        @DisplayName("Should write DOUBLE as fixed 8-byte little-endian value")
        void shouldWriteDoubleAsFixed64() {
            // 1.0 has raw bits 0x3FF0000000000000; the C++ counterpart stores them
            // as fixed 8 little-endian bytes, not as a varint.
            byte[] expectedArray = {
                    0x00, 0x01, 0x00, 0x05,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF0, 0x3F
            };
            var expected = ByteString.copyFrom(expectedArray);
            var writer = new TypeAwareProtoWireProtocolWriter<>(OneDoubleField.class);
            var entity = new OneDoubleField();
            entity.datum = 1.0;
            assertEquals(expected, writer.writeEntity(entity));
        }

        @Test
        @DisplayName("Should read DOUBLE from fixed 8-byte little-endian value")
        void shouldReadDoubleFromFixed64() {
            byte[] chunkArray = {
                    0x00, 0x01, 0x00, 0x05,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF0, 0x3F
            };
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneDoubleField.class);
            assertEquals(1.0, reader.readChunk(chunk).datum);
        }
    }

    @Nested
    @DisplayName("Boolean Type Tests")
    class BooleanTypeTests {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("Should handle boolean values")
        void shouldHandleBooleanValues(boolean value) {
            testRoundTrip(OneBooleanField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("Should read boolean from byte array")
        void shouldReadBooleanFromByteArray(boolean value) {
            byte[] chunkArray = {
                    0x00, // version
                    0x01, // number of values
                    0x00, // value id
                    0x06, // ColumnValueType.BOOLEAN
                    (byte) (value ? 0x01 : 0x00)  // true or false
            };
            var chunk = ByteString.copyFrom(chunkArray);
            var reader = new TypeAwareProtoWireProtocolReader<>(OneBooleanField.class);
            assertEquals(value, reader.readChunk(chunk).datum);
        }
    }

    @Nested
    @DisplayName("NULL Value Tests")
    class NullValueTests {

        @Test
        @DisplayName("Should handle null string field")
        void shouldHandleNullStringField() {
            testNullValue(OneStringField.class, (e, v) -> e.datum = (String) v, e -> e.datum);
        }

        @Test
        @DisplayName("Should handle null Long field")
        void shouldHandleNullLongField() {
            testNullValue(OneLongField.class, (e, v) -> e.datum = (Long) v, e -> e.datum);
        }

        @Test
        @DisplayName("Should handle null Double field")
        void shouldHandleNullDoubleField() {
            testNullValue(OneDoubleField.class, (e, v) -> e.datum = (Double) v, e -> e.datum);
        }

        @Test
        @DisplayName("Should handle null Boolean field")
        void shouldHandleNullBooleanField() {
            testNullValue(OneBooleanField.class, (e, v) -> e.datum = (Boolean) v, e -> e.datum);
        }
    }

    @Nested
    @DisplayName("Multiple Fields Tests")
    class MultipleFieldsTests {

        @Test
        @DisplayName("Should handle two fields with different types")
        void shouldHandleTwoFields() {
            byte[] expectedArray = {
                    0x00, // version
                    0x02, // number of values
                    0x00, // first value id
                    0x10, // ColumnValueType.STRING
                    0x04, // 4 bytes length
                    0x66, 0x6c, 0x6f, 0x77, // "flow"
                    0x01, // second value id
                    0x04, // ColumnValueType.UINT64
                    0x01  // value = 1
            };
            var expected = ByteString.copyFrom(expectedArray);
            var writer = new TypeAwareProtoWireProtocolWriter<>(TwoFields.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(TwoFields.class);

            var entity = new TwoFields();
            entity.stringDatum = "flow";
            entity.longDatum = 1L;

            var chunk = writer.writeEntity(entity);
            assertEquals(expected, chunk);

            var deserialized = reader.readChunk(chunk);
            assertEquals("flow", deserialized.stringDatum);
            assertEquals(1L, deserialized.longDatum);
        }

        @Test
        @DisplayName("Should handle multiple fields with all primitive types")
        void shouldHandleMultipleFieldsWithAllTypes() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(MultipleTypesEntity.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(MultipleTypesEntity.class);

            var entity = new MultipleTypesEntity();
            entity.stringField = "test";
            entity.longField = 5L;
            entity.doubleField = 2.0;
            entity.booleanField = true;

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertEquals("test", deserialized.stringField);
            assertEquals(5L, deserialized.longField);
            assertEquals(2.0, deserialized.doubleField, 0.0001);
            assertTrue(deserialized.booleanField);
        }

        @Test
        @DisplayName("Should handle multiple fields with some null values")
        void shouldHandleMultipleFieldsWithNulls() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(MultipleTypesEntity.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(MultipleTypesEntity.class);

            var entity = new MultipleTypesEntity();
            entity.stringField = "test";
            entity.longField = null;
            entity.doubleField = 2.0;
            entity.booleanField = null;

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertEquals("test", deserialized.stringField);
            assertNull(deserialized.longField);
            assertEquals(2.0, deserialized.doubleField, 0.0001);
            assertNull(deserialized.booleanField);
        }
    }

    @Nested
    @DisplayName("Primitive Type Tests")
    class PrimitiveTypeTests {

        @ParameterizedTest
        @ValueSource(ints = {42, 0, 1, -1, 100, -100, Integer.MAX_VALUE, Integer.MIN_VALUE})
        @DisplayName("Should handle various primitive int values")
        void shouldHandleVariousPrimitiveIntValues(int value) {
            testRoundTrip(PrimitiveIntField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @ParameterizedTest
        @ValueSource(longs = {123456789L, 0L, 1L, -1L, 1000L, -1000L, Long.MAX_VALUE, Long.MIN_VALUE})
        @DisplayName("Should handle various primitive long values")
        void shouldHandleVariousPrimitiveLongValues(long value) {
            testRoundTrip(PrimitiveLongField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @Test
        @DisplayName("Should handle primitive double field")
        void shouldHandlePrimitiveDouble() {
            testRoundTripWithAssertion(PrimitiveDoubleField.class,
                    (e, v) -> e.datum = v, e -> e.datum, 3.14159,
                    (expected, actual) -> assertEquals(expected, actual, 0.0001));
        }

        @Test
        @DisplayName("Should handle primitive boolean field")
        void shouldHandlePrimitiveBoolean() {
            testRoundTrip(PrimitiveBooleanField.class, (e, v) -> e.datum = v, e -> e.datum, true);
        }

        @Test
        @DisplayName("Should handle primitive short field")
        void shouldHandlePrimitiveShort() {
            testRoundTrip(PrimitiveShortField.class, (e, v) -> e.datum = v, e -> e.datum, (short) 1234);
        }

        @Test
        @DisplayName("Should handle primitive byte field")
        void shouldHandlePrimitiveByte() {
            testRoundTrip(PrimitiveByteField.class, (e, v) -> e.datum = v, e -> e.datum, (byte) 127);
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should throw exception for invalid version")
        void shouldThrowExceptionForInvalidVersion() {
            byte[] chunkArray = {
                    0x01, // version = 1 (invalid)
                    0x01, // number of values
                    0x00, // value id
                    0x10, // ColumnValueType.STRING
                    0x02, // 2 bytes length
                    0x74, 0x70 // "tp"
            };
            var chunk = ByteString.copyFrom(chunkArray);

            var reader = new TypeAwareProtoWireProtocolReader<>(OneStringField.class);
            var exception = assertThrows(IllegalStateException.class, () -> reader.readChunk(chunk));
            assertTrue(exception.getMessage().contains("Unversioned row does not support versions"));
        }

        @Test
        @DisplayName("Should throw exception for invalid column ID")
        void shouldThrowExceptionForInvalidColumnId() {
            byte[] chunkArray = {
                    0x00, // version
                    0x01, // number of values
                    0x05, // value id = 5 (expected 0)
                    0x10, // ColumnValueType.STRING
                    0x02, // 2 bytes length
                    0x74, 0x70 // "tp"
            };
            var chunk = ByteString.copyFrom(chunkArray);

            var reader = new TypeAwareProtoWireProtocolReader<>(OneStringField.class);
            var exception = assertThrows(IllegalStateException.class, () -> reader.readChunk(chunk));
            assertTrue(exception.getMessage().contains("Unexpected column id"));
        }

        @Test
        @DisplayName("Should throw exception for unsupported map key type")
        void shouldThrowExceptionForUnsupportedMapKeyType() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TypeAwareProtoWireProtocolWriter<>(MapNonStringKey.class)
            );
        }

        @Test
        @DisplayName("Should reject String value larger than 16MB on output")
        void shouldRejectStringLargerThan16Mb() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(OneStringField.class);
            var entity = new OneStringField();
            entity.datum = "a".repeat(WireProtocol.MAX_STRING_VALUE_LENGTH + 1);
            var exception = assertThrows(IllegalStateException.class, () -> writer.writeEntity(entity));
            assertTrue(exception.getMessage().contains("data length"));
        }

        @Test
        @DisplayName("Should reject byte[] value larger than 16MB on output")
        void shouldRejectByteArrayLargerThan16Mb() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(ByteArrayField.class);
            var entity = new ByteArrayField();
            entity.data = new byte[WireProtocol.MAX_STRING_VALUE_LENGTH + 1];
            var exception = assertThrows(IllegalStateException.class, () -> writer.writeEntity(entity));
            assertTrue(exception.getMessage().contains("data length"));
        }
    }

    @Nested
    @DisplayName("Round-trip Tests")
    class RoundTripTests {

        @Test
        @DisplayName("Should perform round-trip for simple entity")
        void shouldPerformRoundTripForSimpleEntity() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(TwoFields.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(TwoFields.class);

            var original = new TwoFields();
            original.stringDatum = "roundtrip";
            original.longDatum = 42L;

            var chunk = writer.writeEntity(original);
            var deserialized = reader.readChunk(chunk);

            assertEquals(original.stringDatum, deserialized.stringDatum);
            assertEquals(original.longDatum, deserialized.longDatum);
        }

        @Test
        @DisplayName("Should perform round-trip for complex entity")
        void shouldPerformRoundTripForComplexEntity() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(MultipleTypesEntity.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(MultipleTypesEntity.class);

            var original = new MultipleTypesEntity();
            original.stringField = "complex";
            original.longField = 999L;
            original.doubleField = 3.14159;
            original.booleanField = true;

            var chunk = writer.writeEntity(original);
            var deserialized = reader.readChunk(chunk);

            assertEquals(original.stringField, deserialized.stringField);
            assertEquals(original.longField, deserialized.longField);
            assertEquals(original.doubleField, deserialized.doubleField, 0.0001);
            assertEquals(original.booleanField, deserialized.booleanField);
        }
    }

    @Nested
    @DisplayName("Edge Cases Tests")
    class EdgeCasesTests {

        @ParameterizedTest
        @ValueSource(longs = {0L, Long.MAX_VALUE, Long.MIN_VALUE})
        @DisplayName("Should handle edge case numeric values")
        void shouldHandleEdgeCaseNumericValues(long value) {
            testRoundTrip(OneLongField.class, (e, v) -> e.datum = v, e -> e.datum, value);
        }

        @Test
        @DisplayName("Should handle string with special characters")
        void shouldHandleStringWithSpecialCharacters() {
            testRoundTrip(OneStringField.class, (e, v) -> e.datum = v, e -> e.datum,
                    "Line1\nLine2\tTabbed\r\nWindows");
        }

        @Test
        @DisplayName("Should handle string with special utf-8 characters")
        void shouldHandleStringWithSpecialUtf8Characters() {
            testRoundTrip(OneStringField.class, (e, v) -> e.datum = v, e -> e.datum,
                    "Hello 世界 \uD83C\uDF0D");
        }
    }

    @Nested
    @DisplayName("Protobuf Cases Tests")
    class ProtobufCasesTests {
        @Test
        @DisplayName("Should handle protobuf cases")
        void shouldHandleProtobufCases() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(ProtobufEntity.class);
            var reader = new TypeAwareProtoWireProtocolReader<>(ProtobufEntity.class);

            var entity = new ProtobufEntity();
            entity.datum = TTestMessage.newBuilder()
                    .setId("1")
                    .setTime(1714215000L)
                    .setCount(1)
                    .setValue(1.1)
                    .setIsValue(true)
                    .build();

            var chunk = writer.writeEntity(entity);
            var deserialized = reader.readChunk(chunk);

            assertEquals(entity.datum, deserialized.datum);
        }
    }

    @Nested
    @DisplayName("Audit Regression Tests")
    class AuditRegressionTests {

        /**
         * Builds a single-value row chunk so the value bytes can be crafted exactly,
         * independent of writer-side encoding choices.
         */
        private ByteString singleValueChunk(ColumnValueType type, ChunkValueWriter valueWriter) throws Exception {
            var os = new ByteArrayOutputStream();
            var cos = CodedOutputStream.newInstance(os);
            cos.writeUInt32NoTag(0); // version
            cos.writeUInt32NoTag(1); // value count
            cos.writeUInt32NoTag(0); // column id 0
            cos.writeRawByte((byte) (type.getValue() & 0xFF)); // type
            valueWriter.write(cos);
            cos.flush();
            return ByteString.copyFrom(os.toByteArray());
        }

        @FunctionalInterface
        private interface ChunkValueWriter {
            void write(CodedOutputStream cos) throws Exception;
        }

        @Test
        @DisplayName("#6: rejects a negative STRING length on input")
        void shouldRejectNegativeStringLength() throws Exception {
            var chunk = singleValueChunk(ColumnValueType.STRING, cos -> cos.writeUInt64NoTag(-1L));
            var reader = new TypeAwareProtoWireProtocolReader<>(OneStringField.class);
            var ex = assertThrows(IllegalStateException.class, () -> reader.readChunk(chunk));
            assertTrue(ex.getMessage().contains("data length"));
        }

        @Test
        @DisplayName("#6: rejects a STRING length above Integer.MAX_VALUE on input")
        void shouldRejectOversizedStringLength() throws Exception {
            var chunk = singleValueChunk(ColumnValueType.STRING, cos -> cos.writeUInt64NoTag(0x80000000L));
            var reader = new TypeAwareProtoWireProtocolReader<>(OneStringField.class);
            var ex = assertThrows(IllegalStateException.class, () -> reader.readChunk(chunk));
            assertTrue(ex.getMessage().contains("data length"));
        }

        @Test
        @DisplayName("#12: round-trips a payload larger than the default 2KB buffer")
        void shouldRoundTripPayloadLargerThanDefaultBuffer() {
            testRoundTrip(OneStringField.class, (e, v) -> e.datum = v, e -> e.datum, "x".repeat(5000));
        }

        @Test
        @DisplayName("#12: output length equals serialized size, not the grown buffer capacity")
        void shouldNotPadOutputToBufferCapacity() {
            var writer = new TypeAwareProtoWireProtocolWriter<>(ByteArrayField.class);
            var entity = new ByteArrayField();
            entity.data = new byte[4096]; // forces ByteArrayOutputStream growth beyond the 2KB default
            var chunk = writer.writeEntity(entity);
            // version(1) + count(1) + id(1) + type(1) + length-varint(2) + 4096 payload
            assertEquals(4102, chunk.size());
        }
    }
}
