package tech.ytsaurus.flow.typeinfo;

import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeInfoTest {

    @Test
    public void testOneString() {
        var typeInfo = new TypeInfo<>(OneString.class);
        assertNotNull(typeInfo);
        assertEquals(1, typeInfo.getTableSchema().getColumnsCount());
    }

    @Test
    public void testMultipleFields() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        assertNotNull(typeInfo);
        assertEquals(4, typeInfo.getTableSchema().getColumnsCount());
        assertEquals(4, typeInfo.getColumns().size());

        // Verify column names
        var columns = typeInfo.getColumns();
        assertEquals("name", columns.get(0).getSchema().getName());
        assertEquals("age", columns.get(1).getSchema().getName());
        assertEquals("active", columns.get(2).getSchema().getName());
        assertEquals("score", columns.get(3).getSchema().getName());
    }

    @Test
    public void testTransientFieldsExcluded() {
        var typeInfo = new TypeInfo<>(EntityWithTransient.class);
        assertNotNull(typeInfo);
        // Only non-transient fields should be included
        assertEquals(2, typeInfo.getTableSchema().getColumnsCount());
        assertEquals(2, typeInfo.getColumns().size());

        // Verify that transient field is not in columns
        var columnNames = typeInfo.getColumns().stream()
                .map(col -> col.getSchema().getName())
                .toList();
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("name"));
        assertFalse(columnNames.contains("temporaryData"));
    }

    @Test
    public void testCreateInstance() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        MultipleFields instance = typeInfo.createInstance();

        assertNotNull(instance);
        assertNull(instance.name);
        assertNull(instance.age);
        assertNull(instance.active);
        assertNull(instance.score);
    }

    @Test
    public void testColumnSetAndGet() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        MultipleFields instance = typeInfo.createInstance();

        var columns = typeInfo.getColumns();

        // Test set and get for each column
        columns.getFirst().set(instance, "John Doe");
        assertEquals("John Doe", instance.name);
        assertEquals("John Doe", columns.get(0).get(instance));

        columns.get(1).set(instance, 30);
        assertEquals(Integer.valueOf(30), instance.age);
        assertEquals(Integer.valueOf(30), columns.get(1).get(instance));

        columns.get(2).set(instance, true);
        assertTrue(instance.active);
        assertEquals(Boolean.TRUE, columns.get(2).get(instance));

        columns.get(3).set(instance, 95L);
        assertEquals(Long.valueOf(95L), instance.score);
        assertEquals(Long.valueOf(95L), columns.get(3).get(instance));
    }

    @Test
    public void testColumnSetNull() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        MultipleFields instance = typeInfo.createInstance();

        var columns = typeInfo.getColumns();

        // Set values first
        columns.get(0).set(instance, "Test");
        columns.get(1).set(instance, 25);

        // Set to null
        columns.get(0).set(instance, null);
        columns.get(1).set(instance, null);

        assertNull(instance.name);
        assertNull(instance.age);
    }

    @Test
    public void testGetMessageClass() {
        var typeInfo = new TypeInfo<>(OneString.class);
        assertEquals(OneString.class, typeInfo.getMessageClass());

        var typeInfo2 = new TypeInfo<>(MultipleFields.class);
        assertEquals(MultipleFields.class, typeInfo2.getMessageClass());
    }

    @Test
    public void testGetTableSchema() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        var schema = typeInfo.getTableSchema();

        assertNotNull(schema);
        assertEquals(4, schema.getColumnsCount());
        assertFalse(schema.getColumns().isEmpty());
    }

    @Test
    public void testGetColumns() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        var columns = typeInfo.getColumns();

        assertNotNull(columns);
        assertEquals(4, columns.size());

        // Verify each column has schema and descriptor
        for (var column : columns) {
            assertNotNull(column.getSchema());
            assertNotNull(column.getDescriptor());
            assertNotNull(column.getDescriptor().getField());
        }
    }

    @Test
    public void testColumnDescriptor() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        var columns = typeInfo.getColumns();

        // Test first column descriptor
        var firstColumn = columns.get(0);
        var descriptor = firstColumn.getDescriptor();

        assertNotNull(descriptor);
        assertNotNull(descriptor.getField());
        assertEquals("name", descriptor.getField().getName());
        assertFalse(descriptor.isTransient());
        assertFalse(descriptor.isProtobuf());
    }

    @Test
    public void testEmptyEntity() {
        var typeInfo = new TypeInfo<>(EmptyEntity.class);
        assertNotNull(typeInfo);
        assertEquals(0, typeInfo.getTableSchema().getColumnsCount());
        assertEquals(0, typeInfo.getColumns().size());
    }

    @Test
    public void testEntityWithAllTransientFields() {
        var typeInfo = new TypeInfo<>(AllTransientFields.class);
        assertNotNull(typeInfo);
        assertEquals(0, typeInfo.getTableSchema().getColumnsCount());
        assertEquals(0, typeInfo.getColumns().size());
    }

    @Test
    public void testComplexTypes() {
        var typeInfo = new TypeInfo<>(ComplexTypes.class);
        assertNotNull(typeInfo);
        assertEquals(3, typeInfo.getTableSchema().getColumnsCount());

        var instance = typeInfo.createInstance();
        var columns = typeInfo.getColumns();

        // Test List<String>
        List<String> tags = List.of("tag1", "tag2");
        columns.get(0).set(instance, tags);
        assertEquals(tags, instance.tags);

        // Test byte[]
        byte[] data = new byte[]{1, 2, 3};
        columns.get(1).set(instance, data);
        assertEquals(data, instance.data);

        // Test Double
        columns.get(2).set(instance, 3.14);
        assertEquals(3.14, instance.value);
    }

    @Test
    public void testGetNonTransientDescriptors() {
        var descriptors = TypeInfo.getNonTransientDescriptors(EntityWithTransient.class);

        assertNotNull(descriptors);
        assertEquals(2, descriptors.size());

        var fieldNames = descriptors.stream()
                .map(d -> d.getField().getName())
                .toList();

        assertTrue(fieldNames.contains("id"));
        assertTrue(fieldNames.contains("name"));
        assertFalse(fieldNames.contains("temporaryData"));
    }

    @Test
    public void testClassWithoutDefaultConstructor() {
        assertThrows(RuntimeException.class, () -> {
            new TypeInfo<>(NoDefaultConstructor.class);
        });
    }

    @Test
    public void testColumnIsProtobufFlag() {
        var typeInfo = new TypeInfo<>(MultipleFields.class);
        var columns = typeInfo.getColumns();

        // None of the fields in MultipleFields are protobuf
        for (var column : columns) {
            assertFalse(column.isProtobuf);
        }
    }

    // Test entities

    @Entity
    static class OneString {
        String datum;
    }

    @Entity
    static class MultipleFields {
        String name;
        Integer age;
        Boolean active;
        Long score;
    }

    @Entity
    static class EntityWithTransient {
        Long id;
        String name;
        @Transient
        String temporaryData;
    }

    @Entity
    static class EmptyEntity {
        // No fields
    }

    @Entity
    static class AllTransientFields {
        @Transient
        String field1;
        @Transient
        Integer field2;
    }

    @Entity
    static class ComplexTypes {
        List<String> tags;
        byte[] data;
        Double value;
    }

    @Entity
    static class NoDefaultConstructor {
        String name;

        NoDefaultConstructor(String name) {
            this.name = name;
        }
    }
}
