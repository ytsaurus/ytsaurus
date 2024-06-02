package tech.ytsaurus.client.rows;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;

import org.junit.Test;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class EmbeddedEntityTest {

    @Entity
    static class Student {
        @Column(nullable = false, name = "student-name")
        private String name;
        @Embedded
        private University university;
    }

    @Embeddable
    static class University {
        @Column(nullable = false, name = "university-name")
        private String name;
        private Address address;
        private BigDecimal rating;
    }

    @Embeddable
    static class Address {
        @Column(nullable = false)
        private String country;
        @Column(nullable = false)
        private String city;
        private String street;
        private transient String fullAddress;
    }

    @Test
    public void testEmbeddedWithInnerEmbeddedSchema() {
        final var partialSchema = TableSchema.builder()
                .add(ColumnSchema.builder("rating", TiType.decimal(10, 2)).build())
                .build();
        var entitySchema = SchemaConverter.toSkiffSchema(
                EntityTableSchemaCreator.create(Student.class, partialSchema)
        );

        SkiffSchema expectedSchema = SkiffSchema.tuple(
                List.of(
                        SkiffSchema.simpleType(WireType.STRING_32).setName("student-name"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("university-name"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("country"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("city"),
                        SkiffSchema.variant8(List.of(
                                SkiffSchema.nothing(),
                                SkiffSchema.simpleType(WireType.STRING_32)
                        )).setName("street"),
                        SkiffSchema.variant8(List.of(
                                SkiffSchema.nothing(),
                                SkiffSchema.simpleType(WireType.INT_64)
                        )).setName("rating")
                ));

        assertEquals(expectedSchema, entitySchema);
    }

    @Entity
    static class PostV1 {
        private long id;
        private String title;
        @Embedded
        private PostDetailsV1 details;
    }

    static class PostDetailsV1 {
        private long id;
        private String text;
        private ZonedDateTime createdAt;
        private String createdBy;
    }

    @Test
    public void testExceptionWhenEmbeddedFieldIsNotEmbeddable() {
        assertThrows(IllegalArgumentException.class, () -> EntityTableSchemaCreator.create(PostV1.class));
    }

    @Entity
    static class PostV2 {
        private long id;
        private String title;
        @Embedded
        private PostDetailsV2 details;
    }

    @Embeddable
    static class PostDetailsV2 {
        private long id;
        private Content content;
        private ZonedDateTime createdAt;
        private String createdBy;
    }

    @Embeddable
    static class Content {
        private String text;
        private PostDetailsV2 details;
    }

    @Test
    public void testExceptionWhenEntityHasEmbeddableLoop() {
        assertThrows(IllegalArgumentException.class, () -> EntityTableSchemaCreator.create(PostV2.class));
    }

    @Entity
    static class Employee {
        @Column(nullable = false, name = "employee-name")
        private String name;
        @Column(nullable = false, name = "dept")
        Department department;
    }

    static class Department {
        private String id;
        @Embedded
        Address address;
    }

    @Test
    public void testEmbeddedIntoNestedSchema() {
        var entitySchema = SchemaConverter.toSkiffSchema(
                EntityTableSchemaCreator.create(Employee.class)
        );

        SkiffSchema expectedSchema = SkiffSchema.tuple(
                List.of(
                        SkiffSchema.simpleType(WireType.STRING_32).setName("employee-name"),
                        SkiffSchema.tuple(
                                        List.of(
                                                SkiffSchema.variant8(List.of(
                                                        SkiffSchema.nothing(),
                                                        SkiffSchema.simpleType(WireType.STRING_32)
                                                )).setName("id"),
                                                SkiffSchema.simpleType(WireType.STRING_32).setName("country"),
                                                SkiffSchema.simpleType(WireType.STRING_32).setName("city"),
                                                SkiffSchema.variant8(List.of(
                                                        SkiffSchema.nothing(),
                                                        SkiffSchema.simpleType(WireType.STRING_32)
                                                )).setName("street")
                                        )
                                )
                                .setName("dept")
                ));

        assertEquals(expectedSchema, entitySchema);
    }

    @Test
    public void testEmbeddedSerialization() {
        final var serializer = new EntitySkiffSerializer<>(Student.class);
        final var partialSchema = TableSchema.builder()
                .add(ColumnSchema.builder("rating", TiType.decimal(10, 2)).build())
                .build();
        serializer.setTableSchema(partialSchema);
        final var address = new Address();
        address.city = "City";
        address.street = "Street";
        address.country = "Country";
        final var university = new University();
        university.name = "University";
        university.address = address;
        university.rating = new BigDecimal("123.45");
        final var student = new Student();
        student.name = "Student";
        student.university = university;

        final var serializedStudent = serializer.serialize(student);
        final var deserializedStudent = serializer.deserialize(serializedStudent).get();

        assertEquals(student.name, deserializedStudent.name);
        assertEquals(student.university.name, deserializedStudent.university.name);
        assertEquals(student.university.rating, deserializedStudent.university.rating);
        assertEquals(student.university.address.city, deserializedStudent.university.address.city);
        assertEquals(student.university.address.street, deserializedStudent.university.address.street);
        assertEquals(student.university.address.country, deserializedStudent.university.address.country);
    }

}
