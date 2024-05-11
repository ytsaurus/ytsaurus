package tech.ytsaurus.client.rows;

import org.junit.Test;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SchemaOfEmbeddedEntityTest {

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
    public void testCreateSchema() {
        var entitySchema = SchemaConverter.toSkiffSchema(
            EntityTableSchemaCreator.create(Student.class, null)
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
                )).setName("street")
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
        assertThrows(IllegalArgumentException.class, () -> EntityTableSchemaCreator.create(PostV1.class, null));
    }

    @Entity
    static class PostV2{
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
        assertThrows(IllegalArgumentException.class, () -> EntityTableSchemaCreator.create(PostV2.class, null));
    }
}

