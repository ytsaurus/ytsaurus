package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.Test;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;

import static org.junit.Assert.assertEquals;

public class SchemaOfEntityTest {

    @Entity
    static class Person {
        @Column(nullable = false, name = "person-name")
        private String name;
        @Column(nullable = false, columnDefinition = "uint8")
        private Long age;
        @Column(name = "mobile-phone")
        private Phone phone;
        @Transient
        private String password;
        private final List<String> organizations = new ArrayList<>();
    }

    static class Phone {
        private int number;
    }

    @Test
    public void testCreateSchema() {
        var entitySchema = SchemaConverter.toSkiffSchema(
                EntityTableSchemaCreator.create(Person.class)
        );

        SkiffSchema expectedSchema = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.STRING_32).setName("person-name"),
                        SkiffSchema.simpleType(WireType.UINT_8).setName("age"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.tuple(
                                                List.of(
                                                        SkiffSchema.simpleType(WireType.INT_32).setName("number")
                                                )
                                        )
                                ))
                                .setName("mobile-phone"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.repeatedVariant8(List.of(
                                                        SkiffSchema.variant8(List.of(
                                                                        SkiffSchema.nothing(),
                                                                        SkiffSchema.simpleType(WireType.STRING_32)
                                                                )
                                                        )
                                                )
                                        )
                                ))
                                .setName("organizations")
                ));

        assertEquals(entitySchema, expectedSchema);
    }
}
