package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.Test;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.typeinfo.TiType;

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

    @Test
    public void testDecimalWireTypes() {
        var schema = SchemaConverter.toSkiffSchema(TableSchema.builder()
                .add(ColumnSchema.builder("int32", TiType.decimal(9, 2)).build())
                .add(ColumnSchema.builder("int64", TiType.decimal(18, 2)).build())
                .add(ColumnSchema.builder("int128", TiType.decimal(38, 2)).build())
                .add(ColumnSchema.builder("int256", TiType.decimal(39, 2)).build())
                .build());

        assertEquals(
                List.of(WireType.INT_32, WireType.INT_64, WireType.INT_128, WireType.INT_256),
                schema.getChildren().stream().map(SkiffSchema::getWireType).collect(Collectors.toList())
        );
    }
}
