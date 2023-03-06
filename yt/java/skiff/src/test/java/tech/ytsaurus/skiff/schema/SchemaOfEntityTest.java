package tech.ytsaurus.skiff.schema;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.Test;
import tech.ytsaurus.skiff.serialization.EntitySkiffSchemaCreator;

import static org.junit.Assert.assertEquals;

public class SchemaOfEntityTest {

    @Entity
    static class Person {
        @Column(nullable = false, name = "person-name")
        private String name;
        private int age;
        @Column(name = "mobile-phone")
        private Phone phone;
        @Transient
        private String password;
        private final List<String> organizations = new ArrayList<>();

        Person() {
        }

        Person(String name, int age, Phone phone, String password) {
            this.name = name;
            this.age = age;
            this.phone = phone;
            this.password = password;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Phone getPhone() {
            return phone;
        }

        public void setPhone(Phone phone) {
            this.phone = phone;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    static class Phone {
        private int number;

        Phone(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }

    @Test
    public void testCreateSchema() {
        var entitySchema = EntitySkiffSchemaCreator.create(Person.class);

        SkiffSchema expectedSchema = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.STRING_32).setName("person-name"),
                        SkiffSchema.simpleType(WireType.INT_32).setName("age"),
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
