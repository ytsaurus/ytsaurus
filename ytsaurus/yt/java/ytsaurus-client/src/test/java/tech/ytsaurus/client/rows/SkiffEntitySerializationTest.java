package tech.ytsaurus.client.rows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SkiffEntitySerializationTest {

    @Entity
    static class Person {
        private static final int IGNORED_CONSTANT = 10;
        @Column(nullable = false, name = "person-name")
        private String name;
        private int age;
        @Column(name = "mobile-phone")
        private Phone phone;
        @Transient
        private String password;
        private List<String> organizations = new ArrayList<>();
        @Nullable
        private String car;

        Person() {
        }

        Person(String name, int age, Phone phone, String password, List<String> organizations, @Nullable String car) {
            this.name = name;
            this.age = age;
            this.phone = phone;
            this.password = password;
            this.organizations = organizations;
            this.car = car;
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

        public List<String> getOrganizations() {
            return organizations;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return age == person.age &&
                    Objects.equals(name, person.name) &&
                    Objects.equals(phone, person.phone) &&
                    Objects.equals(organizations, person.organizations) &&
                    Objects.equals(car, person.car);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age, phone, password, organizations, car);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", phone=" + phone +
                    ", password='" + password + '\'' +
                    ", organizations=" + organizations +
                    ", car='" + car + '\'' +
                    '}';
        }
    }

    static class Phone {
        private int number;

        private Phone() {
        }

        Phone(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Phone phone = (Phone) o;
            return number == phone.number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(number);
        }

        @Override
        public String toString() {
            return "Phone{" +
                    "number=" + number +
                    '}';
        }
    }

    @Test
    public void testSerializeEntity() {
        Person person = new Person("Ivan", 20,
                new Phone(12345),
                "secret", Arrays.asList("ytsaurus", null, "spbu"), null);

        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(person.getName().length());
        byte[] lengthOfNameBytes = buffer.array();
        byte[] nameBytes = person.getName().getBytes(StandardCharsets.UTF_8);

        buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(person.getAge());
        byte[] ageBytes = buffer.array();

        buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(person.getPhone().getNumber());
        byte[] phoneNumberBytes = buffer.array();

        buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(person.getOrganizations().get(0).length());
        byte[] lengthOfFirstOrganizationBytes = buffer.array();
        byte[] firstOrganizationBytes = person.getOrganizations().get(0).getBytes(StandardCharsets.UTF_8);

        buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(person.getOrganizations().get(2).length());
        byte[] lengthOfThirdOrganizationBytes = buffer.array();
        byte[] thirdOrganizationBytes = person.getOrganizations().get(2).getBytes(StandardCharsets.UTF_8);

        byte[] expectedBytes = ByteBuffer
                .allocate(3 + lengthOfNameBytes.length + nameBytes.length +
                        ageBytes.length + phoneNumberBytes.length + 2 + lengthOfFirstOrganizationBytes.length +
                        firstOrganizationBytes.length + 4 + lengthOfThirdOrganizationBytes.length +
                        thirdOrganizationBytes.length + 1)
                .put(lengthOfNameBytes)
                .put(nameBytes)
                .put(ageBytes)
                .put((byte) 0x01)
                .put(phoneNumberBytes)
                .put((byte) 0x01)
                .put((byte) 0x00)
                .put((byte) 0x01)
                .put(lengthOfFirstOrganizationBytes)
                .put(firstOrganizationBytes)
                .put((byte) 0x00)
                .put((byte) 0x00)
                .put((byte) 0x00)
                .put((byte) 0x01)
                .put(lengthOfThirdOrganizationBytes)
                .put(thirdOrganizationBytes)
                .put((byte) 0xFF)
                .put((byte) 0x00)
                .array();

        byte[] bytes = new EntitySkiffSerializer<>(Person.class).serialize(person);

        assertArrayEquals(expectedBytes, bytes);
    }

    @Test
    public void testDeserializeEntity() {
        Person person = new Person("Ivan", 20,
                new Phone(12345),
                "secret", Arrays.asList("ytsaurus", null, "spbu"), null);

        byte[] bytes = new EntitySkiffSerializer<>(Person.class).serialize(person);

        Person deserializedPerson = new EntitySkiffSerializer<>(Person.class)
                .deserialize(bytes)
                .orElseThrow(IllegalStateException::new);

        assertEquals(person, deserializedPerson);
    }
}
