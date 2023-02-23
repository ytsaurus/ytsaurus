package tech.ytsaurus.skiff.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EntitySkiffDeserializerTest {

    @Entity
    static class Person {
        @Column(nullable = false, name = "person-name")
        private String name;
        private int age;
        @Column(name = "mobile-phone")
        private Phone phone;
        @Transient
        private String password;
        private List<String> organizations = new ArrayList<>();
        private String car;

        Person() {
        }

        Person(String name, int age, Phone phone, String password, List<String> organizations, String car) {
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
    public void testDeserializeEntity() {
        Person person = new Person("Ivan", 20,
                new Phone(12345),
                "secret", Arrays.asList("yandex", null, "spbu"), null);

        byte[] bytes = new EntitySkiffSerializer<>(Person.class).serialize(person);

        Person deserializedPerson = new EntitySkiffSerializer<>(Person.class)
                .deserialize(bytes)
                .get();

        assertEquals(person, deserializedPerson);
    }
}
