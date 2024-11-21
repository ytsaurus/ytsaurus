package tech.ytsaurus.client.rows;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import tech.ytsaurus.client.ArrowTableRowsSerializer;
import tech.ytsaurus.core.rows.YTGetters;
import tech.ytsaurus.typeinfo.ListType;
import tech.ytsaurus.typeinfo.OptionalType;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrowEntitySerializationTest {
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

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
    public void testSerializeEntity() throws Exception {
        Person person = new Person("Ivan", 20,
                new Phone(12345),
                "secret", Arrays.asList("ytsaurus", null, "spbu"), null);
        var byteArrayOutputStream = new ByteArrayOutputStream();
        try (var arrowTableRowsSerializer = new ArrowTableRowsSerializer<>(List.of(
                Map.entry("name", new YTGetters.FromStructToString<Person>() {
                    @Override
                    public ByteBuffer getString(Person person) {
                        return ByteBuffer.wrap(person.name.getBytes());
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        ysonConsumer.onString(person.name);
                    }

                    @Override
                    public TiType getTiType() {
                        return TiType.string();
                    }
                }),
                Map.entry("age", new YTGetters.FromStructToInt<Person>() {
                    @Override
                    public int getInt(Person person) {
                        return person.age;
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        ysonConsumer.onInteger(person.age);
                    }

                    @Override
                    public TiType getTiType() {
                        return TiType.int32();
                    }
                }),
                Map.entry("phone", new YTGetters.FromStructToStruct<Person, Phone>() {
                    @Override
                    public List<Map.Entry<String, YTGetters.FromStruct<Phone>>> getMembersGetters() {
                        return List.of(Map.entry("number", new YTGetters.FromStructToInt<Phone>() {
                            @Override
                            public int getInt(Phone phone) {
                                return phone.number;
                            }

                            @Override
                            public void getYson(Phone phone, YsonConsumer ysonConsumer) {
                                ysonConsumer.onInteger(phone.number);
                            }

                            @Override
                            public TiType getTiType() {
                                return TiType.int32();
                            }
                        }));
                    }

                    @Override
                    public Phone getStruct(Person person) {
                        return person.phone;
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        ysonConsumer.onInteger(person.age);
                    }

                    @Override
                    public TiType getTiType() {
                        return TiType.struct(new StructType.Member("number", TiType.int32()));
                    }
                }),
                Map.entry("password", new YTGetters.FromStructToString<Person>() {
                    @Override
                    public TiType getTiType() {
                        return TiType.string();
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        ysonConsumer.onString(person.password);
                    }

                    @Override
                    public ByteBuffer getString(Person person) {
                        return ByteBuffer.wrap(person.password.getBytes());
                    }
                }),
                Map.entry("organizations", new YTGetters.FromStructToList<Person, List<String>>() {
                    private final YTGetters.FromListToOptional<List<String>> elementGetter =
                            new YTGetters.FromListToOptional<>() {
                                private final OptionalType tiType = TiType.optional(TiType.string());

                                private final YTGetters.FromList<List<String>> notEmptyGetter =
                                        new YTGetters.FromListToString<>() {
                                            @Override
                                            public ByteBuffer getString(List<String> struct, int i) {
                                                return ByteBuffer.wrap(struct.get(i).getBytes());
                                            }

                                            @Override
                                            public TiType getTiType() {
                                                return TiType.string();
                                            }

                                            @Override
                                            public int getSize(List<String> strings) {
                                                return strings.size();
                                            }

                                            @Override
                                            public void getYson(List<String> strings, int i, YsonConsumer ysonConsumer) {
                                                ysonConsumer.onString(strings.get(i));
                                            }
                                        };

                                @Override
                                public TiType getTiType() {
                                    return tiType;
                                }

                                @Override
                                public int getSize(List<String> strings) {
                                    return strings.size();
                                }

                                @Override
                                public void getYson(List<String> strings, int i, YsonConsumer ysonConsumer) {
                                    var string = strings.get(i);
                                    if (string == null) {
                                        ysonConsumer.onEntity();
                                    } else {
                                        ysonConsumer.onString(string);
                                    }
                                }

                                @Override
                                public YTGetters.FromList<List<String>> getNotEmptyGetter() {
                                    return notEmptyGetter;
                                }

                                @Override
                                public boolean isEmpty(List<String> strings, int i) {
                                    return strings.get(i) == null;
                                }
                            };
                    private final ListType tiType = TiType.list(elementGetter.getTiType());

                    @Override
                    public TiType getTiType() {
                        return tiType;
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        ysonConsumer.onBeginList();
                        for (var organization : person.organizations) {
                            ysonConsumer.onString(organization);
                        }
                        ysonConsumer.onEndList();
                    }

                    @Override
                    public YTGetters.FromList<List<String>> getElementGetter() {
                        return elementGetter;
                    }

                    @Override
                    public List<String> getList(Person person) {
                        return person.organizations;
                    }
                }),
                Map.entry("car", new YTGetters.FromStructToOptional<Person>() {
                    @Override
                    public TiType getTiType() {
                        return TiType.optional(TiType.string());
                    }

                    @Override
                    public void getYson(Person person, YsonConsumer ysonConsumer) {
                        String car = person.car;
                        if (car != null) {
                            ysonConsumer.onString(car);
                        } else {
                            ysonConsumer.onEntity();
                        }
                    }

                    @Override
                    public YTGetters.FromStruct<Person> getNotEmptyGetter() {
                        return new YTGetters.FromStructToString<Person>() {
                            @Override
                            public TiType getTiType() {
                                return TiType.string();
                            }

                            @Override
                            public void getYson(Person person, YsonConsumer ysonConsumer) {
                                ysonConsumer.onString(person.car);
                            }

                            @Override
                            public ByteBuffer getString(Person person) {
                                return ByteBuffer.wrap(person.car.getBytes());
                            }
                        };
                    }

                    @Override
                    public boolean isEmpty(Person person) {
                        return person.car == null;
                    }
                })
        ))) {
            arrowTableRowsSerializer.writeRows(Channels.newChannel(byteArrayOutputStream), List.of(person));
        }
        try (var allocator = ROOT_ALLOCATOR.newChildAllocator("fromBatchIterator", 0, Long.MAX_VALUE)) {
            var readChannel = new ReadChannel(Channels.newChannel(
                    new ByteArrayInputStream(byteArrayOutputStream.toByteArray())
            ));
            var schema = MessageSerializer.deserializeSchema(readChannel);
            assertEquals(new Schema(List.of(
                    new Field("name", new FieldType(false, new ArrowType.Binary(), null), List.of()),
                    new Field("age", new FieldType(false, new ArrowType.Int(32, true), null), List.of()),
                    new Field("phone", new FieldType(false, new ArrowType.Struct(), null), List.of(
                            new Field("number", new FieldType(false, new ArrowType.Int(32, true), null), List.of())
                    )),
                    new Field("password", new FieldType(false, new ArrowType.Binary(), null), List.of()),
                    new Field("organizations", new FieldType(false, new ArrowType.List(), null), List.of(
                            new Field("item", new FieldType(true, new ArrowType.Binary(), null), List.of())
                    )),
                    new Field("car", new FieldType(true, new ArrowType.Binary(), null), List.of())
            )), schema);
            try (var root = VectorSchemaRoot.create(schema, allocator)) {
                var vectorLoader = new VectorLoader(root);
                try (var arrowRecordBatch = MessageSerializer.deserializeRecordBatch(readChannel, allocator)) {
                    vectorLoader.load(arrowRecordBatch);
                }

                var columns = root.getFieldVectors().iterator();

                var nameVector = (VarBinaryVector) columns.next();
                assertEquals(1, nameVector.getValueCount());
                assertArrayEquals(person.name.getBytes(), nameVector.get(0));

                var ageVector = (IntVector) columns.next();
                assertEquals(1, ageVector.getValueCount());
                assertEquals(person.age, ageVector.get(0));

                var phoneVector = (StructVector) columns.next();
                assertEquals(1, phoneVector.getValueCount());

                var phoneNumberVector = (IntVector) phoneVector.getChildByOrdinal(0);
                assertEquals(1, phoneNumberVector.getValueCount());
                assertEquals(person.phone.number, phoneNumberVector.get(0));

                var passwordVector = (VarBinaryVector) columns.next();
                assertEquals(1, passwordVector.getValueCount());
                assertArrayEquals(person.password.getBytes(), passwordVector.get(0));

                var organizationsVector = (ListVector) columns.next();
                assertEquals(1, organizationsVector.getValueCount());
                assertEquals(person.organizations.size(), organizationsVector.getInnerValueCountAt(0));
                var organizationVector = (VarBinaryVector) organizationsVector.getDataVector();
                assertEquals(person.organizations.size(), organizationVector.getValueCount());
                {
                    for (int i = 0; i < person.organizations.size(); i++) {
                        var organization = person.organizations.get(i);
                        if (organization == null) {
                            assertTrue(organizationVector.isNull(i));
                        } else {
                            assertArrayEquals(organization.getBytes(), organizationVector.get(i));
                        }
                    }
                }

                var cardVector = (VarBinaryVector) columns.next();
                assertEquals(1, cardVector.getValueCount());
                assertTrue(cardVector.isNull(0));

                assertFalse(columns.hasNext());
            }
        }
    }
}
