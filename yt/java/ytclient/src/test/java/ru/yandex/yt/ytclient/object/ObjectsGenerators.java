package ru.yandex.yt.ytclient.object;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeDateSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeArraySerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeBytesSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeListSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeMapSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeSetSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeBooleanSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeDoubleSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeDurationSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeFloatSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeInstantSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntegerSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeJavaInstantSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeLocalDateTimeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeLongSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeUnsignedLongSerializer;
import ru.yandex.misc.enums.IntEnumResolver;
import ru.yandex.misc.enums.StringEnumResolver;
import ru.yandex.misc.reflection.ClassX;

public class ObjectsGenerators {

    public static <T> Generator<T> generator(Class<T> clazz) {
        return new BuilderImpl<T>(clazz);
    }

    public interface Generator<T> {
        T generateNext(Random random);
    }

    //


    private static class BuilderImpl<T> implements Generator<T> {
        private final YTreeObjectSerializer<T> serializer;
        private final YTreeObjectGenerator generator;

        private BuilderImpl(Class<T> clazz) {
            this.serializer = (YTreeObjectSerializer<T>) YTreeObjectSerializerFactory.forClass(clazz);
            this.generator = new YTreeObjectGenerator(serializer, false);
        }

        @Override
        public T generateNext(Random random) {
            final YTreeBuilder builder = YTree.builder();
            generator.generateNext(builder);
            return serializer.deserialize(builder.build());
        }
    }


    private interface NodeGenerator {
        void generateNext(YTreeBuilder builder);
    }


    private static class FieldWrapper {
        private final YTreeObjectField<?> field;
        private final NodeGenerator generator;

        private FieldWrapper(YTreeObjectField<?> field, NodeGenerator generator) {
            this.field = Objects.requireNonNull(field);
            this.generator = Objects.requireNonNull(generator);
        }
    }

    //


    private static class YTreeObjectGenerator implements NodeGenerator {

        private final Collection<FieldWrapper> fields;
        private final boolean flatten;

        private YTreeObjectGenerator(YTreeObjectSerializer<?> serializer, boolean flatten) {
            this.fields = serializer.getFieldMap().values().map(this::generator).toList();
            this.flatten = flatten;
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            if (!flatten) {
                builder.beginMap();
            }
            for (FieldWrapper field : this.fields) {
                if (!field.field.isFlatten) {
                    builder.onKeyedItem(field.field.key);
                }
                field.generator.generateNext(builder);
            }
            if (!flatten) {
                builder.endMap();
            }
        }

        private FieldWrapper generator(YTreeObjectField<?> field) {
            return new FieldWrapper(field, generator(field.serializer, field.isFlatten));
        }

        private NodeGenerator generator(YTreeSerializer<?> wrappedSerializer, boolean flatten) {
            final YTreeSerializer<?> serializer = MappedRowSerializer.unwrap(wrappedSerializer);
            if (serializer instanceof YTreeBytesSerializer) {
                return new YTreeBytesGenerator();
            } else if (serializer instanceof YTreeUnsignedLongSerializer || serializer instanceof
                    YTreeLongSerializer || serializer instanceof
                    YTreeInstantSerializer || serializer instanceof YTreeDurationSerializer ||
                    serializer instanceof YTreeJavaInstantSerializer) {
                return new YTreeLongGenerator();
            } else if (serializer instanceof YTreeListSerializer) {
                return new YTreeListGenerator(
                        generator(((YTreeListSerializer<?>) serializer).getElemSerializer(), flatten));
            } else if (serializer instanceof YTreeStringEnumSerializer) {
                return new YTreeStringEnumGenerator(((YTreeStringEnumSerializer<?>) serializer).getResolver());
            } else if (serializer instanceof YTreeEnumSerializer) {
                return new YTreeEnumGenerator(((YTreeEnumSerializer<?>) serializer).getClazz());
            } else if (serializer instanceof YTreeSetSerializer) {
                return new YTreeListGenerator(
                        generator(((YTreeSetSerializer<?>) serializer).getElemSerializer(), flatten));
            } else if (serializer instanceof YTreeBooleanSerializer) {
                return new YTreeBooleanGenerator();
            } else if (serializer instanceof YTreeLocalDateTimeSerializer) {
                return new YTreeLocalDateTimeGenerator();
            } else if (serializer instanceof YTreeIntEnumSerializer) {
                return new YTreeIntEnumGenerator(((YTreeIntEnumSerializer<?>) serializer).getResolver());
            } else if (serializer instanceof YTreeMapSerializer) {
                return new YTreeMapGenerator(
                        generator(((YTreeMapSerializer<?>) serializer).getValueSerializer(), flatten));
            } else if (serializer instanceof YTreeObjectSerializer) {
                return new YTreeObjectGenerator((YTreeObjectSerializer<?>) serializer, flatten);
            } else if (serializer instanceof YTreeStringSerializer) {
                return new YTreeStringGenerator();
            } else if (serializer instanceof YTreeDoubleSerializer) {
                return new YTreeDoubleGenerator();
            } else if (serializer instanceof YTreeIntegerSerializer) {
                return new YTreeIntegerGenerator();
            } else if (serializer instanceof AbstractYTreeDateSerializer) {
                return new YTreeDateTimeGenerator(((AbstractYTreeDateSerializer) serializer).getDateFormatter());
            } else if (serializer instanceof YTreeArraySerializer) {
                return new YTreeListGenerator(
                        generator(((YTreeArraySerializer<?, ?>) serializer).getElemSerializer(), flatten));
            } else if (serializer instanceof YTreeFloatSerializer) {
                return new YTreeFloatGenerator();
            } else {
                throw new IllegalArgumentException("Unsupported serializer: " + serializer);
            }
        }
    }

    private static class YTreeBytesGenerator implements NodeGenerator {
        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onBytes(RandomUtils.nextBytes(64));
        }
    }

    private static class YTreeLongGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onInteger(RandomUtils.nextLong());
        }
    }

    private static class YTreeIntegerGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onInteger(RandomUtils.nextInt());
        }
    }

    private static class YTreeBooleanGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onBoolean(RandomUtils.nextDouble(0, 1) >= 0.5);
        }
    }

    private static class YTreeDoubleGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onDouble(RandomUtils.nextDouble());
        }
    }

    private static class YTreeFloatGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onDouble(RandomUtils.nextFloat());
        }
    }

    private static class YTreeStringEnumGenerator implements NodeGenerator {

        private final String[] values;

        YTreeStringEnumGenerator(StringEnumResolver<?> resolver) {
            this.values = resolver.values().toArray(new String[0]);
        }


        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onString(values[RandomUtils.nextInt(0, values.length)]);
        }
    }

    private static class YTreeIntEnumGenerator implements NodeGenerator {

        private final int[] values;

        YTreeIntEnumGenerator(IntEnumResolver<?> resolver) {
            this.values = resolver.values().stream().mapToInt(i -> i).toArray();
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onInteger(values[RandomUtils.nextInt(0, values.length)]);
        }
    }

    private static class YTreeEnumGenerator implements NodeGenerator {

        private final String[] values;

        <T extends Enum<T>> YTreeEnumGenerator(ClassX<T> clazz) {
            this.values = clazz.getEnumConstants().stream().map(Enum::name).toArray(String[]::new);
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onString(values[RandomUtils.nextInt(0, values.length)]);
        }
    }

    private static class YTreeListGenerator implements NodeGenerator {

        private final NodeGenerator generator;

        YTreeListGenerator(NodeGenerator generator) {
            this.generator = Objects.requireNonNull(generator);
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onBeginList();
            for (int i = 0; i < 8; i++) {
                builder.onListItem();
                this.generator.generateNext(builder);
            }
            builder.onEndList();
        }
    }

    private static class YTreeMapGenerator implements NodeGenerator {
        private final NodeGenerator generator;

        YTreeMapGenerator(NodeGenerator generator) {
            this.generator = Objects.requireNonNull(generator);
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onBeginMap();
            for (int i = 0; i < 8; i++) {
                builder.onKeyedItem(RandomStringUtils.randomAlphabetic(16));
                generator.generateNext(builder);

            }
            builder.onEndMap();
        }
    }

    private static class YTreeLocalDateTimeGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onString(LocalDateTime.now().toString());
        }
    }

    private static class YTreeStringGenerator implements NodeGenerator {

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onString(RandomStringUtils.randomAscii(64));
        }
    }

    private static class YTreeDateTimeGenerator implements NodeGenerator {

        private final DateTimeFormatter formatter;

        YTreeDateTimeGenerator(DateTimeFormatter formatter) {
            this.formatter = Objects.requireNonNull(formatter);
        }

        @Override
        public void generateNext(YTreeBuilder builder) {
            builder.onString(formatter.print(Instant.now()));
        }
    }

}
