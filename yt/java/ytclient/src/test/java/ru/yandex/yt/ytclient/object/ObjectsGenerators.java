package ru.yandex.yt.ytclient.object;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class ObjectsGenerators {

    @SuppressWarnings("unchecked")
    public static <T> Generator<T> generator(Class<T> clazz) {
        if (clazz.equals(SmallObjectClass.class)) {
            return (Generator<T>) new GeneratorForSmallObjectClass();
        } else if (clazz.equals(SmallPrimitiveClass.class)) {
            return (Generator<T>) new GeneratorForSmallPrimitiveClass();
        } else if (clazz.equals(LargeObjectClass.class)) {
            return (Generator<T>) new GeneratorForLargeObjectClass();
        } else if (clazz.equals(LargePrimitiveClass.class)) {
            return (Generator<T>) new GeneratorForLargePrimitiveClass();
        } else if (clazz.equals(LargeFlattenObjectClass.class)) {
            return (Generator<T>) new GeneratorForLargeFlattenObjectClass();
        } else if (clazz.equals(LargeFlattenPrimitiveClass.class)) {
            return (Generator<T>) new GeneratorForLargeFlattenPrimitiveClass();
        } else if (clazz.equals(LargeUnflattenObjectClass.class)) {
            return (Generator<T>) new GeneratorForLargeUnflattenObjectClass();
        } else if (clazz.equals(LargeUnflattenPrimitiveClass.class)) {
            return (Generator<T>) new GeneratorForLargeUnflattenPrimitiveClass();
        } else {
            throw new IllegalArgumentException("Unable to create generator for class " + clazz);
        }
    }


    public interface Generator<T> {
        default String generateString(Random random, int length) {
            return RandomStringUtils.randomAscii(length);
        }

        T generateNext(Random random);
    }


    static class GeneratorForSmallObjectClass implements Generator<SmallObjectClass> {

        @Override
        public SmallObjectClass generateNext(Random random) {
            final SmallObjectClass instance = new SmallObjectClass();
            instance.setBooleanField(random.nextBoolean());
            instance.setDoubleField(random.nextDouble());
            instance.setFloatField(random.nextFloat());
            instance.setIntField(random.nextInt());
            instance.setLongField(random.nextLong());
            instance.setStringField(generateString(random, 64));
            return instance;
        }
    }

    static class GeneratorForSmallPrimitiveClass implements Generator<SmallPrimitiveClass> {

        @Override
        public SmallPrimitiveClass generateNext(Random random) {
            final SmallPrimitiveClass instance = new SmallPrimitiveClass();
            instance.setBooleanField(random.nextBoolean());
            instance.setDoubleField(random.nextDouble());
            instance.setFloatField(random.nextFloat());
            instance.setIntField(random.nextInt());
            instance.setLongField(random.nextLong());
            instance.setStringField(generateString(random, 64));
            return instance;
        }
    }

    static class GeneratorForLargeObjectClass implements Generator<LargeObjectClass> {

        @Override
        public LargeObjectClass generateNext(Random random) {
            final LargeObjectClass instance = new LargeObjectClass();
            instance.setBooleanField1(random.nextBoolean());
            instance.setBooleanField2(random.nextBoolean());
            instance.setBooleanField3(random.nextBoolean());
            instance.setBooleanField4(random.nextBoolean());
            instance.setBooleanField5(random.nextBoolean());

            instance.setDoubleField1(random.nextDouble());
            instance.setDoubleField2(random.nextDouble());
            instance.setDoubleField3(random.nextDouble());
            instance.setDoubleField4(random.nextDouble());
            instance.setDoubleField5(random.nextDouble());

            instance.setFloatField1(random.nextFloat());
            instance.setFloatField2(random.nextFloat());
            instance.setFloatField3(random.nextFloat());
            instance.setFloatField4(random.nextFloat());
            instance.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            instance.setLongField1(random.nextLong());
            instance.setLongField2(random.nextLong());
            instance.setLongField3(random.nextLong());
            instance.setLongField4(random.nextLong());
            instance.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }

    static class GeneratorForLargePrimitiveClass implements Generator<LargePrimitiveClass> {

        @Override
        public LargePrimitiveClass generateNext(Random random) {
            final LargePrimitiveClass instance = new LargePrimitiveClass();
            instance.setBooleanField1(random.nextBoolean());
            instance.setBooleanField2(random.nextBoolean());
            instance.setBooleanField3(random.nextBoolean());
            instance.setBooleanField4(random.nextBoolean());
            instance.setBooleanField5(random.nextBoolean());

            instance.setDoubleField1(random.nextDouble());
            instance.setDoubleField2(random.nextDouble());
            instance.setDoubleField3(random.nextDouble());
            instance.setDoubleField4(random.nextDouble());
            instance.setDoubleField5(random.nextDouble());

            instance.setFloatField1(random.nextFloat());
            instance.setFloatField2(random.nextFloat());
            instance.setFloatField3(random.nextFloat());
            instance.setFloatField4(random.nextFloat());
            instance.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            instance.setLongField1(random.nextLong());
            instance.setLongField2(random.nextLong());
            instance.setLongField3(random.nextLong());
            instance.setLongField4(random.nextLong());
            instance.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }

    static class GeneratorForLargeFlattenObjectClass implements Generator<LargeFlattenObjectClass> {

        @Override
        public LargeFlattenObjectClass generateNext(Random random) {
            final LargeFlattenObjectClass instance = new LargeFlattenObjectClass();

            final InternalFlattenObject2 object2 = new InternalFlattenObject2();
            instance.setObject2(object2);

            final InternalObject21 object21 = new InternalObject21();
            object2.setObject21(object21);
            object21.setBooleanField1(random.nextBoolean());
            object21.setBooleanField2(random.nextBoolean());
            object21.setBooleanField3(random.nextBoolean());
            object21.setBooleanField4(random.nextBoolean());
            object21.setBooleanField5(random.nextBoolean());

            object2.setDoubleField1(random.nextDouble());
            object2.setDoubleField2(random.nextDouble());
            object2.setDoubleField3(random.nextDouble());
            object2.setDoubleField4(random.nextDouble());
            object2.setDoubleField5(random.nextDouble());

            object2.setFloatField1(random.nextFloat());
            object2.setFloatField2(random.nextFloat());
            object2.setFloatField3(random.nextFloat());
            object2.setFloatField4(random.nextFloat());
            object2.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            final InternalObject1 object1 = new InternalObject1();
            instance.setObject1(object1);
            object1.setLongField1(random.nextLong());
            object1.setLongField2(random.nextLong());
            object1.setLongField3(random.nextLong());
            object1.setLongField4(random.nextLong());
            object1.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }

    static class GeneratorForLargeFlattenPrimitiveClass implements Generator<LargeFlattenPrimitiveClass> {

        @Override
        public LargeFlattenPrimitiveClass generateNext(Random random) {
            final LargeFlattenPrimitiveClass instance = new LargeFlattenPrimitiveClass();

            final InternalFlattenPrimitive2 object2 = new InternalFlattenPrimitive2();
            instance.setObject2(object2);

            final InternalPrimitive21 object21 = new InternalPrimitive21();
            object2.setObject21(object21);
            object21.setBooleanField1(random.nextBoolean());
            object21.setBooleanField2(random.nextBoolean());
            object21.setBooleanField3(random.nextBoolean());
            object21.setBooleanField4(random.nextBoolean());
            object21.setBooleanField5(random.nextBoolean());

            object2.setDoubleField1(random.nextDouble());
            object2.setDoubleField2(random.nextDouble());
            object2.setDoubleField3(random.nextDouble());
            object2.setDoubleField4(random.nextDouble());
            object2.setDoubleField5(random.nextDouble());

            object2.setFloatField1(random.nextFloat());
            object2.setFloatField2(random.nextFloat());
            object2.setFloatField3(random.nextFloat());
            object2.setFloatField4(random.nextFloat());
            object2.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            final InternalPrimitive1 object1 = new InternalPrimitive1();
            instance.setObject1(object1);
            object1.setLongField1(random.nextLong());
            object1.setLongField2(random.nextLong());
            object1.setLongField3(random.nextLong());
            object1.setLongField4(random.nextLong());
            object1.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }

    static class GeneratorForLargeUnflattenObjectClass implements Generator<LargeUnflattenObjectClass> {

        @Override
        public LargeUnflattenObjectClass generateNext(Random random) {
            final LargeUnflattenObjectClass instance = new LargeUnflattenObjectClass();

            final InternalUnflattenObject2 object2 = new InternalUnflattenObject2();
            instance.setObject2(object2);

            final InternalObject21 object21 = new InternalObject21();
            object2.setObject21(object21);
            object21.setBooleanField1(random.nextBoolean());
            object21.setBooleanField2(random.nextBoolean());
            object21.setBooleanField3(random.nextBoolean());
            object21.setBooleanField4(random.nextBoolean());
            object21.setBooleanField5(random.nextBoolean());

            object2.setDoubleField1(random.nextDouble());
            object2.setDoubleField2(random.nextDouble());
            object2.setDoubleField3(random.nextDouble());
            object2.setDoubleField4(random.nextDouble());
            object2.setDoubleField5(random.nextDouble());

            object2.setFloatField1(random.nextFloat());
            object2.setFloatField2(random.nextFloat());
            object2.setFloatField3(random.nextFloat());
            object2.setFloatField4(random.nextFloat());
            object2.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            final InternalObject1 object1 = new InternalObject1();
            instance.setObject1(object1);
            object1.setLongField1(random.nextLong());
            object1.setLongField2(random.nextLong());
            object1.setLongField3(random.nextLong());
            object1.setLongField4(random.nextLong());
            object1.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }

    static class GeneratorForLargeUnflattenPrimitiveClass implements Generator<LargeUnflattenPrimitiveClass> {

        @Override
        public LargeUnflattenPrimitiveClass generateNext(Random random) {
            final LargeUnflattenPrimitiveClass instance = new LargeUnflattenPrimitiveClass();

            final InternalUnflattenPrimitive2 object2 = new InternalUnflattenPrimitive2();
            instance.setObject2(object2);

            final InternalPrimitive21 object21 = new InternalPrimitive21();
            object2.setObject21(object21);
            object21.setBooleanField1(random.nextBoolean());
            object21.setBooleanField2(random.nextBoolean());
            object21.setBooleanField3(random.nextBoolean());
            object21.setBooleanField4(random.nextBoolean());
            object21.setBooleanField5(random.nextBoolean());

            object2.setDoubleField1(random.nextDouble());
            object2.setDoubleField2(random.nextDouble());
            object2.setDoubleField3(random.nextDouble());
            object2.setDoubleField4(random.nextDouble());
            object2.setDoubleField5(random.nextDouble());

            object2.setFloatField1(random.nextFloat());
            object2.setFloatField2(random.nextFloat());
            object2.setFloatField3(random.nextFloat());
            object2.setFloatField4(random.nextFloat());
            object2.setFloatField5(random.nextFloat());

            instance.setIntField1(random.nextInt());
            instance.setIntField2(random.nextInt());
            instance.setIntField3(random.nextInt());
            instance.setIntField4(random.nextInt());
            instance.setIntField5(random.nextInt());

            final InternalPrimitive1 object1 = new InternalPrimitive1();
            instance.setObject1(object1);
            object1.setLongField1(random.nextLong());
            object1.setLongField2(random.nextLong());
            object1.setLongField3(random.nextLong());
            object1.setLongField4(random.nextLong());
            object1.setLongField5(random.nextLong());

            instance.setStringField1(generateString(random, 8));
            instance.setStringField2(generateString(random, 16));
            instance.setStringField3(generateString(random, 24));
            instance.setStringField4(generateString(random, 32));
            instance.setStringField5(generateString(random, 64));

            return instance;
        }
    }
}
