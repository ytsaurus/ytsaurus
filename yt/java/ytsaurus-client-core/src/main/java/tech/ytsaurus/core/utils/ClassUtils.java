package tech.ytsaurus.core.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ClassUtils {
    private ClassUtils() {
    }

    public static <A extends Annotation, T extends AnnotatedElement> boolean hasAnnotationRecursive(
            T cls, Class<A> annotationCls) {
        return getAnnotationRecursive(cls, annotationCls).isPresent();
    }

    public static <A extends Annotation, T extends AnnotatedElement> Optional<A> getAnnotationRecursive(
            T cls, Class<A> annotationCls) {
        Set<Class<? extends Annotation>> seen = new HashSet<>();
        List<A> annotations = getAnnotationsRecursive(cls, seen, annotationCls);
        if (annotations.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(annotations.get(0));
    }

    @SuppressWarnings("unchecked")
    private static <A extends Annotation, T extends AnnotatedElement> List<A> getAnnotationsRecursive(
            T cls,
            Set<Class<? extends Annotation>> seen,
            Class<A> annotationClass
    ) {
        final List<A> rv = new ArrayList<>();
        for (Annotation annotation : cls.getAnnotations()) {
            final Class<? extends Annotation> type = annotation.annotationType();
            if (seen.contains(type)) {
                continue;
            }
            if (type.isAssignableFrom(annotationClass)) {
                rv.add((A) annotation);
            } else {
                seen.add(type);
                rv.addAll(getAnnotationsRecursive(type, seen, annotationClass));
            }
        }
        return rv;
    }

    public static List<Type> getAllGenericInterfaces(Class<?> cls) {
        List<Type> result = new ArrayList<>();
        if (cls.getSuperclass() != null) {
            result.addAll(getAllGenericInterfaces(cls.getSuperclass()));
        }
        result.addAll(Arrays.asList(cls.getGenericInterfaces()));
        for (Class<?> intf : cls.getInterfaces()) {
            result.addAll(getAllGenericInterfaces(intf));
        }

        return result.stream().distinct().collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public static <A> Class<A> erasure(Type type) {
        if (type instanceof Class<?>) {
            return ((Class<A>) type);
        } else if (type instanceof ParameterizedType) {
            return erasure(((ParameterizedType) type).getRawType());
        } else if (type instanceof GenericArrayType) {
            Class<?> componentType = erasure(((GenericArrayType) type).getGenericComponentType());
            return (Class<A>) makeArrayClass(componentType);
        } else if (type instanceof TypeVariable) {
            Type[] bounds = ((TypeVariable<?>) type).getBounds();
            if (bounds.length != 1) {
                throw new RuntimeException("Not a single");
            }
            return erasure(bounds[0]);
        } else if (type instanceof WildcardType) {
            Type[] upperBounds = ((WildcardType) type).getUpperBounds();
            if (upperBounds.length == 0) {
                throw new RuntimeException("Empty list");
            }
            return erasure(upperBounds[0]);
        } else {
            throw new IllegalArgumentException("don't know how to get erasure of " + type);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T[]> makeArrayClass(Class<T> cls) {
        return (Class<T[]>) Array.newInstance(cls, 0).getClass();
    }

    public static List<Type> getActualTypeArguments(Type type) {
        return Arrays.asList(((ParameterizedType) type).getActualTypeArguments());
    }

    public static <T> List<Field> getAllDeclaredFields(Class<T> clazz) {
        Class<? super T> superClazz = clazz.getSuperclass();
        if (superClazz == null) {
            return new ArrayList<>();
        }

        List<Field> fields = getAllDeclaredFields(superClazz).stream()
                .filter(field -> Modifier.isPublic(field.getModifiers())
                        || Modifier.isProtected(field.getModifiers()))
                .collect(Collectors.toList());
        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        return fields;
    }

    @SuppressWarnings("unchecked")
    public static <Type> Type castToType(Object object) {
        return (Type) object;
    }

    public static <E, T> E[] boxArray(T array, Class<?> elementClass) {
        if (elementClass.equals(int.class)) {
            return castToType(Arrays.stream((int[]) array).boxed().toArray(Integer[]::new));
        }
        if (elementClass.equals(long.class)) {
            return castToType(Arrays.stream((long[]) array).boxed().toArray(Long[]::new));
        }
        if (elementClass.equals(double.class)) {
            return castToType(Arrays.stream((double[]) array).boxed().toArray(Double[]::new));
        }
        if (elementClass.equals(byte.class)) {
            var unboxedArray = (byte[]) array;
            var boxedArray = new Byte[unboxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                boxedArray[i] = unboxedArray[i];
            }
            return castToType(boxedArray);
        }
        if (elementClass.equals(short.class)) {
            var unboxedArray = (short[]) array;
            var boxedArray = new Short[unboxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                boxedArray[i] = unboxedArray[i];
            }
            return castToType(boxedArray);
        }
        if (elementClass.equals(float.class)) {
            var unboxedArray = (float[]) array;
            var boxedArray = new Float[unboxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                boxedArray[i] = unboxedArray[i];
            }
            return castToType(boxedArray);
        }
        if (elementClass.equals(boolean.class)) {
            var unboxedArray = (boolean[]) array;
            var boxedArray = new Boolean[unboxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                boxedArray[i] = unboxedArray[i];
            }
            return castToType(boxedArray);
        }
        if (elementClass.equals(char.class)) {
            var unboxedArray = (char[]) array;
            var boxedArray = new Character[unboxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                boxedArray[i] = unboxedArray[i];
            }
            return castToType(boxedArray);
        }
        throw new IllegalArgumentException("Elements of array are not primitive");
    }

    public static <T, O> O unboxArray(T array, Class<?> elementClass) {
        if (elementClass.equals(Integer.class)) {
            return castToType(Arrays.stream((Integer[]) array).mapToInt(Integer::intValue).toArray());
        }
        if (elementClass.equals(Long.class)) {
            return castToType(Arrays.stream((Long[]) array).mapToLong(Long::longValue).toArray());
        }
        if (elementClass.equals(Double.class)) {
            return castToType(Arrays.stream((Double[]) array).mapToDouble(Double::doubleValue).toArray());
        }
        if (elementClass.equals(Byte.class)) {
            var boxedArray = (Byte[]) array;
            var unboxedArray = new byte[boxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                unboxedArray[i] = boxedArray[i];
            }
            return castToType(unboxedArray);
        }
        if (elementClass.equals(Short.class)) {
            var boxedArray = (Short[]) array;
            var unboxedArray = new short[boxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                unboxedArray[i] = boxedArray[i];
            }
            return castToType(unboxedArray);
        }
        if (elementClass.equals(Float.class)) {
            var boxedArray = (Float[]) array;
            var unboxedArray = new float[boxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                unboxedArray[i] = boxedArray[i];
            }
            return castToType(unboxedArray);
        }
        if (elementClass.equals(Boolean.class)) {
            var boxedArray = (Boolean[]) array;
            var unboxedArray = new boolean[boxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                unboxedArray[i] = boxedArray[i];
            }
            return castToType(unboxedArray);
        }
        if (elementClass.equals(Character.class)) {
            var boxedArray = (Character[]) array;
            var unboxedArray = new char[boxedArray.length];
            for (int i = 0; i < boxedArray.length; i++) {
                unboxedArray[i] = boxedArray[i];
            }
            return castToType(unboxedArray);
        }
        throw new IllegalArgumentException("Elements of array are not primitive");
    }

    public static List<Type> getTypeParametersOfGenericField(Field field) {
        return getTypeParametersOfGeneric(field.getGenericType());
    }

    public static List<Type> getTypeParametersOfGeneric(Type type) {
        return List.of(((ParameterizedType) type).getActualTypeArguments());
    }

    public static boolean isFieldTransient(Field field, Set<String> transientAnnotations) {
        return ((field.getModifiers() & Modifier.TRANSIENT) != 0 ||
                anyOfAnnotationsPresent(field, transientAnnotations));

    }

    public static boolean anyOfAnnotationsPresent(AnnotatedElement element,
                                                  Set<String> annotations) {
        return Arrays.stream(element.getDeclaredAnnotations())
                .map(annotation -> annotation.annotationType().getName())
                .anyMatch(annotations::contains);
    }

    public static boolean anyMatchWithAnnotation(Annotation annotation,
                                                 Set<String> annotationNames) {
        return annotationNames.contains(annotation.annotationType().getName());
    }

    public static Optional<Annotation> getAnnotationIfPresent(AnnotatedElement element,
                                                              Set<String> annotations) {
        return Arrays.stream(element.getDeclaredAnnotations())
                .filter(annotation -> annotations.contains(annotation.annotationType().getName()))
                .findAny();
    }

    public static <T> T getValueOfAnnotationProperty(Annotation annotation, String propertyName) {
        try {
            return castToType(annotation.annotationType().getDeclaredMethod(propertyName).invoke(annotation));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFieldsAccessibleToTrue(List<Field> fields) {
        fields.forEach(field -> field.setAccessible(true));
    }
}
