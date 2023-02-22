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

        List<Field> fields = getAllDeclaredFields(superClazz);
        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        return fields;
    }

    @SuppressWarnings("unchecked")
    public static <Type> Type castToType(Object object) {
        return (Type) object;
    }

    @SuppressWarnings("unchecked")
    public static <E, T> List<E> castToList(T object) {
        return (List<E>) object;
    }

    public static Class<?> getTypeParameterOfGeneric(Field field) {
        return (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
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

    public static void setFieldsAccessibleToTrue(Field[] fields) {
        for (var field : fields) {
            field.setAccessible(true);
        }
    }
}
