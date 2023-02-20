package tech.ytsaurus.client.operations;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.persistence.Entity;

import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeMapNode;


@NonNullApi
@NonNullFields
final class YTableEntryTypeUtils {
    private static final List<Class<?>> DEFAULT_CLASSES = Arrays.asList(
            MapperOrReducer.class,
            Mapper.class,
            Reducer.class,
            ReducerWithKey.class,
            VanillaJob.class
    );

    private YTableEntryTypeUtils() {
    }

    public static <T, I, O> YTableEntryType<T> resolve(MapperOrReducer<I, O> mapperOrReducer, int genericParam) {
        Optional<Type> genericSuperclass = ClassUtils.getAllGenericInterfaces(mapperOrReducer.getClass()).stream()
                .filter(i -> DEFAULT_CLASSES.contains(ClassUtils.erasure(i)))
                .findFirst();

        if (genericSuperclass.isPresent()) {
            List<Type> actualTypes = ClassUtils.getActualTypeArguments(genericSuperclass.get());
            return forType(actualTypes.get(genericParam), mapperOrReducer.trackIndices(), isInputType(genericParam));
        } else {
            throw new IllegalStateException("Can't resolve types for " + mapperOrReducer);
        }
    }

    private static boolean isInputType(int genericParam) {
        return genericParam == 0;
    }

    public static <T> YTableEntryType<T> forType(Type type, boolean trackIndices, boolean isInputType) {
        Class<T> clazz = ClassUtils.erasure(type);
        if (clazz.equals(YTreeMapNode.class)) {
            return ClassUtils.castToType(YTableEntryTypes.YSON);
        } else if (clazz.isAnnotationPresent(Entity.class)) {
            return new EntityTableEntryType<>(clazz, isInputType && trackIndices, isInputType);
        } else {
            throw new IllegalArgumentException("Can't resolve type for " + type);
        }
    }
}
