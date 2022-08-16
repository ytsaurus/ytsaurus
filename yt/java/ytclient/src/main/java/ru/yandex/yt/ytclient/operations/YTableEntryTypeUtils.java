package ru.yandex.yt.ytclient.operations;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import ru.yandex.inside.yt.kosher.utils.ClassUtils;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;

public final class YTableEntryTypeUtils {
    private static final List<Class<?>> DEFAULT_CLASSES = Arrays.asList(
            MapperOrReducer.class,
            Mapper.class,
            Reducer.class,
            ReducerWithKey.class,
            VanillaJob.class
    );

    private YTableEntryTypeUtils() {
    }

    public static YTableEntryType resolve(Object obj, int genericParam) {
        Optional<Type> genericSuperclass = ClassUtils.getAllGenericInterfaces(obj.getClass())
                .stream().filter(i -> DEFAULT_CLASSES.contains(ClassUtils.erasure(i)))
                .findFirst();

        if (genericSuperclass.isPresent()) {
            List<Type> actualTypes = ClassUtils.getActualTypeArguments(genericSuperclass.get());
            return forType(actualTypes.get(genericParam));
        } else {
            throw new IllegalStateException("Can't resolve types for " + obj);
        }
    }

    public static YTableEntryType forType(Type type) {
        Class clazz = ClassUtils.erasure(type);
        if (clazz.equals(YTreeMapNode.class)) {
            return YTableEntryTypes.YSON;
        } else {
            throw new IllegalArgumentException("Can't resolve type for " + type);
        }
    }
}
