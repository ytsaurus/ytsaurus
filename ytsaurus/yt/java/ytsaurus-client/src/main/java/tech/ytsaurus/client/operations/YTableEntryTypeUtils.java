package tech.ytsaurus.client.operations;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.Message;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeMapNode;

import static tech.ytsaurus.client.rows.EntityUtil.isEntityAnnotationPresent;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.erasure;


@NonNullApi
@NonNullFields
final class YTableEntryTypeUtils {
    private static final List<Class<?>> DEFAULT_CLASSES = Arrays.asList(MapperOrReducer.class, Mapper.class,
            Reducer.class, ReducerWithKey.class, VanillaJob.class);

    private YTableEntryTypeUtils() {
    }

    public static <T, I, O> YTableEntryType<T> resolve(MapperOrReducer<I, O> mapperOrReducer, int genericParam) {
        Optional<Type> genericSuperclass = ClassUtils.getAllGenericInterfaces(mapperOrReducer.getClass()).stream()
                .filter(i -> DEFAULT_CLASSES.contains(erasure(i)))
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
        Class<T> clazz = erasure(type);
        boolean actualTrackIndices = trackIndices && isInputType;
        if (clazz.equals(YTreeMapNode.class)) {
            return castToType(YTableEntryTypes.yson(actualTrackIndices));
        } else if (isEntityAnnotationPresent(clazz)) {
            return YTableEntryTypes.entity(clazz, actualTrackIndices, isInputType);
        } else if (Message.class.isAssignableFrom(clazz)) {
            return castToType(YTableEntryTypes.proto(castToType(clazz), actualTrackIndices, isInputType));
        } else {
            throw new IllegalArgumentException("Can't resolve type for " + type);
        }
    }
}
