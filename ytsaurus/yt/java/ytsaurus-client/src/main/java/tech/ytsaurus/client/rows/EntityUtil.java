package tech.ytsaurus.client.rows;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;

public class EntityUtil {
    private EntityUtil() {
    }

    public static boolean isEntityAnnotationPresent(Class<?> clazz) {
        return anyOfAnnotationsPresent(clazz, JavaPersistenceApi.entityAnnotations());
    }
}
