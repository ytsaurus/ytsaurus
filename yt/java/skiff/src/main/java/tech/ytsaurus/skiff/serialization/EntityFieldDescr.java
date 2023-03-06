package tech.ytsaurus.skiff.serialization;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

class EntityFieldDescr {
    private final Field field;
    private final boolean isTransient;

    private EntityFieldDescr(Field field) {
        this.field = field;
        this.isTransient = isFieldTransient(field, JavaPersistenceApi.transientAnnotations());
    }

    Field getField() {
        return field;
    }

    boolean isTransient() {
        return isTransient;
    }

    static List<EntityFieldDescr> of(List<Field> fields) {
        return fields.stream()
                .map(EntityFieldDescr::new)
                .collect(Collectors.toUnmodifiableList());
    }
}
