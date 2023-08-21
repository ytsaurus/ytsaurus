package tech.ytsaurus.client.rows;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfField;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

class EntityFieldDescr {
    private final Field field;
    private final boolean isTransient;
    private final List<Type> typeParameters;

    private EntityFieldDescr(Field field) {
        this.field = field;
        this.isTransient = isFieldTransient(field, JavaPersistenceApi.transientAnnotations());
        this.typeParameters = getTypeParametersOfField(field);
    }

    Field getField() {
        return field;
    }

    boolean isTransient() {
        return isTransient;
    }

    public List<Type> getTypeParameters() {
        return typeParameters;
    }

    static List<EntityFieldDescr> of(List<Field> fields) {
        return fields.stream()
                .map(EntityFieldDescr::new)
                .collect(Collectors.toUnmodifiableList());
    }
}
