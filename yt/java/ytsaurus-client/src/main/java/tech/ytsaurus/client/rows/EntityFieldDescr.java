package tech.ytsaurus.client.rows;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfField;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

class EntityFieldDescr {
    private final Field field;
    private final boolean isTransient;
    private final boolean isEmbeddable;
    private final List<Type> typeParameters;

    private EntityFieldDescr(Field field) {
        this.field = field;
        this.isTransient = isFieldTransient(field, JavaPersistenceApi.transientAnnotations());
        this.isEmbeddable = anyOfAnnotationsPresent(field.getType(), JavaPersistenceApi.embeddableAnnotations());
        this.typeParameters = getTypeParametersOfField(field);
    }

    Field getField() {
        return field;
    }

    boolean isTransient() {
        return isTransient;
    }

    boolean isEmbeddable() {
        return isEmbeddable;
    }

    public List<Type> getTypeParameters() {
        return typeParameters;
    }

    static List<EntityFieldDescr> of(List<Field> fields) {
        return fields.stream()
                .map(EntityFieldDescr::new)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public String toString() {
        return "EntityFieldDescr{" +
            "field=" + field +
            ", isTransient=" + isTransient +
            ", isEmbeddable=" + isEmbeddable +
            ", typeParameters=" + typeParameters +
            '}';
    }
}
