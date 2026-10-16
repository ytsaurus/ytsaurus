package tech.ytsaurus.flow.typeinfo;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.MessageLite;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.utils.AnnotationUtils;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

/**
 * Cached reflection metadata for a single entity field.
 */
public class FieldDescriptor {
    private final Field field;
    private final boolean isTransient;
    private final boolean isProtobuf;
    private final @Nullable YTreeSerializer<?> yTreeSerializer;

    /**
     * Inspects {@code field} and caches its descriptor metadata.
     *
     * @param field the entity field to describe
     */
    public FieldDescriptor(Field field) {
        this.field = field;
        this.isTransient = isFieldTransient(field, AnnotationUtils.transientAnnotations());
        if (anyOfAnnotationsPresent(field.getType(), AnnotationUtils.embeddableAnnotations())) {
            throw new IllegalArgumentException("Embedded fields are not supported. Field: " + field.getName());
        }
        this.isProtobuf = MessageLite.class.isAssignableFrom(field.getType());
        if (isTransient || isProtobuf) {
            this.yTreeSerializer = null;
        } else {
            this.yTreeSerializer = CodecRegistry.getInstance()
                    .getYsonCodec()
                    .getOrCreateSerializer(field.getGenericType());
        }
    }

    /**
     * Builds a descriptor for each of the given fields, preserving their order.
     *
     * @param fields the entity fields to describe
     * @return descriptors corresponding to {@code fields} in the same order
     */
    public static List<FieldDescriptor> create(List<Field> fields) {
        return fields.stream()
                .map(FieldDescriptor::new)
                .collect(Collectors.toList());
    }

    /**
     * Returns the reflective field this descriptor describes.
     *
     * @return the reflective {@link Field}
     */
    public Field getField() {
        return field;
    }

    /**
     * Tells whether the described field is marked as transient.
     *
     * @return {@code true} if the field is transient
     */
    public boolean isTransient() {
        return isTransient;
    }

    /**
     * Tells whether the described field holds a Protobuf message.
     *
     * @return {@code true} if the field type extends {@link MessageLite}
     */
    public boolean isProtobuf() {
        return isProtobuf;
    }

    /**
     * Returns the cached YSON serializer for the described field, if one is available.
     *
     * @return the serializer, or {@code null} for transient and Protobuf fields
     */
    public @Nullable YTreeSerializer<?> getYTreeSerializer() {
        return yTreeSerializer;
    }
}
