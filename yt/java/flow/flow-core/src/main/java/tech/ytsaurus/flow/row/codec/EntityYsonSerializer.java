package tech.ytsaurus.flow.row.codec;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.protobuf.MessageLite;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.flow.typeinfo.TypeInfo;
import tech.ytsaurus.flow.utils.AnnotationUtils;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.yson.YsonTextWriter;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Maps an {@code @Entity}-annotated POJO to and from YSON.
 *
 * <p>An entity is represented as a YSON <em>map</em> whose keys are the entity's column names and
 * whose values are the serialized field values; a {@code null} field value is written as a YSON
 * entity ({@code #}). On deserialization, unknown map keys are ignored, absent keys leave the
 * corresponding field at its default, and an entity-valued key sets the field to {@code null}
 * (a primitive field, which cannot hold {@code null}, is instead left at its default).
 *
 * <p>Protobuf message fields are stored as a YSON string of their wire bytes. Nested
 * {@code @Entity} fields are supported and serialized as nested YSON maps; a cyclic reference
 * between entity classes is rejected when the serializer is created.
 *
 * @param <T> entity type to (de)serialize
 */
class EntityYsonSerializer<T> implements YTreeSerializer<T> {

    private final Class<T> entityClass;
    private final TypeInfo<T> typeInfo;
    private final ColumnCodec[] columnCodecs;

    /**
     * Creates a serializer by introspecting {@code entityClass} for its column layout.
     *
     * @param entityClass class of the entity to be (de)serialized; must be annotated with
     *                    {@code @Entity}
     * @throws IllegalArgumentException if {@code entityClass} is not annotated with {@code @Entity},
     *                                  or if its fields form a cyclic entity reference
     */
    EntityYsonSerializer(Class<T> entityClass) {
        this(validateEntity(entityClass), new TypeInfo<>(entityClass));
    }

    private EntityYsonSerializer(Class<T> entityClass, TypeInfo<T> typeInfo) {
        this.entityClass = entityClass;
        this.typeInfo = typeInfo;
        List<TypeInfo.Column> columns = typeInfo.getColumns();
        this.columnCodecs = new ColumnCodec[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            this.columnCodecs[i] = new ColumnCodec(columns.get(i));
        }
    }

    /**
     * Validates that {@code entityClass} is an entity and free of cyclic entity references.
     *
     * @param entityClass the class to validate
     * @return {@code entityClass} unchanged
     * @throws IllegalArgumentException if it is not {@code @Entity}-annotated or its fields form a
     *                                  cyclic entity reference
     */
    private static <T> Class<T> validateEntity(Class<T> entityClass) {
        if (!ClassUtils.anyOfAnnotationsPresent(entityClass, AnnotationUtils.entityAnnotations())) {
            throw new IllegalArgumentException(
                    "Class must be annotated with @Entity: " + entityClass.getName());
        }
        // Fail fast on cyclic nested-entity references before EntityTableSchemaCreator recurses
        // into them (its struct recursion is not cycle-guarded and would StackOverflow).
        detectEntityCycle(entityClass, new LinkedHashSet<>());
        return entityClass;
    }

    /**
     * Recursively walks entity-typed fields, throwing if {@code entityClass} reappears on the
     * current traversal path.
     *
     * @param entityClass the entity class to inspect
     * @param path        the entity classes currently on the traversal path
     * @throws IllegalArgumentException if a cyclic entity reference is found
     */
    private static void detectEntityCycle(Class<?> entityClass, LinkedHashSet<Class<?>> path) {
        if (!path.add(entityClass)) {
            String chain = path.stream().map(Class::getName).collect(Collectors.joining(" -> "));
            throw new IllegalArgumentException(
                    "Entity " + entityClass.getName() + " has a cyclic field reference: "
                            + chain + " -> " + entityClass.getName());
        }
        for (Field field : ClassUtils.getAllDeclaredFields(entityClass)) {
            if (ClassUtils.isFieldTransient(field, AnnotationUtils.transientAnnotations())) {
                continue;
            }
            for (Class<?> referenced : referencedEntities(field.getGenericType())) {
                detectEntityCycle(referenced, path);
            }
        }
        path.remove(entityClass);
    }

    /**
     * Collects the {@code @Entity}-annotated classes reachable from {@code type}, including those
     * nested in arrays and generic type arguments.
     *
     * @param type the field type to inspect
     * @return the referenced entity classes, possibly empty
     */
    private static List<Class<?>> referencedEntities(Type type) {
        var result = new ArrayList<Class<?>>();
        collectReferencedEntities(type, result);
        return result;
    }

    /**
     * Adds the {@code @Entity}-annotated classes reachable from {@code type} to {@code out}.
     *
     * @param type the type to inspect
     * @param out  the collection that receives the referenced entity classes
     */
    private static void collectReferencedEntities(Type type, List<Class<?>> out) {
        if (type instanceof ParameterizedType parameterized) {
            for (Type arg : parameterized.getActualTypeArguments()) {
                collectReferencedEntities(arg, out);
            }
            return;
        }
        if (type instanceof GenericArrayType arrayType) {
            collectReferencedEntities(arrayType.getGenericComponentType(), out);
            return;
        }
        if (!(type instanceof Class<?> clazz)) {
            // Wildcards / type variables carry nothing concrete to inspect.
            return;
        }
        if (clazz.isArray()) {
            collectReferencedEntities(clazz.getComponentType(), out);
            return;
        }
        if (ClassUtils.anyOfAnnotationsPresent(clazz, AnnotationUtils.entityAnnotations())) {
            out.add(clazz);
        }
    }

    /**
     * Returns the table schema derived from the entity class.
     *
     * @return the entity's table schema
     */
    public TableSchema getEntityTableSchema() {
        return typeInfo.getTableSchema();
    }

    /**
     * Writes {@code obj} to {@code consumer} as a YSON map of column name to field value.
     *
     * <p>A {@code null} entity is written as a YSON entity; otherwise every column is emitted in
     * schema order, with {@code null} field values written as YSON entities.
     *
     * @param obj      the entity to serialize, or {@code null}
     * @param consumer the YSON consumer to write to
     */
    @Override
    public void serialize(@Nullable T obj, YsonConsumer consumer) {
        if (obj == null) {
            consumer.onEntity();
            return;
        }
        consumer.onBeginMap();
        for (ColumnCodec columnCodec : columnCodecs) {
            byte[] key = columnCodec.keyBytes;
            consumer.onKeyedItem(key, 0, key.length);
            Object value = columnCodec.column.get(obj);
            if (value == null) {
                consumer.onEntity();
            } else if (columnCodec.isProtobuf) {
                byte[] bytes = ((MessageLite) value).toByteArray();
                consumer.onString(bytes, 0, bytes.length);
            } else {
                columnCodec.requireSerializer().serialize(value, consumer);
            }
        }
        consumer.onEndMap();
    }

    /**
     * Serializes an entity to binary YSON.
     *
     * @param obj the entity to serialize
     * @return the binary YSON bytes
     */
    public byte[] serialize(T obj) {
        var baos = new ByteArrayOutputStream();
        try (var consumer = YTreeBinarySerializer.getSerializer(baos)) {
            serialize(obj, consumer);
        }
        return baos.toByteArray();
    }

    /**
     * Serializes an entity to binary or text YSON.
     *
     * @param obj    the entity to serialize
     * @param binary {@code true} for binary YSON, {@code false} for text YSON
     * @return the YSON bytes
     */
    public byte[] serialize(T obj, boolean binary) {
        if (binary) {
            return serialize(obj);
        }
        var baos = new ByteArrayOutputStream();
        try (var writer = new YsonTextWriter(baos)) {
            serialize(obj, writer);
        }
        return baos.toByteArray();
    }

    /**
     * Reconstructs an entity from a YSON map node.
     *
     * <p>Each column is populated from the map entry matching its column name; unknown entries are
     * ignored, absent entries leave the field at its default, and an entity-valued entry sets the
     * field to {@code null}.
     *
     * <p>An entity-valued entry ({@code #}) sets the field to {@code null}, except for a primitive
     * field, which cannot hold {@code null} and is therefore left at its default.
     *
     * @param node the YSON node to read
     * @return the reconstructed entity, or {@code null} if {@code node} is {@code null} or a YSON
     * entity
     * @throws IllegalArgumentException if {@code node} is neither a YSON entity nor a YSON map
     */
    @Override
    public @Nullable T deserialize(@Nullable YTreeNode node) {
        if (node == null || node.isEntityNode()) {
            return null;
        }
        if (!node.isMapNode()) {
            throw new IllegalArgumentException(
                    "Expected a YSON map to deserialize " + entityClass.getName()
                            + ", got node: " + node.getClass().getSimpleName());
        }
        var map = node.mapNode().asMap();
        T obj = typeInfo.createInstance();
        for (ColumnCodec columnCodec : columnCodecs) {
            YTreeNode child = map.get(columnCodec.column.getSchema().getName());
            if (child == null) {
                continue;
            }
            if (child.isEntityNode()) {
                // A YSON entity (#) means null; a primitive field cannot hold null, so leave it at
                // its default value, matching the treatment of an absent key.
                if (!columnCodec.isPrimitive) {
                    columnCodec.column.set(obj, null);
                }
                continue;
            }
            if (columnCodec.isProtobuf) {
                columnCodec.column.set(obj, ProtoUtils.parseBytes(child.bytesValue(), columnCodec.messageClass));
            } else {
                columnCodec.column.set(obj, columnCodec.requireSerializer().deserialize(child));
            }
        }
        return obj;
    }

    /**
     * Deserializes an entity from binary YSON.
     *
     * @param bytes the binary YSON bytes
     * @return the deserialized entity, or {@code null} if {@code bytes} encode a YSON entity
     * ({@code #}), the inverse of {@link #serialize(Object, YsonConsumer) serialize(null)}
     */
    public @Nullable T deserialize(byte[] bytes) {
        return deserialize(YsonUtils.yTreeFromBinary(bytes));
    }

    /**
     * Deserializes an entity from a binary YSON stream.
     *
     * @param in the binary YSON input stream
     * @return the deserialized entity, or {@code null} if the stream encodes a YSON entity
     * ({@code #}), the inverse of {@link #serialize(Object, YsonConsumer) serialize(null)}
     */
    public @Nullable T deserialize(InputStream in) {
        return deserialize(YTreeBinarySerializer.deserialize(in));
    }

    /**
     * Returns the entity class this serializer is bound to.
     *
     * @return the entity class
     */
    @Override
    public Class<T> getClazz() {
        return entityClass;
    }

    /**
     * Returns the YT column type used to represent a serialized entity.
     *
     * @return an optional YSON column type
     */
    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

    /**
     * Pre-computed per-column (de)serialization metadata, resolved once when the serializer is
     * built so neither {@link #serialize} nor {@link #deserialize} pays per-row reflection.
     */
    private static final class ColumnCodec {
        private final TypeInfo.Column column;
        private final byte[] keyBytes;
        private final boolean isProtobuf;
        /**
         * Whether the target field has a primitive type, which cannot hold {@code null}.
         */
        private final boolean isPrimitive;
        /**
         * Serializer for the column value; {@code null} for protobuf columns.
         */
        private final @Nullable YTreeSerializer<Object> serializer;
        /**
         * Protobuf message class of the column value; {@code null} for non-protobuf columns.
         */
        private final @Nullable Class<? extends MessageLite> messageClass;

        /**
         * Resolves and caches the (de)serialization metadata for a single entity column.
         *
         * @param column the entity column to describe
         */
        @SuppressWarnings("unchecked")
        ColumnCodec(TypeInfo.Column column) {
            this.column = column;
            this.keyBytes = column.getSchema().getName().getBytes(StandardCharsets.UTF_8);
            this.isProtobuf = column.getDescriptor().isProtobuf();
            this.isPrimitive = column.getDescriptor().getField().getType().isPrimitive();
            if (isProtobuf) {
                this.serializer = null;
                this.messageClass = column.getDescriptor().getField().getType().asSubclass(MessageLite.class);
            } else {
                this.serializer = (YTreeSerializer<Object>) column.getDescriptor().getYTreeSerializer();
                this.messageClass = null;
            }
        }

        /**
         * Returns the value serializer, asserting the non-protobuf invariant under which it is
         * non-{@code null}.
         *
         * @return the column value serializer
         */
        private YTreeSerializer<Object> requireSerializer() {
            return Objects.requireNonNull(serializer, "serializer is null for non-protobuf column");
        }
    }
}
