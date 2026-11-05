package tech.ytsaurus.flow.typeinfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.MessageLite;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.EntityTableSchemaCreator;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.core.utils.ClassUtils;

public class TypeInfo<T> {
    private final Class<T> messageClass;
    private final TableSchema tableSchema;
    private final Constructor<T> defaultConstructor;
    private final List<Column> columns;

    public TypeInfo(Class<T> messageClass) {
        this.messageClass = messageClass;
        this.tableSchema = EntityTableSchemaCreator.create(messageClass);
        this.defaultConstructor = ClassUtils.getEmptyConstructor(messageClass);
        List<FieldDescriptor> fieldDescriptors = getNonTransientDescriptors(messageClass);
        // Verify schemas.
        if (fieldDescriptors.size() != tableSchema.getColumns().size()) {
            throw new IllegalArgumentException("Failed to crate TypeInfo for class: " + messageClass.getName());
        }
        this.columns = new ArrayList<>(tableSchema.getColumns().size());
        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            columns.add(new Column(tableSchema.getColumns().get(i), fieldDescriptors.get(i)));
        }
    }

    static <T> List<FieldDescriptor> getNonTransientDescriptors(Class<T> messageClass) {
        var fields = ClassUtils.getAllDeclaredFields(messageClass);
        ClassUtils.setFieldsAccessibleToTrue(fields);
        var descriptors = FieldDescriptor.create(fields);
        return descriptors.stream()
                .filter(descriptor -> !descriptor.isTransient())
                .collect(Collectors.toList());
    }

    public T createInstance() {
        try {
            return defaultConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to create instance of " + messageClass.getName(), e);
        }
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Class<T> getMessageClass() {
        return messageClass;
    }

    public static class Column {
        final ColumnSchema schema;
        final FieldDescriptor fieldDescr;
        final boolean isProtobuf;

        Column(ColumnSchema schema, FieldDescriptor fieldDescr) {
            this.schema = schema;
            this.fieldDescr = fieldDescr;
            this.isProtobuf = MessageLite.class.isAssignableFrom(fieldDescr.getField().getType());
        }

        public <T> void set(T instance, @Nullable Object columnValue) {
            try {
                fieldDescr.getField().set(instance, columnValue);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Failed to set columnValue to filed: " + fieldDescr.getField().getName()
                        , e
                );
            }
        }

        @SuppressWarnings("unchecked")
        public <T, V> @Nullable V get(T instance) {
            try {
                return (V) fieldDescr.getField().get(instance);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Failed to get columnValue from filed: " + fieldDescr.getField().getName(),
                        e
                );
            }
        }

        public ColumnSchema getSchema() {
            return schema;
        }

        public FieldDescriptor getDescriptor() {
            return fieldDescr;
        }
    }
}
