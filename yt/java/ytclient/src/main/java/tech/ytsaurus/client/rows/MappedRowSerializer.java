package tech.ytsaurus.client.rows;

import java.util.Collection;
import java.util.Map;

import tech.ytsaurus.core.rows.YTreeObjectField;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeNullSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionalSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;

public class MappedRowSerializer<T> extends YTreeWireRowSerializer<T> {
    private final boolean supportState;

    private MappedRowSerializer(YTreeRowSerializer<T> objectSerializer) {
        super(objectSerializer);
        this.tableSchema = asTableSchema(objectSerializer.getFieldMap());
        this.delegate = new YTreeWireRowSerializer.YTreeConsumerProxy(tableSchema);
        this.supportState = YTreeStateSupport.class.isAssignableFrom(objectSerializer.getClazz());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serializeRow(T row, WireProtocolWriteable writeable, boolean keyFieldsOnly, boolean aggregate,
                             int[] idMapping) {
        final T compareWith = !keyFieldsOnly && supportState
                ? ((YTreeStateSupport<? extends T>) row).getYTreeObjectState()
                : null;
        this.objectSerializer.serializeRow(row, delegate.wrap(writeable, aggregate), keyFieldsOnly, compareWith);
        delegate.complete();
    }

    public static <T> MappedRowSerializer<T> forClass(YTreeSerializer<T> serializer) {
        if (serializer instanceof YTreeRowSerializer) {
            return new MappedRowSerializer<>((YTreeRowSerializer<T>) serializer);
        }
        throw new RuntimeException("Expected YTreeRowSerializer in MappedRowSerializer.forClass()");
    }

    public static TableSchema asTableSchema(Map<String, YTreeObjectField<?>> fieldMap) {
        final TableSchema.Builder builder = new TableSchema.Builder();
        asTableSchema(builder, fieldMap.values());
        return builder.build();
    }

    public static YTreeSerializer<?> unwrap(YTreeSerializer<?> serializer) {
        if (serializer instanceof YTreeOptionSerializer) {
            return unwrap(((YTreeOptionSerializer<?>) serializer).getDelegate());
        } else if (serializer instanceof YTreeOptionalSerializer) {
            return unwrap(((YTreeOptionalSerializer<?>) serializer).getDelegate());
        } else if (serializer instanceof YTreeNullSerializer) {
            return unwrap(((YTreeNullSerializer<?>) serializer).getDelegate());
        } else {
            return serializer;
        }
    }

    private static void asTableSchema(TableSchema.Builder builder, Collection<YTreeObjectField<?>> fields) {
        boolean hasKeys = false;

        for (YTreeObjectField<?> field : fields) {
            final boolean isKeyField = field.sortOrder != null;
            final YTreeSerializer<?> serializer = unwrap(field.serializer);
            if (field.isFlatten) {
                asTableSchema(builder, serializer.getFieldMap().values());
            } else {
                hasKeys |= isKeyField;

                builder.add(ColumnSchema.builder(field.key, asType(serializer))
                        .setAggregate(field.aggregate)
                        .setSortOrder(field.sortOrder)
                        .build());
            }
        }

        builder.setUniqueKeys(hasKeys);
    }
}
