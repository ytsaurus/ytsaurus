package ru.yandex.yt.ytclient.object;

import java.util.Collection;
import java.util.Map;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeNullSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionalSerializer;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class MappedRowSerializer<T> extends YTreeWireRowSerializer<T> {
    private MappedRowSerializer(YTreeRowSerializer<T> objectSerializer) {
        super(objectSerializer);
        this.tableSchema = asTableSchema(objectSerializer.getFieldMap());
        this.delegate = new YTreeWireRowSerializer.YTreeConsumerProxy(tableSchema);
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
