package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.yson.UInt64Type;
import tech.ytsaurus.spyt.patch.annotations.Decorate;
import tech.ytsaurus.spyt.patch.annotations.DecoratedMethod;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader")
public class SpecializedGettersReaderDecorator {

    @DecoratedMethod
    public static Object read(
            SpecializedGetters obj,
            int ordinal,
            DataType dataType,
            boolean handleNull,
            boolean handleUserDefinedType) {
        if (handleNull && (obj.isNullAt(ordinal) || dataType instanceof NullType)) {
            return null;
        }
        if (dataType instanceof UInt64Type) {
            return obj.getLong(ordinal);
        }
        return __read(obj, ordinal, dataType, handleNull, handleUserDefinedType);
    }

    public static Object __read(
            SpecializedGetters obj,
            int ordinal,
            DataType dataType,
            boolean handleNull,
            boolean handleUserDefinedType) {
        throw new RuntimeException("Must be replaced with original method");
    }
}
