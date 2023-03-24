#include "column_reader.h"

#include "boolean_column_reader.h"
#include "floating_point_column_reader.h"
#include "integer_column_reader.h"
#include "null_column_reader.h"
#include "string_column_reader.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedColumnReader(
    const TColumnSchema& schema,
    const TColumnMeta& meta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder)
{
    switch (schema.GetWireType()) {
        case EValueType::Int64:
            return CreateUnversionedInt64ColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        case EValueType::Uint64:
            return CreateUnversionedUint64ColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        case EValueType::Double:
            switch (auto simplifiedLogicalType = schema.CastToV1Type()) {
                case ESimpleLogicalValueType::Float:
                    return CreateUnversionedFloatingPointColumnReader<float>(meta, columnIndex, columnId, sortOrder, schema);
                default:
                    YT_VERIFY(simplifiedLogicalType == ESimpleLogicalValueType::Double);
                    return CreateUnversionedFloatingPointColumnReader<double>(meta, columnIndex, columnId, sortOrder, schema);
            }
        case EValueType::String:
            return CreateUnversionedStringColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        case EValueType::Boolean:
            return CreateUnversionedBooleanColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        case EValueType::Any:
            return CreateUnversionedAnyColumnReader(meta, columnIndex, columnId, sortOrder, schema);
        case EValueType::Composite:
            return CreateUnversionedCompositeColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        case EValueType::Null:
            return CreateUnversionedNullColumnReader(meta, columnIndex, columnId, sortOrder, schema);

        default:
            ThrowUnexpectedValueType(schema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const TColumnSchema& columnSchema,
    const TColumnMeta& meta,
    int columnId)
{
    auto simplifiedLogicalType = columnSchema.CastToV1Type();
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateVersionedInt64ColumnReader(
                meta,
                columnId,
                columnSchema);

        case EValueType::Uint64:
            return CreateVersionedUint64ColumnReader(
                meta,
                columnId,
                columnSchema);

        case EValueType::Double:
            switch (simplifiedLogicalType) {
                case ESimpleLogicalValueType::Float:
                    return CreateVersionedFloatingPointColumnReader<float>(
                        meta,
                        columnId,
                        columnSchema);
                default:
                    return CreateVersionedFloatingPointColumnReader<double>(
                        meta,
                        columnId,
                        columnSchema);
            }

        case EValueType::Boolean:
            return CreateVersionedBooleanColumnReader(
                meta,
                columnId,
                columnSchema);

        case EValueType::String:
            return CreateVersionedStringColumnReader(
                meta,
                columnId,
                columnSchema);

        case EValueType::Any:
            return CreateVersionedAnyColumnReader(
                meta,
                columnId,
                columnSchema);

        case EValueType::Composite:
            return CreateVersionedCompositeColumnReader(
                meta,
                columnId,
                columnSchema);

        default:
            ThrowUnexpectedValueType(columnSchema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
