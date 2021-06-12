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
    switch (schema.GetPhysicalType()) {
        case EValueType::Int64:
            return CreateUnversionedInt64ColumnReader(meta, columnIndex, columnId, sortOrder);

        case EValueType::Uint64:
            return CreateUnversionedUint64ColumnReader(meta, columnIndex, columnId, sortOrder);

        case EValueType::Double:
            switch (auto simplifiedLogicalType = schema.CastToV1Type()) {
                case ESimpleLogicalValueType::Float:
                    return CreateUnversionedFloatingPointColumnReader<float>(meta, columnIndex, columnId, sortOrder);
                default:
                    YT_VERIFY(simplifiedLogicalType == ESimpleLogicalValueType::Double);
                    return CreateUnversionedFloatingPointColumnReader<double>(meta, columnIndex, columnId, sortOrder);
            }
        case EValueType::String:
            return CreateUnversionedStringColumnReader(meta, columnIndex, columnId, sortOrder);

        case EValueType::Boolean:
            return CreateUnversionedBooleanColumnReader(meta, columnIndex, columnId, sortOrder);

        case EValueType::Any:
            if (schema.IsOfV1Type()) {
                return CreateUnversionedAnyColumnReader(meta, columnIndex, columnId, sortOrder);
            } else {
                return CreateUnversionedComplexColumnReader(meta, columnIndex, columnId, sortOrder);
            }

        case EValueType::Null:
            return CreateUnversionedNullColumnReader(meta, columnIndex, columnId, sortOrder);

        default:
            ThrowUnexpectedValueType(schema.GetPhysicalType());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const TColumnSchema& columnSchema,
    const TColumnMeta& meta,
    int columnId)
{
    auto simplifiedLogicalType = columnSchema.CastToV1Type();
    switch (columnSchema.GetPhysicalType()) {
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

        default:
            ThrowUnexpectedValueType(columnSchema.GetPhysicalType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat