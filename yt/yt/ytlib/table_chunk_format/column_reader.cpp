#include "column_reader.h"

#include "boolean_column_reader.h"
#include "floating_point_column_reader.h"
#include "integer_column_reader.h"
#include "null_column_reader.h"
#include "string_column_reader.h"

#include <yt/client/table_client/schema.h>

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
    const TColumnSchema& schema,
    const TColumnMeta& meta,
    int columnId)
{
    auto simplifiedLogicalType = schema.CastToV1Type();
    switch (schema.GetPhysicalType()) {
        case EValueType::Int64:
            return CreateVersionedInt64ColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

        case EValueType::Uint64:
            return CreateVersionedUint64ColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

        case EValueType::Double:
            switch (simplifiedLogicalType) {
                case ESimpleLogicalValueType::Float:
                    return CreateVersionedFloatingPointColumnReader<float>(
                        meta,
                        columnId,
                        static_cast<bool>(schema.Aggregate()));
                default:
                    return CreateVersionedFloatingPointColumnReader<double>(
                        meta,
                        columnId,
                        static_cast<bool>(schema.Aggregate()));
            }

        case EValueType::Boolean:
            return CreateVersionedBooleanColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

        case EValueType::String:
            return CreateVersionedStringColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

        case EValueType::Any:
            return CreateVersionedAnyColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

        default:
            ThrowUnexpectedValueType(schema.GetPhysicalType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat