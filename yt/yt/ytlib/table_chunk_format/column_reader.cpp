#include "column_reader.h"

#include "boolean_column_reader.h"
#include "double_column_reader.h"
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
    int columnId)
{
    auto simplifiedLogicalType = schema.SimplifiedLogicalType();
    if (!simplifiedLogicalType) {
        return CreateUnversionedComplexColumnReader(meta, columnIndex, columnId);
    }

    switch (GetPhysicalType(*simplifiedLogicalType)) {
        case EValueType::Int64:
            return CreateUnversionedInt64ColumnReader(meta, columnIndex, columnId);

        case EValueType::Uint64:
            return CreateUnversionedUint64ColumnReader(meta, columnIndex, columnId);

        case EValueType::Double:
            return CreateUnversionedDoubleColumnReader(meta, columnIndex, columnId);

        case EValueType::String:
            return CreateUnversionedStringColumnReader(meta, columnIndex, columnId);

        case EValueType::Boolean:
            return CreateUnversionedBooleanColumnReader(meta, columnIndex, columnId);

        case EValueType::Any:
            return CreateUnversionedAnyColumnReader(meta, columnIndex, columnId);

        case EValueType::Null:
            return CreateUnversionedNullColumnReader(meta, columnIndex, columnId);

        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(schema.GetPhysicalType());
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const TColumnSchema& schema,
    const TColumnMeta& meta,
    int columnId)
{
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
            return CreateVersionedDoubleColumnReader(
                meta,
                columnId,
                static_cast<bool>(schema.Aggregate()));

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

        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(schema.GetPhysicalType());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
