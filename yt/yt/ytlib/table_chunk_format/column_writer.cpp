#include "column_writer.h"

#include "boolean_column_writer.h"
#include "floating_point_column_writer.h"
#include "integer_column_writer.h"
#include "null_column_writer.h"
#include "string_column_writer.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedColumnWriter(
    int columnIndex,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount)
{
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateUnversionedInt64ColumnWriter(columnIndex, blockWriter, maxValueCount);

        case EValueType::Uint64:
            return CreateUnversionedUint64ColumnWriter(columnIndex, blockWriter, maxValueCount);

        case EValueType::Double:
            switch (columnSchema.CastToV1Type()) {
                case NTableClient::ESimpleLogicalValueType::Float:
                    return CreateUnversionedFloatingPointColumnWriter<float>(columnIndex, blockWriter, maxValueCount);
                default:
                    return CreateUnversionedFloatingPointColumnWriter<double>(columnIndex, blockWriter, maxValueCount);
            }

        case EValueType::String:
            return CreateUnversionedStringColumnWriter(columnIndex, columnSchema, blockWriter, maxValueCount);

        case EValueType::Boolean:
            return CreateUnversionedBooleanColumnWriter(columnIndex, blockWriter);

        case EValueType::Any:
            return CreateUnversionedAnyColumnWriter(columnIndex, columnSchema, blockWriter, maxValueCount);

        case EValueType::Composite:
            return CreateUnversionedCompositeColumnWriter(columnIndex, columnSchema, blockWriter, maxValueCount);

        case EValueType::Null:
            return CreateUnversionedNullColumnWriter(blockWriter);

        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            break;
    }
    ThrowUnexpectedValueType(columnSchema.GetWireType());
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedColumnWriter(
    int columnId,
    const TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount)
{
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateVersionedInt64ColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                maxValueCount);

        case EValueType::Uint64:
            return CreateVersionedUint64ColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                maxValueCount);

        case EValueType::Double:
            switch (auto simplifiedLogicalType = columnSchema.CastToV1Type()) {
                case ESimpleLogicalValueType::Float:
                    return CreateVersionedFloatingPointColumnWriter<float>(
                        columnId,
                        columnSchema,
                        blockWriter,
                        maxValueCount);
                default:
                    return CreateVersionedFloatingPointColumnWriter<double>(
                        columnId,
                        columnSchema,
                        blockWriter,
                        maxValueCount);
            }

        case EValueType::Boolean:
            return CreateVersionedBooleanColumnWriter(
                columnId,
                columnSchema,
                blockWriter);

        case EValueType::Any:
            return CreateVersionedAnyColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                maxValueCount);

        case EValueType::String:
            return CreateVersionedStringColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                maxValueCount);

        case EValueType::Composite:
            return CreateVersionedCompositeColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                maxValueCount);

        default:
            ThrowUnexpectedValueType(columnSchema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
