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
    IMemoryUsageTrackerPtr memoryUsageTracker,
    bool serializeFloatsAsDoubles,
    int maxValueCount)
{
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateUnversionedInt64ColumnWriter(
                columnIndex,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Uint64:
            return CreateUnversionedUint64ColumnWriter(
                columnIndex,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Double:
            switch (columnSchema.CastToV1Type()) {
                case NTableClient::ESimpleLogicalValueType::Float:
                    if (!serializeFloatsAsDoubles) {
                        return CreateUnversionedFloatingPointColumnWriter<float>(
                            columnIndex,
                            blockWriter,
                            std::move(memoryUsageTracker),
                            maxValueCount);
                    }
                    [[fallthrough]];
                default:
                    return CreateUnversionedFloatingPointColumnWriter<double>(
                        columnIndex,
                        blockWriter,
                        std::move(memoryUsageTracker),
                        maxValueCount);
            }

        case EValueType::String:
            return CreateUnversionedStringColumnWriter(
                columnIndex,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Boolean:
            return CreateUnversionedBooleanColumnWriter(
                columnIndex,
                blockWriter,
                std::move(memoryUsageTracker));

        case EValueType::Any:
            return CreateUnversionedAnyColumnWriter(
                columnIndex,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Composite:
            return CreateUnversionedCompositeColumnWriter(
                columnIndex,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

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
    IMemoryUsageTrackerPtr memoryUsageTracker,
    int maxValueCount)
{
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateVersionedInt64ColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Uint64:
            return CreateVersionedUint64ColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Double:
            return CreateVersionedDoubleColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Boolean:
            return CreateVersionedBooleanColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker));

        case EValueType::Any:
            return CreateVersionedAnyColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::String:
            return CreateVersionedStringColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        case EValueType::Composite:
            return CreateVersionedCompositeColumnWriter(
                columnId,
                columnSchema,
                blockWriter,
                std::move(memoryUsageTracker),
                maxValueCount);

        default:
            ThrowUnexpectedValueType(columnSchema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
