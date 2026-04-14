#include "helpers.h"

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NComplexTypes;
using namespace NKafka;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsKafkaQueue(const TTableSchemaPtr& schema)
{
    static const THashMap<std::string, EValueType> KafkaQueueColumns = {
        {"key", EValueType::String},
        {"value", EValueType::String},
    };

    for (const auto& column : schema->Columns()) {
        if (column.Name().starts_with(SystemColumnNamePrefix)) {
            continue;
        }

        auto kafkaQueueColumnIt = KafkaQueueColumns.find(column.Name());
        if (kafkaQueueColumnIt == KafkaQueueColumns.end() || kafkaQueueColumnIt->second != column.GetWireType()) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TRecordBatch> ConvertKafkaQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    auto nameTable = rowset->GetNameTable();
    auto keyColumnId = nameTable->FindId("key");
    YT_VERIFY(keyColumnId.has_value());
    auto valueColumnId = nameTable->FindId("value");
    YT_VERIFY(valueColumnId.has_value());

    auto rowIndexColumnId = nameTable->FindId(RowIndexColumnName);
    YT_VERIFY(rowIndexColumnId.has_value());

    auto timestampColumnId = nameTable->FindId(TimestampColumnName);

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.empty());

    TRecordBatch recordBatch;
    recordBatch.MagicByte = 2;
    recordBatch.BaseOffset = rowset->GetStartOffset();

    auto& records = recordBatch.Records;
    records.reserve(rows.size());

    std::optional<i64> firstTimestamp;
    std::optional<i64> maxTimestamp;

    for (const auto& row : rows) {
        std::optional<i64> rowTimestamp;
        if (!row) {
            records.emplace_back();
        } else {
            // TODO(nadya73): Handle nulls.
            auto offsetValue = row[*rowIndexColumnId];
            YT_VERIFY(offsetValue.Type == EValueType::Int64);
            auto key = row[*keyColumnId];
            if (key.Type != EValueType::String) {
                return TError("Row with offset %Qv has key that is not a string", offsetValue.Data.Uint64);
            }
            auto value = row[*valueColumnId];
            if (value.Type != EValueType::String) {
                return TError("Row with offset %Qv has value that is not a string", offsetValue.Data.Uint64);
            }
            records.push_back({
                .OffsetDelta = static_cast<i32>(offsetValue.Data.Uint64 - recordBatch.BaseOffset),
                .Key = key.AsString(),
                .Value = value.AsString(),
            });
            if (timestampColumnId) {
                rowTimestamp = row[*timestampColumnId].Data.Uint64 / 1'000;  // Convert to milliseconds.

                if (!firstTimestamp) {
                    firstTimestamp = rowTimestamp;
                }
                maxTimestamp = std::max(maxTimestamp.value_or(0), *rowTimestamp);
            }
        }
        if (rowTimestamp && firstTimestamp) {
            records.back().TimestampDelta = *rowTimestamp - *firstTimestamp;
        }
    }

    recordBatch.LastOffsetDelta = records.back().OffsetDelta;

    if (firstTimestamp) {
        recordBatch.FirstTimestamp = *firstTimestamp;
    }
    if (maxTimestamp) {
        recordBatch.MaxTimestamp = *maxTimestamp;
    }

    return recordBatch;
}

TErrorOr<TRecordBatch> ConvertGenericQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    TRecordBatch recordBatch;
    recordBatch.MagicByte = 2;
    auto& records = recordBatch.Records;

    auto nameTable = rowset->GetNameTable();
    auto schema = rowset->GetSchema();

    auto rowIndexColumnId = nameTable->FindId(RowIndexColumnName);
    YT_VERIFY(rowIndexColumnId.has_value());

    THashMap<int, TYsonServerToClientConverter> columnConverters;
    for (const auto& column : schema->Columns()) {
        if (IsV3Composite(column.LogicalType())) {
            auto id = nameTable->GetIdOrThrow(column.Name());
            TComplexTypeFieldDescriptor descriptor(column.Name(), column.LogicalType());
            if (auto converter = CreateYsonServerToClientConverter(descriptor, /*config*/ {})) {
                columnConverters.emplace(id, std::move(converter));
            }
        }
    }

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.empty());
    recordBatch.BaseOffset = rowset->GetStartOffset();

    TBlobOutput blobOutput;
    auto writer = TYsonWriter(
        &blobOutput,
        EYsonFormat::Text,
        EYsonType::Node,
        /*enableRaw*/ true);

    records.reserve(rows.size());

    std::optional<i64> firstTimestamp;
    std::optional<i64> maxTimestamp;

    int columnCount = schema->GetColumnCount();
    for (auto row : rows) {
        std::optional<i64> rowTimestamp;

        if (!row) {
            writer.OnEntity();
        } else {
            YT_VERIFY(static_cast<int>(row.GetCount()) >= columnCount);
            writer.OnBeginMap();
            for (int index = 0; index < columnCount; ++index) {
                const auto& value = row[index];

                const auto& column = schema->Columns()[index];

                if (column.Name() == TimestampColumnName) {
                    if (value.Type != EValueType::Uint64) {
                        THROW_ERROR_EXCEPTION("Unexpected type of timestamp column")
                            << TErrorAttribute("actual_type", value.Type)
                            << TErrorAttribute("expected_type", EValueType::Uint64);
                    }
                    rowTimestamp = static_cast<i64>(value.Data.Uint64 / 1'000); // Convert to milliseconds.

                    if (!firstTimestamp) {
                        firstTimestamp = *rowTimestamp;
                    }
                    maxTimestamp = std::max(maxTimestamp.value_or(0), *rowTimestamp);
                }

                if (column.Name().starts_with(SystemColumnNamePrefix)) {
                    continue;
                }

                writer.OnKeyedItem(column.Name());

                switch (value.Type) {
                    case EValueType::Int64:
                        writer.OnInt64Scalar(value.Data.Int64);
                        break;
                    case EValueType::Uint64:
                        writer.OnUint64Scalar(value.Data.Uint64);
                        break;
                    case EValueType::Double:
                        writer.OnDoubleScalar(value.Data.Double);
                        break;
                    case EValueType::Boolean:
                        writer.OnBooleanScalar(value.Data.Boolean);
                        break;
                    case EValueType::String:
                        writer.OnStringScalar(value.AsStringBuf());
                        break;
                    case EValueType::Null:
                        writer.OnEntity();
                        break;
                    case EValueType::Any:
                        writer.OnRaw(value.AsStringBuf(), EYsonType::Node);
                        break;

                    case EValueType::Composite: {
                        if (auto it = columnConverters.find(value.Id); it != columnConverters.end()) {
                            it->second(value, &writer);
                        } else {
                            writer.OnRaw(value.AsStringBuf(), EYsonType::Node);
                        }
                        break;
                    }

                    case EValueType::Min:
                    case EValueType::Max:
                    case EValueType::TheBottom:
                        ThrowUnexpectedValueType(value.Type);
                }
            }
            writer.OnEndMap();
        }

        writer.Flush();
        auto buffer = blobOutput.Flush();

        auto offsetValue = row[*rowIndexColumnId];
        YT_VERIFY(offsetValue.Type == EValueType::Int64);
        TRecord record{
            .OffsetDelta = static_cast<i32>(offsetValue.Data.Int64 - recordBatch.BaseOffset),
            .Value = TString(buffer.data(), buffer.size()),
        };
        if (rowTimestamp && firstTimestamp) {
            record.TimestampDelta = *rowTimestamp - *firstTimestamp;
        }

        records.push_back(std::move(record));
    }

    recordBatch.LastOffsetDelta = records.back().OffsetDelta;

    if (firstTimestamp) {
        recordBatch.FirstTimestamp = *firstTimestamp;
    }
    if (maxTimestamp) {
        recordBatch.MaxTimestamp = *maxTimestamp;
    }

    return recordBatch;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TRecordBatch> ConvertQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    if (IsKafkaQueue(rowset->GetSchema())) {
        return ConvertKafkaQueueRowsToRecordBatch(rowset);
    }

    return ConvertGenericQueueRowsToRecordBatch(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
