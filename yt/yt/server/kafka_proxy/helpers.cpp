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
    static const THashMap<TString, EValueType> KafkaQueueColumns = {
        {"key", EValueType::String},
        {"value", EValueType::String},
    };

    for (const auto& column : schema->Columns()) {
        if (column.Name().StartsWith(SystemColumnNamePrefix)) {
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

TRecordBatch ConvertKafkaQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    auto nameTable = rowset->GetNameTable();
    auto keyColumnId = nameTable->FindId("key");
    auto valueColumnId = nameTable->FindId("value");
    auto timestampColumnId = nameTable->FindId(TimestampColumnName);
    YT_VERIFY(keyColumnId && valueColumnId);

    const auto& rows = rowset->GetRows();

    TRecordBatch recordBatch;
    recordBatch.MagicByte = 2;
    recordBatch.BaseOffset = rowset->GetStartOffset();

    auto& records = recordBatch.Records;
    records.reserve(rows.size());

    std::optional<i64> firstTimestamp;
    std::optional<i64> maxTimestamp;

    for (auto [offset, row] : Enumerate(rows)) {
        std::optional<i64> rowTimestamp;
        if (!row) {
            records.emplace_back();
        } else {
            records.push_back({
                .Key = row[*keyColumnId].AsString(),
                .Value = row[*valueColumnId].AsString(),
            });
            if (timestampColumnId) {
                rowTimestamp = row[*timestampColumnId].Data.Uint64 / 1'000;  // Convert to milliseconds.

                if (!firstTimestamp) {
                    firstTimestamp = rowTimestamp;
                }
                maxTimestamp = std::max(maxTimestamp.value_or(0), *rowTimestamp);
            }
        }
        records.back().OffsetDelta = static_cast<i32>(offset);
        if (rowTimestamp && firstTimestamp) {
            records.back().TimestampDelta = *rowTimestamp - *firstTimestamp;
        }
    }

    recordBatch.LastOffsetDelta = records.size();

    if (firstTimestamp) {
        recordBatch.FirstTimestamp = *firstTimestamp;
    }
    if (maxTimestamp) {
        recordBatch.MaxTimestamp = *maxTimestamp;
    }

    return recordBatch;
}

TRecordBatch ConvertGenericQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    TRecordBatch recordBatch;
    recordBatch.MagicByte = 2;
    auto& records = recordBatch.Records;

    auto nameTable = rowset->GetNameTable();
    auto schema = rowset->GetSchema();

    THashMap<int, TYsonServerToClientConverter> columnConverters;
    for (const auto& column : schema->Columns()) {
        if (IsV3Composite(column.LogicalType())) {
            auto id = nameTable->GetIdOrThrow(column.Name());
            TComplexTypeFieldDescriptor descriptor(column.Name(), column.LogicalType());
            auto converter = CreateYsonServerToClientConverter(descriptor, /*config*/ {});
            if (converter) {
                columnConverters.emplace(id, std::move(converter));
            }
        }
    }

    auto rows = rowset->GetRows();
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
    for (auto [offset, row] : Enumerate(rows)) {
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
                            << TErrorAttribute("actual_type", ToString(value.Type))
                            << TErrorAttribute("expected_type", ToString(EValueType::Uint64));
                    }
                    rowTimestamp = static_cast<i64>(value.Data.Uint64 / 1'000); // Convert to milliseconds.

                    if (!firstTimestamp) {
                        firstTimestamp = *rowTimestamp;
                    }
                    maxTimestamp = std::max(maxTimestamp.value_or(0), *rowTimestamp);
                }

                if (column.Name().StartsWith(SystemColumnNamePrefix)) {
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

        TRecord record{
            .OffsetDelta = static_cast<i32>(offset),
            .Value = TString(buffer.data(), buffer.size()),
        };
        if (rowTimestamp && firstTimestamp) {
            record.TimestampDelta = *rowTimestamp - *firstTimestamp;
        }

        records.push_back(std::move(record));
    }

    recordBatch.LastOffsetDelta = records.size();
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

TRecordBatch ConvertQueueRowsToRecordBatch(
    const IQueueRowsetPtr& rowset)
{
    if (IsKafkaQueue(rowset->GetSchema())) {
        return ConvertKafkaQueueRowsToRecordBatch(rowset);
    }

    return ConvertGenericQueueRowsToRecordBatch(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
