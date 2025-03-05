#include "helpers.h"

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NComplexTypes;
using namespace NKafka;
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

std::vector<TRecord> ConvertKafkaQueueRowsToRecords(
    const IUnversionedRowsetPtr& rowset)
{
    auto nameTable = rowset->GetNameTable();
    auto keyColumnId = nameTable->FindId("key");
    auto valueColumnId = nameTable->FindId("value");
    YT_VERIFY(keyColumnId && valueColumnId);

    auto rows = rowset->GetRows();

    std::vector<TRecord> records;
    records.reserve(rows.size());

    for (auto [offset, row] : Enumerate(rows)) {
        if (!row) {
            records.emplace_back();
        } else {
            records.push_back({
                .Key = row[*keyColumnId].AsString(),
                .Value = row[*valueColumnId].AsString(),
            });
        }
        records.back().OffsetDelta = static_cast<i32>(offset);
    }

    return records;
}

std::vector<TRecord> ConvertGenericQueueRowsToRecords(
    const IUnversionedRowsetPtr& rowset)
{
    auto nameTable = rowset->GetNameTable();
    auto schema = rowset->GetSchema();

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

    TBlobOutput blobOutput;
    auto writer = TYsonWriter(
        &blobOutput,
        EYsonFormat::Text,
        EYsonType::Node,
        /*enableRaw*/ true);

    std::vector<TRecord> records;
    records.reserve(rows.size());

    int columnCount = schema->GetColumnCount();
    for (auto [offset, row] : Enumerate(rows)) {
        if (!row) {
            writer.OnEntity();
        } else {
            YT_VERIFY(static_cast<int>(row.GetCount()) >= columnCount);
            writer.OnBeginMap();
            for (int index = 0; index < columnCount; ++index) {
                const auto& value = row[index];

                const auto& column = schema->Columns()[index];
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
        records.push_back({
            .OffsetDelta = static_cast<i32>(offset),
            .Value = TString(buffer.data(), buffer.size()),
        });
    }

    return records;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TRecord> ConvertQueueRowsToRecords(
    const IUnversionedRowsetPtr& rowset)
{
    if (IsKafkaQueue(rowset->GetSchema())) {
        return ConvertKafkaQueueRowsToRecords(rowset);
    }

    return ConvertGenericQueueRowsToRecords(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
