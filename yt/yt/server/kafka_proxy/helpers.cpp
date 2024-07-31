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

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NComplexTypes;
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

std::vector<NKafka::TMessage> ConvertKafkaQueueRowsToMessages(
    IUnversionedRowsetPtr rowset)
{
    auto nameTable = rowset->GetNameTable();
    auto keyColumnId = nameTable->FindId("key");
    auto valueColumnId = nameTable->FindId("value");
    YT_VERIFY(keyColumnId && valueColumnId);

    const auto& rows = rowset->GetRows();

    std::vector<NKafka::TMessage> messages;
    messages.reserve(rows.size());

    for (const auto& row : rows) {
        if (!row) {
            messages.push_back({
                .Key = "",
                .Value = "",
            });
            continue;
        }

        messages.push_back({
            .Key = row[*keyColumnId].AsString(),
            .Value = row[*valueColumnId].AsString(),
        });
    }

    return messages;
}

std::vector<NKafka::TMessage> ConvertGenericQueueRowsToMessages(
    IUnversionedRowsetPtr rowset)
{
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

    const auto& rows = rowset->GetRows();

    TBlobOutput blobOutput;
    auto writer = TYsonWriter(
        &blobOutput,
        EYsonFormat::Text,
        EYsonType::Node,
        /*enableRaw*/ true);

    std::vector<NKafka::TMessage> messages;
    messages.reserve(rows.size());

    int columnCount = schema->GetColumnCount();
    for (auto row : rows) {
        if (!row) {
            writer.OnEntity();
            continue;
        }

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

        writer.Flush();
        auto buffer = blobOutput.Flush();
        messages.push_back({
            .Key = "",
            .Value = TString(buffer.data(), buffer.size()),
        });
    }

    return messages;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<NKafka::TMessage> ConvertQueueRowsToMessages(
    IUnversionedRowsetPtr rowset)
{
    if (IsKafkaQueue(rowset->GetSchema())) {
        return ConvertKafkaQueueRowsToMessages(rowset);
    }

    return ConvertGenericQueueRowsToMessages(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
