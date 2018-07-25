#include "helpers.h"
#include "schema.h"
#include "name_table.h"

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void YTreeNodeToUnversionedValue(TUnversionedOwningRowBuilder* builder, const INodePtr& value, int id, bool aggregate)
{
    switch (value->GetType()) {
        case ENodeType::Entity:
            builder->AddValue(MakeUnversionedSentinelValue(EValueType::Null, id, aggregate));
            break;
        case ENodeType::Int64:
            builder->AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id, aggregate));
            break;
        case ENodeType::Uint64:
            builder->AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id, aggregate));
            break;
        case ENodeType::Double:
            builder->AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id, aggregate));
            break;
        case ENodeType::String:
            builder->AddValue(MakeUnversionedStringValue(value->GetValue<TString>(), id, aggregate));
            break;
        default:
            builder->AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).GetData(), id, aggregate));
            break;
    }
}

TUnversionedOwningRow YsonToSchemafulRow(
    const TString& yson,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull)
{
    auto nameTable = TNameTable::FromSchema(tableSchema);

    auto rowParts = ConvertTo<THashMap<TString, INodePtr>>(
        TYsonString(yson, EYsonType::MapFragment));

    TUnversionedOwningRowBuilder rowBuilder;
    auto addValue = [&] (int id, INodePtr value) {
        if (value->GetType() == ENodeType::Entity) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(
                value->Attributes().Get<EValueType>("type", EValueType::Null), id));
            return;
        }

        switch (tableSchema.Columns()[id].GetPhysicalType()) {
            case EValueType::Boolean:
                rowBuilder.AddValue(MakeUnversionedBooleanValue(value->GetValue<bool>(), id));
                break;
            case EValueType::Int64:
                rowBuilder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                break;
            case EValueType::Uint64:
                rowBuilder.AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id));
                break;
            case EValueType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                break;
            case EValueType::String:
                rowBuilder.AddValue(MakeUnversionedStringValue(value->GetValue<TString>(), id));
                break;
            case EValueType::Any:
                rowBuilder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).GetData(), id));
                break;
            default:
                Y_UNREACHABLE();
        }
    };

    const auto& keyColumns = tableSchema.GetKeyColumns();

    // Key
    for (int id = 0; id < static_cast<int>(keyColumns.size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it == rowParts.end()) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        } else {
            addValue(id, it->second);
        }
    }

    // Fixed values
    for (int id = static_cast<int>(keyColumns.size()); id < static_cast<int>(tableSchema.Columns().size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        } else if (treatMissingAsNull) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    // Variable values
    for (const auto& pair : rowParts) {
        int id = nameTable->GetIdOrRegisterName(pair.first);
        if (id >= tableSchema.Columns().size()) {
            YTreeNodeToUnversionedValue(&rowBuilder, pair.second, id, false);
        }
    }

    return rowBuilder.FinishRow();
}

TUnversionedOwningRow YsonToSchemalessRow(const TString& valueYson)
{
    TUnversionedOwningRowBuilder builder;

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (const auto& value : values) {
        int id = value->Attributes().Get<int>("id");
        bool aggregate = value->Attributes().Find<bool>("aggregate").Get(false);
        YTreeNodeToUnversionedValue(&builder, value, id, aggregate);
    }

    return builder.FinishRow();
}

TVersionedRow YsonToVersionedRow(
    const TRowBufferPtr& rowBuffer,
    const TString& keyYson,
    const TString& valueYson,
    const std::vector<TTimestamp>& deleteTimestamps,
    const std::vector<TTimestamp>& extraWriteTimestamps)
{
    TVersionedRowBuilder builder(rowBuffer);

    auto keys = ConvertTo<std::vector<INodePtr>>(TYsonString(keyYson, EYsonType::ListFragment));

    for (auto key : keys) {
        int id = key->Attributes().Get<int>("id");
        switch (key->GetType()) {
            case ENodeType::Int64:
                builder.AddKey(MakeUnversionedInt64Value(key->GetValue<i64>(), id));
                break;
            case ENodeType::Uint64:
                builder.AddKey(MakeUnversionedUint64Value(key->GetValue<ui64>(), id));
                break;
            case ENodeType::Double:
                builder.AddKey(MakeUnversionedDoubleValue(key->GetValue<double>(), id));
                break;
            case ENodeType::String:
                builder.AddKey(MakeUnversionedStringValue(key->GetValue<TString>(), id));
                break;
            default:
                Y_UNREACHABLE();
                break;
        }
    }

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (auto value : values) {
        int id = value->Attributes().Get<int>("id");
        auto timestamp = value->Attributes().Get<TTimestamp>("ts");
        bool aggregate = value->Attributes().Find<bool>("aggregate").Get(false);
        switch (value->GetType()) {
            case ENodeType::Entity:
                builder.AddValue(MakeVersionedSentinelValue(EValueType::Null, timestamp, id, aggregate));
                break;
            case ENodeType::Int64:
                builder.AddValue(MakeVersionedInt64Value(value->GetValue<i64>(), timestamp, id, aggregate));
                break;
            case ENodeType::Uint64:
                builder.AddValue(MakeVersionedUint64Value(value->GetValue<ui64>(), timestamp, id, aggregate));
                break;
            case ENodeType::Double:
                builder.AddValue(MakeVersionedDoubleValue(value->GetValue<double>(), timestamp, id, aggregate));
                break;
            case ENodeType::String:
                builder.AddValue(MakeVersionedStringValue(value->GetValue<TString>(), timestamp, id, aggregate));
                break;
            default:
                builder.AddValue(MakeVersionedAnyValue(ConvertToYsonString(value).GetData(), timestamp, id, aggregate));
                break;
        }
    }

    for (auto timestamp : deleteTimestamps) {
        builder.AddDeleteTimestamp(timestamp);
    }

    for (auto timestamp : extraWriteTimestamps) {
        builder.AddWriteTimestamp(timestamp);
    }

    return builder.FinishRow();
}

TUnversionedOwningRow YsonToKey(const TString& yson)
{
    TUnversionedOwningRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        TYsonString(yson, EYsonType::ListFragment));

    for (int id = 0; id < keyParts.size(); ++id) {
        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            case ENodeType::Int64:
                keyBuilder.AddValue(MakeUnversionedInt64Value(
                    keyPart->GetValue<i64>(),
                    id));
                break;
            case ENodeType::Uint64:
                keyBuilder.AddValue(MakeUnversionedUint64Value(
                    keyPart->GetValue<ui64>(),
                    id));
                break;
            case ENodeType::Double:
                keyBuilder.AddValue(MakeUnversionedDoubleValue(
                    keyPart->GetValue<double>(),
                    id));
                break;
            case ENodeType::String:
                keyBuilder.AddValue(MakeUnversionedStringValue(
                    keyPart->GetValue<TString>(),
                    id));
                break;
            case ENodeType::Entity:
                keyBuilder.AddValue(MakeUnversionedSentinelValue(
                    keyPart->Attributes().Get<EValueType>("type", EValueType::Null),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeUnversionedAnyValue(
                    ConvertToYsonString(keyPart).GetData(),
                    id));
                break;
        }
    }

    return keyBuilder.FinishRow();
}

TString KeyToYson(TUnversionedRow row)
{
    return ConvertToYsonString(row, EYsonFormat::Text).GetData();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
