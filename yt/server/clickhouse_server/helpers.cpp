#include "helpers.h"

#include "schema.h"
#include "table.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/permission.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/logging/log.h>

#include <yt/library/re2/re2.h>

#include <Common/FieldVisitors.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/IAST.h>
#include <Parsers/formatAST.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NLogging;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NApi;
using namespace NConcurrency;
using namespace NYson;
using namespace NRe2;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field)
{
    for (auto* value = row.Begin(); value != row.End(); ) {
        *(field++) = ConvertToField(*(value++));
    }
}

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, int count, DB::Field* field)
{
    auto* value = row.Begin();
    for (int index = 0; index < count; ++index) {
        *(field++) = ConvertToField(*(value++));
    }
}

Field ConvertToField(const NTableClient::TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            return Field();
        case EValueType::Int64:
            return Field(static_cast<Int64>(value.Data.Int64));
        case EValueType::Uint64:
            return Field(static_cast<UInt64>(value.Data.Uint64));
        case EValueType::Double:
            return Field(static_cast<Float64>(value.Data.Double));
        case EValueType::Boolean:
            return Field(static_cast<UInt64>(value.Data.Boolean ? 1 : 0));
        case EValueType::String:
            return Field(value.Data.String, value.Length);
        case EValueType::Any:
            return Field(value.Data.String, value.Length);
        case EValueType::Composite:
            return Field(value.Data.String, value.Length);
        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", value.Type);
    }
}

void ConvertToUnversionedValue(const DB::Field& field, TUnversionedValue* value)
{
    switch (value->Type) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double: {
            memcpy(&value->Data, &field.get<ui64>(), sizeof(value->Data));
            break;
        }
        case EValueType::Boolean: {
            if (field.get<ui64>() > 1) {
                THROW_ERROR_EXCEPTION("Cannot convert value %v to boolean", field.get<ui64>());
            }
            memcpy(&value->Data, &field.get<ui64>(), sizeof(value->Data));
            break;
        }
        case EValueType::String: {
            const auto& str = field.get<std::string>();
            value->Data.String = str.data();
            value->Length = str.size();
            break;
        }
        default: {
            THROW_ERROR_EXCEPTION("Unexpected data type %v", value->Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TString MaybeTruncateSubquery(TString query)
{
    static const auto ytSubqueryRegex = New<TRe2>("ytSubquery\\([^()]*\\)");
    static constexpr const char* replacement = "ytSubquery(...)";
    RE2::GlobalReplace(&query, *ytSubqueryRegex, replacement);
    return query;
}

TTableSchema InferCommonSchema(const std::vector<TTablePtr>& tables, const TLogger& logger)
{
    THashSet<TTableSchema> schemas;
    for (const auto& table : tables) {
        schemas.emplace(table->Schema);
    }

    if (schemas.empty()) {
        return TTableSchema();
    }

    if (schemas.size() == 1) {
        return *schemas.begin();
    }

    const auto& Logger = logger;

    const auto& firstSchema = *schemas.begin();

    THashMap<TString, TColumnSchema> nameToColumn;
    THashMap<TString, size_t> nameCounter;

    for (const auto& column : firstSchema.Columns()) {
        auto [it, _] = nameToColumn.emplace(column.Name(), column);
        // We will set sorted order for key columns later.
        it->second.SetSortOrder(std::nullopt);
    }

    for (const auto& schema : schemas) {
        for (const auto& column : schema.Columns()) {
            if (auto it = nameToColumn.find(column.Name()); it != nameToColumn.end()) {
                if (it->second.SimplifiedLogicalType() == column.SimplifiedLogicalType()) {
                    ++nameCounter[column.Name()];
                    if (!column.Required() && it->second.Required()) {
                        // If at least in one schema the column isn't required, the result common column isn't required too.
                        it->second.SetLogicalType(OptionalLogicalType(it->second.LogicalType()));
                    }
                }
            }
        }
    }

    std::vector<TColumnSchema> resultColumns;
    resultColumns.reserve(firstSchema.Columns().size());
    for (const auto& column : firstSchema.Columns()) {
        if (nameCounter[column.Name()] == schemas.size()) {
            resultColumns.push_back(nameToColumn[column.Name()]);
        }
    }

    for (size_t index = 0; index < resultColumns.size(); ++index) {
        bool isKeyColumn = true;
        for (const auto& schema : schemas) {
            if (schema.Columns().size() <= index) {
                isKeyColumn = false;
                break;
            }
            const auto& column = schema.Columns()[index];
            if (column.Name() != resultColumns[index].Name() || !column.SortOrder()) {
                isKeyColumn = false;
                break;
            }
        }
        if (!isKeyColumn) {
            // Key columns are the prefix of all columns, so all following collumns aren't key.
            break;
        }
        resultColumns[index].SetSortOrder(ESortOrder::Ascending);
    }

    TTableSchema commonSchema(resultColumns);

    YT_LOG_INFO("Common schema inferred (Schemas: %v, CommonSchema: %v)",
        schemas,
        commonSchema);

    return commonSchema;
}

////////////////////////////////////////////////////////////////////////////////

//! Leaves only some of the "significant" profile counters.
THashMap<TString, size_t> GetBriefProfileCounters(const ProfileEvents::Counters& profileCounters)
{
    static const std::vector<ProfileEvents::Event> SignificantEvents = {
        ProfileEvents::Query,
        ProfileEvents::SelectQuery,
        ProfileEvents::InsertQuery,
        ProfileEvents::InsertedRows,
        ProfileEvents::InsertedBytes,
        ProfileEvents::ContextLock,
        ProfileEvents::NetworkErrors,
        ProfileEvents::RealTimeMicroseconds,
        ProfileEvents::UserTimeMicroseconds,
        ProfileEvents::SystemTimeMicroseconds,
        ProfileEvents::SoftPageFaults,
        ProfileEvents::HardPageFaults,
        ProfileEvents::OSIOWaitMicroseconds,
        ProfileEvents::OSCPUWaitMicroseconds,
        ProfileEvents::OSCPUVirtualTimeMicroseconds,
        ProfileEvents::OSReadChars,
        ProfileEvents::OSWriteChars,
        ProfileEvents::OSReadBytes,
        ProfileEvents::OSWriteBytes,
    };

    THashMap<TString, size_t> result;

    for (const auto& event : SignificantEvents) {
        result[CamelCaseToUnderscoreCase(ProfileEvents::getName(event))] = profileCounters[event].load(std::memory_order::memory_order_relaxed);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const IAST& ast)
{
    return NYT::NClickHouseServer::MaybeTruncateSubquery(TString(DB::serializeAST(ast, true)));
}

TString ToString(const NameSet& nameSet)
{
    return NYT::Format("%v", std::vector<TString>(nameSet.begin(), nameSet.end()));
}

void Serialize(const QueryStatusInfo& query, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("query").Value(NYT::NClickHouseServer::MaybeTruncateSubquery(TString(query.query)))
            .Item("elapsed_seconds").Value(query.elapsed_seconds)
            .Item("read_rows").Value(query.read_rows)
            .Item("read_bytes").Value(query.read_bytes)
            .Item("total_rows").Value(query.total_rows)
            .Item("written_rows").Value(query.written_rows)
            .Item("written_bytes").Value(query.written_bytes)
            .Item("memory_usage").Value(query.memory_usage)
            .Item("peak_memory_usage").Value(query.peak_memory_usage)
        .EndMap();
}

void Serialize(const ProcessListForUserInfo& processListForUserInfo, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("memory_usage").Value(processListForUserInfo.memory_usage)
            .Item("peak_memory_usage").Value(processListForUserInfo.peak_memory_usage)
            .Item("brief_profile_counters").Value(NYT::NClickHouseServer::GetBriefProfileCounters(*processListForUserInfo.profile_counters))
        .EndMap();
}

TString ToString(const Block& block)
{
    NYT::TStringBuilder content;
    const auto& columns = block.getColumns();
    content.AppendChar('{');
    for (size_t rowIndex = 0; rowIndex < block.rows(); ++rowIndex) {
        if (rowIndex != 0) {
            content.AppendString(", ");
        }
        content.AppendChar('{');
        for (size_t columnIndex = 0; columnIndex < block.columns(); ++columnIndex) {
            if (columnIndex != 0) {
                content.AppendString(", ");
            }
            const auto& field = (*columns[columnIndex])[rowIndex];
            content.AppendString(applyVisitor(FieldVisitorToString(), field));
        }
        content.AppendChar('}');
    }
    content.AppendChar('}');

    return NYT::Format(
        "{RowCount: %v, ColumnCount: %v, Structure: %v, Content: %v}",
        block.rows(),
        block.columns(),
        block.dumpStructure(),
        content.Flush());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

namespace std {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const std::shared_ptr<DB::IAST>& astPtr)
{
    return astPtr ? ToString(*astPtr) : "#";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace std
