#include "helpers.h"

#include "bootstrap.h"
#include "table.h"
#include "table_schema.h"
#include "type_translation.h"

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/permission.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/logging/log.h>

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

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

KeyCondition CreateKeyCondition(
    const Context& context,
    const SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema)
{
    auto pkExpression = std::make_shared<ExpressionActions>(
        schema.KeyColumns,
        context);

    return KeyCondition(queryInfo, context, schema.PrimarySortColumns, std::move(pkExpression));
}

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

TClickHouseTablePtr FetchClickHouseTableFromCache(
    TBootstrap* bootstrap,
    const NNative::IClientPtr& client,
    const TRichYPath& path,
    const TLogger& Logger)
{
    try {
        YT_LOG_DEBUG("Fetching clickhouse table");

        auto attributes = bootstrap->GetHost()->CheckPermissionsAndGetCachedObjectAttributes({path.GetPath()}, client)[0]
            .ValueOrThrow();

        return std::make_shared<TClickHouseTable>(path, AdaptSchemaToClickHouse(ConvertTo<TTableSchema>(attributes.at("schema"))));
    } catch (TErrorException& ex) {
        if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
            return nullptr;
        }
        throw;
    }
}

TTableSchema ConvertToTableSchema(const ColumnsDescription& columns, const TKeyColumns& keyColumns)
{
    std::vector<TString> columnOrder;
    THashSet<TString> usedColumns;

    for (const auto& keyColumnName : keyColumns) {
        if (!columns.has(keyColumnName)) {
            THROW_ERROR_EXCEPTION("Column %Qv is specified as key column but is missing",
                keyColumnName);
        }
        columnOrder.emplace_back(keyColumnName);
        usedColumns.emplace(keyColumnName);
    }

    for (const auto& column : columns) {
        if (usedColumns.emplace(column.name).second) {
            columnOrder.emplace_back(column.name);
        }
    }

    std::vector<TColumnSchema> columnSchemas;
    columnSchemas.reserve(columnOrder.size());
    for (int index = 0; index < static_cast<int>(columnOrder.size()); ++index) {
        const auto& name = columnOrder[index];
        const auto& column = columns.get(name);
        const auto& type = RepresentClickHouseType(column.type);
        std::optional<ESortOrder> sortOrder;
        if (index < static_cast<int>(keyColumns.size())) {
            sortOrder = ESortOrder::Ascending;
        }
        columnSchemas.emplace_back(name, type, sortOrder);
    }

    return TTableSchema(columnSchemas);
}

TString MaybeTruncateSubquery(TString query)
{
    // TODO(max42): rewrite properly.
    auto begin = query.find("ytSubquery");
    if (begin == TString::npos) {
        return query;
    }
    begin += 10;
    if (begin >= query.size() || query[begin] != '(') {
        return query;
    }
    ++begin;
    auto end = query.find(")", begin);
    if (end == TString::npos) {
        return query;
    }
    return query.substr(0, begin) + "..." + query.substr(end, query.size() - end);
}

TTableSchema AdaptSchemaToClickHouse(const TTableSchema& schema)
{
    std::vector<TColumnSchema> columns;
    columns.reserve(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        ESimpleLogicalValueType type = static_cast<ESimpleLogicalValueType>(column.GetPhysicalType());
        columns.emplace_back(column.Name(), MakeLogicalType(type, column.Required()), column.SortOrder());
    }
    return TTableSchema(std::move(columns), schema.GetStrict(), schema.GetUniqueKeys());
}

TTableSchema GetCommonSchema(const std::vector<TTableSchema>& schemas)
{
    if (schemas.empty()) {
        return TTableSchema();
    }

    THashMap<TString, TColumnSchema> nameToColumn;
    THashMap<TString, size_t> nameCounter;

    for (const auto& column : schemas[0].Columns()) {
        auto [it, _] = nameToColumn.emplace(column.Name(), column);
        // We will set sorted order for key collumns later.
        it->second.SetSortOrder(std::nullopt);
    }

    for (const auto& schema: schemas) {
        for (const auto& column: schema.Columns()) {
            if (auto it = nameToColumn.find(column.Name()); it != nameToColumn.end()) {
                if (it->second.SimplifiedLogicalType() == column.SimplifiedLogicalType()) {
                    ++nameCounter[column.Name()];
                    if (!column.Required() && it->second.Required()) {
                        it->second.SetLogicalType(New<TOptionalLogicalType>(it->second.LogicalType()));
                    }
                }
            }
        }
    }

    std::vector<TColumnSchema> resultColumns;
    resultColumns.reserve(schemas[0].Columns().size());
    for (const auto& column : schemas[0].Columns()) {
        if (auto it = nameCounter.find(column.Name());
            it != nameCounter.end() && it->second == schemas.size())
        {
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
            break;
        }
        resultColumns[index].SetSortOrder(ESortOrder::Ascending);
    }
    return TTableSchema(resultColumns);
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

/////////////////////////////////////////////////////////////////////////////

TString ToString(const IAST& ast)
{
    return TString(DB::serializeAST(ast, true));
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

/////////////////////////////////////////////////////////////////////////////


} // namespace DB

namespace std {

/////////////////////////////////////////////////////////////////////////////

TString ToString(const std::shared_ptr<DB::IAST>& astPtr)
{
    return astPtr ? ToString(*astPtr) : "#";
}

/////////////////////////////////////////////////////////////////////////////

} // namespace std
