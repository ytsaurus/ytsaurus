#include "helpers.h"

#include "table.h"
#include "table_schema.h"
#include "type_translation.h"

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

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

std::unique_ptr<TTableObject> GetTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    EPermission permission,
    const TLogger& logger)
{
    auto Logger = NLogging::TLogger(logger)
        .AddTag("Path: %v", path);

    auto userObject = std::make_unique<TTableObject>();
    userObject->Path = path;

    // TODO(max42): YT-10402, columnar ACL
    GetUserObjectBasicAttributes(
        client,
        {userObject.get()},
        NullTransactionId,
        Logger,
        permission,
        TGetUserObjectBasicAttributesOptions{
            .SuppressAccessTracking = true,
            .ChannelKind = EMasterChannelKind::Cache
        });

    if (userObject->Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            userObject->Type);
    }

    YT_LOG_INFO("Requesting extended table attributes");

    {
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, userObject->ExternalCellTag);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(userObject->GetObjectIdPath() + "/@");
        AddCellTagToSyncWith(req, userObject->ObjectId);
        SetSuppressAccessTracking(req, true);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "schema",
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        userObject->ChunkCount = attributes->Get<int>("chunk_count");
        userObject->Dynamic = attributes->Get<bool>("dynamic");
        userObject->Schema = attributes->Get<TTableSchema>("schema");
    }

    return userObject;
}

TClickHouseTablePtr FetchClickHouseTable(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    const TLogger& logger)
{
    try {
        auto userObject = GetTableAttributes(
            client,
            path,
            EPermission::Read,
            logger);

        return std::make_shared<TClickHouseTable>(path, userObject->Schema);
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
