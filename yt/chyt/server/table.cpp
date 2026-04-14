#include "table.h"

#include "host.h"
#include "table_schema_cache.h"
#include "query_context.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYTree;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

namespace {

void FetchTableSchemas(TQueryContext* queryContext, const std::vector<TTablePtr>& tables)
{
    const auto& client = queryContext->Client();
    // TableSchemaCache may be not configured.
    const auto& tableSchemaCache = queryContext->Host->GetTableSchemaCache();

    THashMap<TGuid, std::vector<TTablePtr>> schemaIdToTables;
    for (const auto& table : tables) {
        schemaIdToTables[table->SchemaId].push_back(table);
    }

    THashMap<TCellTag, std::vector<TGuid>> cellTagToSchemaIds;
    // Iterate over schemaIds and filter those that are not in the cache and must be additionally fetched.
    // Group missed schemaIds by externall cell of table.
    for (auto& [schemaId, tablesWithIdenticalSchema] : schemaIdToTables) {
        auto schema = tableSchemaCache ? tableSchemaCache->Get(schemaId) : nullptr;
        if (!schema) {
            const auto& table = schemaIdToTables[schemaId].front();
            cellTagToSchemaIds[table->ExternalCellTag].push_back(schemaId);
            continue;
        }

        for (const auto& table : tablesWithIdenticalSchema) {
            table->Schema = schema;
        }
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    asyncResults.reserve(cellTagToSchemaIds.size());
    for (const auto& [cellTag, cellSchemaIds] : cellTagToSchemaIds) {
        auto proxy = CreateObjectServiceReadProxy(client, NApi::EMasterChannelKind::Follower, cellTag);
        auto batchReq = proxy.ExecuteBatch();

        // NB: Schemas can be accessed by ID without the use of transactions, thus no need to specify one here.
        for (const auto& schemaId : cellSchemaIds) {
            auto req = TTableYPathProxy::Get(FromObjectId(schemaId));
            const auto& table = schemaIdToTables[schemaId].front();
            AddCellTagToSyncWith(req, table->ObjectId);

            req->Tag() = schemaId;
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
    }

    auto checkError = [] (const auto& error) {
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error fetching table schemas");
    };

    auto result = WaitFor(AllSucceeded(asyncResults));
    checkError(result);

    for (const auto& batchRsp : result.Value()) {
        checkError(GetCumulativeError(batchRsp));
        for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            auto ysonSchema = NYson::TYsonString(rsp->value());
            auto schemaId = std::any_cast<TGuid>(rsp->Tag());
            auto schema = ConvertTo<TTableSchemaPtr>(ysonSchema);
            for (const auto& table : schemaIdToTables[schemaId]) {
                table->Schema = schema;
            }

            if (tableSchemaCache) {
                tableSchemaCache->Insert(schemaId, std::move(schema));
            }
        }
    }

    for (const auto& table : tables) {
        table->Comparator = table->Schema->ToComparator();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TTable)

TTable::TTable(TRichYPath path, const IAttributeDictionaryPtr& attributes)
    : TUserObject(std::move(path))
{
    ObjectId = attributes->Get<TObjectId>("id");
    Type = TypeFromId(ObjectId);
    Dynamic = attributes->Get<bool>("dynamic", false);
    ExternalCellTag = attributes->Get<bool>("external")
        ? attributes->Get<TCellTag>("external_cell_tag")
        : CellTagFromId(ObjectId);
    ChunkCount = attributes->Get<i64>("chunk_count", 0);
    Revision = attributes->Get<NHydra::TRevision>("revision");
    RowCount = attributes->Find<i64>("row_count");

    SchemaId = attributes->Get<TObjectId>("schema_id", NullObjectId);
    Schema = attributes->Find<TTableSchemaPtr>("schema");
    if (Schema) {
        Comparator = Schema->ToComparator();
    }
}

bool TTable::IsSortedDynamic() const
{
    return Dynamic && Schema->IsSorted();
}

bool TTable::IsOrderedDynamic() const
{
    return Dynamic && !Schema->IsSorted();
}

void FormatValue(TStringBuilderBase* builder, const TTablePtr& table, TStringBuf spec)
{
    FormatValue(builder, table->Path, spec);
}

////////////////////////////////////////////////////////////////////////////////

void RemoveIncompatibleSortOrder(TTablePtr& table)
{
    const auto& schema = table->Schema;

    auto hasIncompatibleSortOrder = [] (const TColumnSchema& column) -> bool {
        if (column.SortOrder()) {
            // ESortOrder::Descending is not supported in ClickHouse.
            if (column.SortOrder() != ESortOrder::Ascending) {
                return true;
            }
            // We convert 'any' values to yson-strings, so sort order is broken.
            if (*column.LogicalType() == *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))) {
                return true;
            }
        }
        return false;
    };

    bool foundIncompatibleSortOrder = false;
    for (const auto& column : schema->Columns()) {
        if (hasIncompatibleSortOrder(column)) {
            foundIncompatibleSortOrder = true;
            break;
        }
    }

    // Fast path.
    if (!foundIncompatibleSortOrder) {
        return;
    }

    auto columns = schema->Columns();
    foundIncompatibleSortOrder = false;

    for (auto& column : columns) {
        if (hasIncompatibleSortOrder(column)) {
            foundIncompatibleSortOrder = true;
        }
        // Delete sort order from all columns after incompatible one.
        if (foundIncompatibleSortOrder) {
            column.SetSortOrder(std::nullopt);
        }
    }

    table->Schema = New<TTableSchema>(std::move(columns), schema->IsStrict(), /* uniqueKeys*/ false);
    table->Comparator = table->Schema->ToComparator();
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> FetchTables(
    TQueryContext* queryContext,
    const std::vector<TRichYPath>& richPaths,
    bool skipUnsuitableNodes,
    bool ignoreFetchErrors,
    bool enableDynamicStoreRead,
    TLogger logger)
{
    const auto& Logger = logger;

    YT_LOG_INFO("Fetching tables (PathCount: %v)", richPaths.size());

    std::vector<TYPath> paths;
    paths.reserve(richPaths.size());
    for (const auto& path: richPaths) {
        paths.emplace_back(path.GetPath());
    }

    auto attributesOrErrors = queryContext->GetObjectAttributesSnapshot(paths);

    int dynamicTableCount = 0;

    std::vector<TTablePtr> tables;
    std::vector<TError> errors;
    std::vector<TError> unsuitableErrors;
    for (int index = 0; index < static_cast<int>(richPaths.size()); ++index) {
        const auto& path = richPaths[index];
        const auto& attributesOrError = attributesOrErrors[index];

        if (attributesOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            unsuitableErrors.emplace_back(std::move(attributesOrError).Wrap());
            YT_LOG_DEBUG("Skip %Qv because it wasn't resolved", path);
            continue;
        }

        if (!attributesOrError.IsOK()) {
            errors.emplace_back(TError("Error fetching table %v", path)
                << std::move(attributesOrError).Wrap()
                << TErrorAttribute("path", path));
            continue;
        }

        const auto& attributes = attributesOrError.Value();
        auto type = attributes->Get<EObjectType>("type", EObjectType::Null);
        static THashSet<EObjectType> allowedTypes = {EObjectType::Table};
        if (!allowedTypes.contains(type)) {
            unsuitableErrors.emplace_back(TError("Path %Qv does not correspond to a table; expected one of types %Qlv, actual type %Qlv",
                path,
                allowedTypes,
                type));
            YT_LOG_DEBUG("Skip %Qv because it's not a table", path);
            continue;
        }

        if (attributes->Get<bool>("dynamic", false) &&
            enableDynamicStoreRead && !attributes->Get<bool>("enable_dynamic_store_read", false))
        {
            unsuitableErrors.emplace_back(TError(
                "Dynamic store read for table %Qv is disabled; in order to read dynamic stores, "
                "set attribute \"enable_dynamic_store_read\" to true and remount table; "
                "if you indeed want to read only static part of dynamic table, "
                "pass setting chyt.dynamic_table.enable_dynamic_store_read = 0",
                path.GetPath()));
            YT_LOG_DEBUG("Skip %Qv because it has disabled dynamic store read when required", path);
            continue;
        }

        auto& table = tables.emplace_back(New<TTable>(path, attributes));
        if (table->Dynamic) {
            ++dynamicTableCount;
        }

        if (auto it = queryContext->SnapshotLocks.find(path.GetPath()); it != queryContext->SnapshotLocks.end()) {
            table->ExternalTransactionId = it->second.ExternalTransactionId;
        }
    }

    if (ignoreFetchErrors) {
        errors.clear();
    }

    if (!skipUnsuitableNodes) {
        std::move(unsuitableErrors.begin(), unsuitableErrors.end(), std::back_inserter(errors));
        unsuitableErrors.clear();
    }

    auto throwOnErrors = [&] {
        if (!errors.empty()) {
            // CH drops the error below, so log it.
            auto error = TError("Table fetching failed")
                << errors;
            YT_LOG_DEBUG(error, "Table fetching failed");
            THROW_ERROR error;
        }
    };

    throwOnErrors();

    YT_LOG_INFO("Tables fetched (SkippedCount: %v)", richPaths.size() - tables.size());

    if (queryContext->Host->GetConfig()->EnableSchemaIdFetching) {
        FetchTableSchemas(queryContext, tables);
        YT_LOG_INFO("Table schemas fetched");
    }

    if (dynamicTableCount) {
        // Let's fetch table mount infos.
        YT_LOG_INFO("Fetching table mount infos (TableCount: %v)", dynamicTableCount);
        const auto& connection = queryContext->Client()->GetNativeConnection();
        const auto& tableMountCache = connection->GetTableMountCache();
        std::vector<TFuture<void>> asyncResults;
        for (auto& table : tables) {
            if (table->Dynamic) {
                asyncResults.emplace_back(tableMountCache->GetTableInfo(table->GetPath())
                    .Apply(BIND([&] (const TErrorOr<TTableMountInfoPtr>& errorOrMountInfo) {
                        table->TableMountInfo = errorOrMountInfo.ValueOrThrow();
                    })));
            }
        }
        errors = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();
        auto it = std::remove_if(errors.begin(), errors.end(), [] (TError error) { return error.IsOK(); });
        errors.erase(it, errors.end());
        YT_LOG_INFO("Table mount infos fetched");
    }

    throwOnErrors();

    for (auto& table : tables) {
        RemoveIncompatibleSortOrder(table);
    }

    for (const auto& table : tables) {
        YT_LOG_TRACE(
            "Fetched table (Path: %v, Revision: %v, Columns: %v)",
            table->Path,
            table->Revision,
            MakeShrunkFormattableView(table->Schema->GetColumnNames(), TDefaultFormatter(), 5));
    }

    return tables;
}

std::vector<TTablePtr> FetchTablesSoft(
    TQueryContext* queryContext,
    const std::vector<TRichYPath>& richPaths,
    bool skipUnsuitableNodes,
    bool enableDynamicStoreRead,
    TLogger logger)
{
    return FetchTables(
        queryContext,
        richPaths,
        skipUnsuitableNodes,
        /*ignoreFetchErrors*/ false,
        enableDynamicStoreRead,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
