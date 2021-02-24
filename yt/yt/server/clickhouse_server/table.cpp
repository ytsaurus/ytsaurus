#include "table.h"

#include "host.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/schema.h>

#include <yt/core/logging/log.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYTree;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TTable);

TTable::TTable(TRichYPath path, const IAttributeDictionaryPtr& attributes)
    : TUserObject(std::move(path))
{
    ObjectId = attributes->Get<TObjectId>("id");
    Type = TypeFromId(ObjectId);
    Dynamic = attributes->Get<bool>("dynamic", false);
    ExternalCellTag = attributes->Get<bool>("external")
        ? attributes->Get<ui64>("external_cell_tag")
        : CellTagFromId(ObjectId);
    ChunkCount = attributes->Get<i64>("chunk_count", 0);
    Revision = attributes->Get<i64>("revision");
    Schema = attributes->Get<TTableSchemaPtr>("schema");
    Comparator = Schema->ToComparator();
}

TString ToString(const TTablePtr& table)
{
    return ToString(table->Path);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> FetchTables(
    const NApi::NNative::IClientPtr& client,
    THost* host,
    const std::vector<TRichYPath>& richPaths,
    bool skipUnsuitableNodes,
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

    auto attributesOrErrors = host->GetObjectAttributes(paths, client);

    int dynamicTableCount = 0;

    std::vector<TTablePtr> tables;
    std::vector<TError> errors;
    for (int index = 0; index < static_cast<int>(richPaths.size()); ++index) {
        const auto& path = richPaths[index];
        const auto& attributesOrError = attributesOrErrors[index];

        try {
            const auto& attributes = attributesOrError.ValueOrThrow();
            auto type = attributes->Get<EObjectType>("type", EObjectType::Null);
            static THashSet<EObjectType> allowedTypes = {EObjectType::Table};
            if (!allowedTypes.contains(type)) {
                THROW_ERROR_EXCEPTION("Path %Qv does not correspond to a table; expected one of types %Qlv, actual type %Qlv",
                    path,
                    allowedTypes,
                    type);
            }
            // COMPAT(max42): remove this when 20.2 is everywhere.
            if (attributes->Get<bool>("dynamic", false) && !host->GetConfig()->EnableDynamicTables) {
                THROW_ERROR_EXCEPTION(
                    "Table %Qv is dynamic; dynamic tables are not supported yet (CHYT-57)",
                    path.GetPath());
            }
            if (attributes->Get<bool>("dynamic", false) && !attributes->Get<TTableSchemaPtr>("schema")->IsSorted()) {
                THROW_ERROR_EXCEPTION(
                    "Table %Qv is an ordered dynamic table; they are not supported yet (CHYT-419)",
                    path.GetPath());
            }
            if (attributes->Get<bool>("dynamic", false) &&
                enableDynamicStoreRead && !attributes->Get<bool>("enable_dynamic_store_read", false))
            {
                THROW_ERROR_EXCEPTION(
                    "Dynamic store read for table %Qv is disabled; in order to read dynamic stores, "
                    "set attribute \"enable_dynamic_store_read\" to true and remount table; "
                    "if you indeed want to read only static part of dynamic table, "
                    "pass setting chyt.dynamic_table.enable_dynamic_store_read = 0",
                    path.GetPath());
            }

            auto& table = tables.emplace_back(New<TTable>(path, attributes));

            if (table->Dynamic) {
                ++dynamicTableCount;
            }
        } catch (const std::exception& ex) {
            if (!skipUnsuitableNodes) {
                errors.emplace_back(TError("Error fetching table %v", path)
                    << ex
                    << TErrorAttribute("path", path));
            }
        }
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

    if (dynamicTableCount) {
        // Let's fetch table mount infos.
        YT_LOG_INFO("Fetching table mount infos (TableCount: %v)", dynamicTableCount);
        const auto& connection = client->GetNativeConnection();
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

    for (const auto& table : tables) {
        YT_LOG_TRACE(
            "Fetched table (Path: %v, Revision: %v, Columns: %v)",
            table->Path,
            table->Revision,
            MakeShrunkFormattableView(table->Schema->GetColumnNames(), TDefaultFormatter(), 5));
    }

    return tables;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
