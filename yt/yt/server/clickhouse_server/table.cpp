#include "table.h"

#include "host.h"

#include <yt/client/ypath/rich.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/logging/log.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TTable);

TTable::TTable(TRichYPath path, const IAttributeDictionaryPtr& attributes)
    : TUserObject(std::move(path))
{
    ObjectId = attributes->Get<TObjectId>("id");
    Type = TypeFromId(ObjectId);
    Dynamic = attributes->Get<bool>("dynamic");
    ExternalCellTag = attributes->Get<bool>("external")
        ? attributes->Get<ui64>("external_cell_tag")
        : CellTagFromId(ObjectId);
    ChunkCount = attributes->Get<i64>("chunk_count");
    Schema = attributes->Get<TTableSchemaPtr>("schema");
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> FetchTables(
    const NApi::NNative::IClientPtr& client,
    THost* host,
    const std::vector<TRichYPath>& richPaths,
    bool skipUnsuitableNodes,
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

    std::vector<TTablePtr> tables;
    std::vector<TError> errors;
    for (int index = 0; index < static_cast<int>(richPaths.size()); ++index) {
        const auto& path = richPaths[index];
        const auto& attributesOrError = attributesOrErrors[index];

        try {
            const auto& attributes = attributesOrError.ValueOrThrow();
            auto maybeType = attributes->Find<EObjectType>("type");
            if (maybeType != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Path %Qv does not correspond to a table; expected type %Qlv, actual type %Qlv",
                    path,
                    EObjectType::Table,
                    maybeType);
            }
            // COMPAT(max42): remove this when 20.2 is everywhere.
            if (attributes->Get<bool>("dynamic") && !host->GetConfig()->EnableDynamicTables) {
                THROW_ERROR_EXCEPTION(
                    "Table %Qv is dynamic; dynamic tables are not supported yet (CHYT-57)",
                    path.GetPath());
            }
            if (attributes->Get<bool>("dynamic") && !attributes->Get<TTableSchemaPtr>("schema")->IsSorted()) {
                THROW_ERROR_EXCEPTION(
                    "Table %Qv is an ordered dynamic table; they are not supported yet (CHYT-419)",
                    path.GetPath());
            }

            tables.emplace_back(New<TTable>(path, attributes));
        } catch (const std::exception& ex) {
            if (!skipUnsuitableNodes) {
                errors.emplace_back(TError("Error fetching table %v", path)
                    << ex
                    << TErrorAttribute("path", path));
            }
        }
    }

    if (!errors.empty()) {
        THROW_ERROR_EXCEPTION("Table fetching failed")
            << errors;
    }

    YT_LOG_INFO("Tables fetched (SkippedCount: %v)", richPaths.size() - tables.size());

    return tables;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
