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

TTable::TTable(TRichYPath path, const TAttributeMap& attributes)
    : TUserObject(std::move(path))
{
    ObjectId = TObjectId::FromString(attributes.at("id")->GetValue<TString>());
    Type = TypeFromId(ObjectId);
    Dynamic = attributes.at("dynamic")->GetValue<bool>();
    ExternalCellTag = attributes.at("external")->GetValue<bool>()
        ? attributes.at("external_cell_tag")->GetValue<ui64>()
        : CellTagFromId(ObjectId);
    ChunkCount = attributes.at("chunk_count")->GetValue<i64>();
    Schema = ConvertTo<TTableSchemaPtr>(attributes.at("schema"));
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
            std::optional<EObjectType> type;
            if (attributes.contains("type")) {
                type = ConvertTo<EObjectType>(attributes.at("type"));
            }
            if (type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Path %Qv does not correspond to a table; expected type %Qlv, actual type %Qlv",
                    path,
                    EObjectType::Table,
                    type);
            }
            if (attributes.at("dynamic")->GetValue<bool>() && !host->GetConfig()->EnableDynamicTables) {
                THROW_ERROR_EXCEPTION(
                    "Table %Qv is dynamic; dynamic tables are not supported yet (CHYT-57)",
                    path.GetPath());
            }
            if (attributes.at("dynamic")->GetValue<bool>() && !ConvertTo<TTableSchemaPtr>(attributes.at("schema"))->IsSorted()) {
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
