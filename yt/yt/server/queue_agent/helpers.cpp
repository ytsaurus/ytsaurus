#include "helpers.h"

#include "private.h"
#include "queue_static_table_exporter.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/federated/client.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NYPath;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueueClient;
using namespace NYTree;

static constexpr auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(
    const TQueueTableRow& row,
    const std::optional<TReplicatedTableMappingTableRow>& replicatedTableMappingRow)
{
    if (!row.RowRevision) {
        return TError("Queue is not in-sync yet");
    }
    if (!row.ObjectType) {
        return TError("Object type is not known yet");
    }
    if (IsReplicatedTableObjectType(row.ObjectType) && !replicatedTableMappingRow) {
        return TError("No replicated table mapping row is known for replicated queue");
    }
    if (replicatedTableMappingRow) {
        replicatedTableMappingRow->Validate();
    }

    if (row.ObjectType == EObjectType::Table || IsReplicatedTableObjectType(row.ObjectType)) {
        // NB: Dynamic and Sorted are optionals.
        // TODO(achulkov2): Support checking chaos_replicated_table sortedness via Cypress synchronizer.
        // We can either add @sorted, or fetch schema and check for all queues.
        if (row.Dynamic == true && (row.Sorted == false || row.ObjectType == EObjectType::ChaosReplicatedTable)) {
            return EQueueFamily::OrderedDynamicTable;
        }
        return TError(
            "Only ordered dynamic tables are supported as queues, "
            "found object of type %Qlv with dynamic=%v and sorted=%v instead",
            row.ObjectType,
            row.Dynamic,
            row.Sorted);
    }

    return TError("Invalid queue object type %Qlv", row.ObjectType);
}

bool IsReplicatedTableObjectType(EObjectType type)
{
    return type == EObjectType::ReplicatedTable || type == EObjectType::ChaosReplicatedTable;
}

bool IsReplicatedTableObjectType(const std::optional<EObjectType>& type)
{
    return type && IsReplicatedTableObjectType(*type);
}

////////////////////////////////////////////////////////////////////////////////

TQueueAgentClientDirectory::TQueueAgentClientDirectory(TClientDirectoryPtr clientDirectory)
    : ClientDirectory_(std::move(clientDirectory))
    , FederationConfig_(New<NClient::NFederated::TFederationConfig>())
{ }

// TODO(achulkov2): Rewrite this with better decomposition and some sort of expiry mechanism.
NApi::IClientPtr TQueueAgentClientDirectory::GetFederatedClient(const std::vector<TRichYPath>& replicas)
{
    if (replicas.empty()) {
        THROW_ERROR_EXCEPTION("Cannot get federated client for an empty list of replicas");
    }

    std::vector<std::string> clusters;
    std::vector<NApi::IClientPtr> replicaClients;
    for (const auto& replica : replicas) {
        const auto& cluster = replica.GetCluster();
        YT_VERIFY(cluster);
        clusters.push_back(*cluster);
        replicaClients.emplace_back(ClientDirectory_->GetClientOrThrow(*cluster));
    }
    std::sort(clusters.begin(), clusters.end());

    auto key = JoinToString(std::move(clusters));

    auto guard = Guard(Lock_);
    auto clientIt = FederatedClients_.find(key);
    if (clientIt != FederatedClients_.end() &&
        !clientIt->second->GetConnection()->IsTerminated())
    {
        return clientIt->second;
    }

    bool emplaced = false;
    auto client = NClient::NFederated::CreateClient(std::move(replicaClients), FederationConfig_);
    if (clientIt == FederatedClients_.end()) {
        emplaced = true;
        clientIt = EmplaceOrCrash(FederatedClients_, key, client);
    } else {
        clientIt->second = client;
    }

    guard.Release();

    YT_LOG_DEBUG("New federated client created (Key: %v, Emplaced: %v)",
        key,
        emplaced);

    return client;
}

std::vector<TRichYPath> GetRelevantReplicas(
    const TReplicatedTableMappingTableRow& row,
    bool onlySyncReplicas,
    bool onlyDataReplicas,
    bool validatePaths)
{
    std::optional<ETableReplicaMode> modeFilter;
    if (onlySyncReplicas) {
        modeFilter = ETableReplicaMode::Sync;
    }

    std::optional<ETableReplicaContentType> contentTypeFilter;
    if (onlyDataReplicas) {
        contentTypeFilter = ETableReplicaContentType::Data;
    }

    auto replicas = row.GetReplicas(modeFilter, contentTypeFilter);

    if (validatePaths) {
        THashSet<std::string> clusters;
        for (const auto& replica : replicas) {
            if (!clusters.insert(*replica.GetCluster()).second) {
                THROW_ERROR_EXCEPTION(
                    "Cannot work with replicated object with two replicas on the same cluster %Qv",
                    *replica.GetCluster());
            }

            if (replica.GetPath() != replicas[0].GetPath()) {
                THROW_ERROR_EXCEPTION(
                    "Cannot work with replicated object with differently named replicas %v and %v",
                    replica.GetPath(),
                    replicas[0].GetPath());
            }
        }
    }

    return replicas;
}

NApi::NNative::IConnectionPtr TQueueAgentClientDirectory::GetNativeConnection(const std::string& cluster) const
{
    // TODO(achulkov2): Make this more efficient by exposing the inner cluster directory from client directory.
    return ClientDirectory_->GetClientOrThrow(cluster)->GetNativeConnection();
}

NApi::NNative::IClientPtr TQueueAgentClientDirectory::GetClientOrThrow(const std::string& cluster) const
{
    return ClientDirectory_->GetClientOrThrow(cluster);
}

TClientDirectoryPtr TQueueAgentClientDirectory::GetUnderlyingClientDirectory() const
{
    return ClientDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client)
{
    auto nativeClient = MakeStrong(dynamic_cast<NApi::NNative::IClient*>(client.Get()));
    YT_VERIFY(nativeClient);
    return nativeClient;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> OptionalSub(const std::optional<i64> lhs, const std::optional<i64> rhs)
{
    if (lhs && rhs) {
        return *lhs - *rhs;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TQueueExportProgressPtr> DoGetQueueExportProgressFromObjectService(
    const IYPathServicePtr& queueService,
    const TRichYPath& queuePath)
{
    auto queueRef = Format("%v:%v", queuePath.GetCluster(), queuePath.GetPath());
    auto exportsPath = Format("/%v/status/exports", ToYPathLiteral(queueRef));
    auto queueExportsYson = WaitFor(AsyncYPathGet(queueService, exportsPath))
        .ValueOrThrow("Get request failed (Queue: %v, ExportsPath: %v)",
            queueRef, exportsPath);

    auto queueExportsNode = ConvertTo<IMapNodePtr>(queueExportsYson);
    if (auto error = queueExportsNode->FindChildValue<TError>("error")) {
        THROW_ERROR_EXCEPTION(*error);
    }

    auto progress = queueExportsNode->FindChildValue<THashMap<TString, TQueueExportProgressPtr>>("progress");
    if (!progress) {
        THROW_ERROR_EXCEPTION("Field \"progress\" of queue status is not present");
    }
    return *progress;
}

TFuture<THashMap<TString, TQueueExportProgressPtr>> GetQueueExportProgressFromObjectService(
    const IYPathServicePtr& queueService,
    const TRichYPath& queuePath,
    const IInvokerPtr& invoker)
{
    return BIND(&DoGetQueueExportProgressFromObjectService,
        queueService,
        queuePath)
        .AsyncVia(invoker)
        .Run()
        .ApplyUnique(BIND([queuePath] (TErrorOr<THashMap<TString, TQueueExportProgressPtr>>&& result) -> TErrorOr<THashMap<TString, TQueueExportProgressPtr>> {
            if (!result.IsOK()) {
                return TError("Failed to get queue exports progress for %v", queuePath) << TError(result);
            }
            return result;
        }));
}

////////////////////////////////////////////////////////////////////////////////

TString TrimProfilingTagValue(const TString& value)
{
    static constexpr int MaxProfilingTagValueLength = 200;

    if (value.size() <= MaxProfilingTagValueLength) {
        return value;
    }

    auto result = value;
    result.resize(MaxProfilingTagValueLength);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
