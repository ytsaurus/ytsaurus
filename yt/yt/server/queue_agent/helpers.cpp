#include "helpers.h"

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
        // TODO(achulkov2): Support checking chaos_replicated_table sortedness via cypress synchronizer.
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

    std::vector<TString> clusters;
    std::vector<NApi::IClientPtr> replicaClients;
    for (const auto& replica : replicas) {
        const auto& cluster = replica.GetCluster();
        YT_VERIFY(cluster);
        clusters.push_back(*cluster);
        replicaClients.emplace_back(ClientDirectory_->GetClientOrThrow(*cluster));
    }
    std::sort(clusters.begin(), clusters.end());

    auto key = JoinToString(clusters);
    if (auto* clientPtr = FederatedClients_.FindPtr(key)) {
        return *clientPtr;
    }
    auto client = NClient::NFederated::CreateClient(replicaClients, FederationConfig_);
    FederatedClients_.emplace(key, client);
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
        THashSet<TString> clusters;
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

NApi::NNative::IConnectionPtr TQueueAgentClientDirectory::GetNativeConnection(const TString& cluster) const
{
    // TODO(achulkov2): Make this more efficient by exposing the inner cluster directory from client directory.
    return ClientDirectory_->GetClientOrThrow(cluster)->GetNativeConnection();
}

NApi::NNative::IClientPtr TQueueAgentClientDirectory::GetClientOrThrow(const TString& cluster) const
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

THashMap<int, THashMap<i64, i64>> CollectCumulativeDataWeights(
    const TYPath& path,
    const NApi::IClientPtr& client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const TLogger& logger)
{
    const auto& Logger = logger;

    if (tabletAndRowIndices.empty()) {
        return {};
    }

    TStringBuilder queryBuilder;
    queryBuilder.AppendFormat("[$tablet_index], [$row_index], [$cumulative_data_weight] from [%v] where ([$tablet_index], [$row_index]) in (",
        path);
    bool isFirstTuple = true;
    for (const auto& [partitionIndex, rowIndex] : tabletAndRowIndices) {
        if (!isFirstTuple) {
            queryBuilder.AppendString(", ");
        }
        queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, rowIndex);
        isFirstTuple = false;
    }

    queryBuilder.AppendString(")");

    YT_VERIFY(!isFirstTuple);

    auto query = queryBuilder.Flush();
    TSelectRowsOptions options;
    options.ReplicaConsistency = EReplicaConsistency::Sync;
    YT_LOG_TRACE("Executing query for cumulative data weights (Query: %v)", query);
    auto selectResult = WaitFor(client->SelectRows(query, options))
        .ValueOrThrow();

    THashMap<int, THashMap<i64, i64>> result;

    for (const auto& row : selectResult.Rowset->GetRows()) {
        YT_VERIFY(row.GetCount() == 3);

        auto tabletIndex = FromUnversionedValue<int>(row[0]);
        auto rowIndex = FromUnversionedValue<i64>(row[1]);
        auto cumulativeDataWeight = FromUnversionedValue<std::optional<i64>>(row[2]);

        if (!cumulativeDataWeight) {
            continue;
        }

        result[tabletIndex].emplace(rowIndex, *cumulativeDataWeight);
    }

    return result;
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

} // namespace NYT::NQueueAgent
