#include "topic_info.h"

#include "client_factory.h"
#include "helpers.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <util/generic/algorithm.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using NConcurrency::WaitFor;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsYdbPartitionSelected(const std::optional<std::vector<std::pair<int, int>>>& partitionFilter, i64 partitionId)
{
    return !partitionFilter || AnyOf(*partitionFilter, [&] (const auto& range) {
        return partitionId >= range.first && partitionId < range.second;
    });
}

void AddParent(TYdbPartitionNode* node, i64 parentId)
{
    if (!IsIn(node->ParentIds, parentId)) {
        node->ParentIds.push_back(parentId);
    }
}

bool HasIncompleteSelectedAncestor(
    const TYdbPartitionGraph& graph,
    i64 partitionId,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter)
{
    THashSet<i64> visited;
    std::vector<i64> pending = GetOrCrash(graph.Partitions, partitionId).ParentIds;
    while (!pending.empty()) {
        const auto ancestorId = pending.back();
        pending.pop_back();
        if (!visited.insert(ancestorId).second) {
            continue;
        }
        const auto& ancestor = GetOrCrash(graph.Partitions, ancestorId);
        if (IsYdbPartitionSelected(partitionFilter, ancestorId) && !ancestor.Completed) {
            return true;
        }
        pending.insert(pending.end(), ancestor.ParentIds.begin(), ancestor.ParentIds.end());
    }
    return false;
}

THashMap<i64, std::vector<i64>> CollectYdbChildIds(const TYdbPartitionGraph& graph)
{
    THashMap<i64, std::vector<i64>> childIds;
    for (const auto& [id, node] : graph.Partitions) {
        for (auto parentId : node.ParentIds) {
            childIds[parentId].push_back(id);
        }
    }
    return childIds;
}

bool IsUnrelatedLegacyPartition(const THashMap<i64, std::vector<i64>>& childIds, i64 partitionId, const TYdbPartitionNode& node)
{
    return !node.Described && node.ParentIds.empty() && !childIds.contains(partitionId);
}

TYdbPartitionNode* FindYdbPartitionNode(TYdbPartitionGraphSnapshot* snapshot, const std::string& endpoint, i64 partitionId)
{
    auto* graph = snapshot->Clusters.FindPtr(endpoint);
    if (!graph) {
        return nullptr;
    }
    auto it = graph->Partitions.find(partitionId);
    return it == graph->Partitions.end() ? nullptr : &it->second;
}

//! A partition only a legacy count knew of, which no description relates to the others, may be an
//! ancestor of any partition.
bool HasUnrelatedLegacyPartition(
    const TYdbPartitionGraph& graph,
    const THashMap<i64, std::vector<i64>>& childIds,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter)
{
    return AnyOf(graph.Partitions, [&] (const auto& item) {
        const auto& [id, node] = item;
        return IsUnrelatedLegacyPartition(childIds, id, node) &&
            !node.Completed &&
            IsYdbPartitionSelected(partitionFilter, id);
    });
}

//! The selected partitions to list before |partitionId| completes, or null if it must not complete yet: data
//! may still be read from a partition its completion would leave unrepresented in the watermark.
std::optional<THashSet<i64>> FindYdbSuccessorsToList(
    const TYdbPartitionGraph& graph,
    const THashMap<i64, std::vector<i64>>& childIds,
    i64 partitionId,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter,
    bool finite)
{
    THashSet<i64> successors;
    const auto& partition = GetOrCrash(graph.Partitions, partitionId);
    if (IsUnrelatedLegacyPartition(childIds, partitionId, partition)) {
        for (const auto& [id, node] : graph.Partitions) {
            if (id != partitionId && node.Present && !node.Completed && IsYdbPartitionSelected(partitionFilter, id)) {
                successors.insert(id);
            }
        }
        return successors;
    }
    // A finite source reads the partitions it knows of, while an infinite one may have partitions nobody
    // described yet descending from a partition without known children.
    if (!childIds.contains(partitionId)) {
        return finite ? std::optional(successors) : std::nullopt;
    }

    THashSet<i64> visited;
    std::vector<i64> pending = GetOrCrash(childIds, partitionId);
    while (!pending.empty()) {
        const auto id = pending.back();
        pending.pop_back();
        if (!visited.insert(id).second) {
            continue;
        }
        const auto& node = GetOrCrash(graph.Partitions, id);
        if (IsYdbPartitionSelected(partitionFilter, id)) {
            if (!node.Completed) {
                successors.insert(id);
            }
            continue;
        }
        if (const auto* children = childIds.FindPtr(id)) {
            pending.insert(pending.end(), children->begin(), children->end());
        } else if (!finite) {
            return std::nullopt;
        }
    }
    return successors;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void AdoptLegacyYdbPartitionCounts(TYdbTopologySnapshot* state)
{
    for (const auto& [endpoint, partitionCount] : state->CachedPartitionCounts) {
        // A null count carries nothing, and adopting it would make an undiscovered endpoint look like a
        // discovered cluster still waiting to be described.
        if (!partitionCount) {
            continue;
        }
        auto& cluster = state->Clusters[endpoint];
        if (!cluster.PartitionCount) {
            cluster.PartitionCount = partitionCount;
        }
    }
}

void PublishLegacyYdbPartitionCounts(TYdbTopologySnapshot* state)
{
    state->CachedPartitionCounts.clear();
    for (const auto& [endpoint, cluster] : state->Clusters) {
        state->CachedPartitionCounts[endpoint] = cluster.PartitionCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString FormatYdbClusters(const THashMap<std::string, TYdbClusterSnapshot>& clusters)
{
    return NYTree::BuildYsonStringFluently(NYson::EYsonFormat::Text)
        .DoMapFor(clusters, [] (NYTree::TFluentMap fluent, const auto& item) {
            fluent
                .Item(item.first)
                .BeginMap()
                .OptionalItem("partition_count", item.second.PartitionCount)
                .OptionalItem("read_only_at", item.second.ReadOnlyAt)
                .EndMap();
        });
}

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListYdbTopicKeys(
    const THashMap<std::string, TYdbClusterSnapshot>& clusters,
    const std::string& sourceIdentity,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;
    if (clusters.empty()) {
        // Nothing discovered yet.
        return std::nullopt;
    }
    const auto& filter = partitionFilter;

    auto trivialSpec = NYTree::GetEphemeralNodeFactory()->CreateMap();
    THashMap<TKey, NYTree::IMapNodePtr> keys;
    i64 skipped = 0;
    for (const auto& [endpoint, cluster] : clusters) {
        if (!cluster.PartitionCount) {
            return std::nullopt;
        }
        for (int i = 0; i < *cluster.PartitionCount; ++i) {
            if (!IsYdbPartitionSelected(filter, i)) {
                ++skipped;
                continue;
            }
            keys[GenerateYdbTopicKey(sourceIdentity, endpoint, i)] = trivialSpec;
        }
    }
    if (skipped != 0) {
        YT_TLOG_DEBUG("Skipped some partitions due to filter")
            .With("Skipped", skipped)
            .With("Left", std::size(keys));
    }
    return keys;
}

////////////////////////////////////////////////////////////////////////////////

void SeedLegacyYdbPartitions(TYdbPartitionGraph* graph, i64 partitionCount)
{
    for (i64 id = 0; id < partitionCount; ++id) {
        graph->Partitions[id].Scheduled = true;
    }
}

void MergeYdbPartitionInfo(TYdbPartitionGraph* graph, const NYdb::NTopic::TPartitionInfo& partition)
{
    const auto id = static_cast<i64>(partition.GetPartitionId());
    auto& node = graph->Partitions[id];
    node.Active = partition.GetActive();
    node.Described = true;
    node.Present = true;
    node.Removed = false;
    if (const auto& stats = partition.GetPartitionStats()) {
        node.EndOffsetExclusive = static_cast<i64>(stats->GetEndOffset());
    }
    for (auto parentId : partition.GetParentPartitionIds()) {
        AddParent(&node, static_cast<i64>(parentId));
        graph->Partitions.try_emplace(static_cast<i64>(parentId));
    }
    for (auto childId : partition.GetChildPartitionIds()) {
        AddParent(&graph->Partitions[static_cast<i64>(childId)], id);
    }
}

void MergeYdbTopicDescription(TYdbPartitionGraph* graph, const NYdb::NTopic::TTopicDescription& description)
{
    for (auto& [id, node] : graph->Partitions) {
        node.Present = false;
    }
    for (const auto& partition : description.GetPartitions()) {
        MergeYdbPartitionInfo(graph, partition);
    }
    graph->TopologyKnown = true;
}

std::vector<std::pair<std::string, i64>> RecordYdbPartitionStatuses(
    TYdbPartitionGraphSnapshot* snapshot,
    const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses,
    const std::string& sourceIdentity)
{
    std::vector<std::pair<std::string, i64>> completed;
    for (const auto& [key, status] : statuses) {
        auto [endpoint, partitionId] = UnpackYdbTopicKey(key);
        if (key != GenerateYdbTopicKey(sourceIdentity, endpoint, partitionId)) {
            continue;
        }
        auto* node = FindYdbPartitionNode(snapshot, endpoint, partitionId);
        if (status->PartitionState == EPartitionState::Completed) {
            if (!node) {
                node = &snapshot->Clusters[endpoint].Partitions[partitionId];
            }
            if (!node->Completed) {
                node->Completed = true;
                completed.emplace_back(std::move(endpoint), partitionId);
            }
        } else if (node && status->PartitionStatus) {
            node->InputExhausted |= NYTree::ConvertTo<TYdbSourcePartitionStatusPtr>(status->PartitionStatus)->InputExhausted;
        }
    }
    return completed;
}

std::vector<i64> GetUnresolvedYdbPartitions(const TYdbPartitionGraph& graph)
{
    std::vector<i64> ids;
    for (const auto& [id, node] : graph.Partitions) {
        if (!node.Present && !node.Removed && !node.Completed) {
            ids.push_back(id);
        }
    }
    return ids;
}

TYdbPartitionKeys ListYdbPartitionKeys(
    const std::string& endpoint,
    TYdbPartitionGraph* graph,
    const std::string& sourceIdentity,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter,
    bool finite,
    bool descriptionFailed)
{
    const auto childIds = CollectYdbChildIds(*graph);
    const bool legacyPartitionBlocksReads = HasUnrelatedLegacyPartition(*graph, childIds, partitionFilter);

    struct TListing
    {
        TYdbSourcePartitionSpecPtr Spec;
        bool WaitsForOtherPartitions = false;
        bool Listed = false;
        std::optional<THashSet<i64>> Successors;
    };

    std::map<i64, TListing> listings;
    for (const auto& [id, node] : graph->Partitions) {
        if (node.Completed || !IsYdbPartitionSelected(partitionFilter, id)) {
            continue;
        }
        auto& listing = listings[id];
        listing.Spec = New<TYdbSourcePartitionSpec>();
        if (graph->TopologyKnown && node.Removed) {
            listing.Spec->Retired = true;
            listing.Spec->FinalOffsetExclusive = node.EndOffsetExclusive;
        } else if (!graph->TopologyKnown || !node.Present) {
            listing.Spec->ReadBlocked = true;
        } else if (legacyPartitionBlocksReads || HasIncompleteSelectedAncestor(*graph, id, partitionFilter)) {
            listing.Spec->ReadBlocked = true;
            listing.WaitsForOtherPartitions = true;
        } else if (!node.Active) {
            listing.Spec->FinalOffsetExclusive = node.EndOffsetExclusive;
        }
        listing.Spec->TopologyUnavailable = descriptionFailed && listing.Spec->ReadBlocked;
        listing.Listed = !listing.Spec->ReadBlocked || node.Scheduled;
        listing.Successors = graph->TopologyKnown
            ? FindYdbSuccessorsToList(*graph, childIds, id, partitionFilter, finite)
            : std::nullopt;
    }

    // Successors are listed before an exhausted partition may complete, so the watermark it holds
    // passes to them rather than to the completed partition.
    for (const auto& [id, listing] : listings) {
        if (!listing.Listed || !listing.Successors || !GetOrCrash(graph->Partitions, id).InputExhausted) {
            continue;
        }
        for (auto successorId : *listing.Successors) {
            GetOrCrash(listings, successorId).Listed = true;
        }
    }

    TYdbPartitionKeys result;
    for (auto& [id, listing] : listings) {
        if (!listing.Listed) {
            continue;
        }
        listing.Spec->CompletionAllowed = listing.Successors && AllOf(*listing.Successors, [&] (i64 successorId) {
            return GetOrCrash(listings, successorId).Listed;
        });
        GetOrCrash(graph->Partitions, id).Scheduled = true;
        auto key = GenerateYdbTopicKey(sourceIdentity, endpoint, static_cast<int>(id));
        if (listing.WaitsForOtherPartitions) {
            result.WaitingForOtherPartitions.insert(key);
        }
        result.Keys.emplace(std::move(key), NYTree::ConvertTo<NYTree::IMapNodePtr>(listing.Spec));
    }
    return result;
}

bool AreAllYdbPartitionsScheduled(
    const TYdbPartitionGraph& graph,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter)
{
    return graph.TopologyKnown && AllOf(graph.Partitions, [&] (const auto& item) {
        const auto& [id, node] = item;
        return node.Completed || node.Scheduled || !IsYdbPartitionSelected(partitionFilter, id);
    });
}

bool HasYdbPartitionListingStarted(
    const TYdbPartitionGraph& graph,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter)
{
    for (const auto& [id, node] : graph.Partitions) {
        if (!IsYdbPartitionSelected(partitionFilter, id)) {
            continue;
        }
        if (node.Scheduled || node.Completed) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TYdbTopicInfoController::TYdbTopicInfoController(
    TYdbTopologyOptions options,
    IYdbClientFactoryPtr clientFactory,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    const IStatusProfilerPtr statusProfiler,
    TYdbStateAccess<TYdbTopologySnapshot> stateAccess)
    : Logger(logger)
    , Options_(std::move(options))
    , ClientFactory_(std::move(clientFactory))
    , Invoker_(std::move(invoker))
    , StatusProfiler_(statusProfiler)
    , DiscoverClustersErrorState_(statusProfiler->ErrorState("/discover_clusters"))
    , StateAccess_(std::move(stateAccess))
{
    if (Options_.DiscoveryEntries.empty()) {
        THROW_ERROR_EXCEPTION("Empty discovery endpoints for topic %Qv", Options_.Topic);
    }
}

void TYdbTopicInfoController::Init(IInitContextPtr initContext)
{
    StateAccess_.Init(initContext);

    auto state = StateAccess_.Read();
    AdoptLegacyYdbPartitionCounts(&state);
    StateAccess_.Write(state);

    Executor_ = New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TYdbTopicInfoController::TryUpdateClusters, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(Options_.UpdatePartitionCountPeriod));
    Executor_->Start();
    Executor_->ScheduleOutOfBand();
}

void TYdbTopicInfoController::Sync()
{ }

void TYdbTopicInfoController::Commit()
{ }

const std::string& TYdbTopicInfoController::GetDatabase() const
{
    return Options_.Database;
}

const std::string& TYdbTopicInfoController::GetTopic() const
{
    return Options_.Topic;
}

bool TYdbTopicInfoController::MutateCluster(
    const std::string& endpoint,
    const std::function<bool(TYdbClusterSnapshot*)>& mutator)
{
    auto state = StateAccess_.Read();
    bool updated = mutator(&state.Clusters[endpoint]);
    StateAccess_.Write(state);
    return updated;
}

void TYdbTopicInfoController::TryUpdateClusters()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

    bool wasUpdated = false;
    bool discoveryComplete = true;
    std::vector<TError> discoveryErrors;
    THashSet<std::string> discoveredEndpoints;
    bool federated = Options_.ExpectFederation;

    for (const auto& entry : Options_.DiscoveryEntries) {
        const auto& entryEndpoint = entry.Endpoint;
        std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> clusters;
        try {
            // A fresh client per tick, so every answer is a live discovery request. A shared observer
            // would serve its cached state: a discovery outage stays invisible while |ReadOnlyAt| keeps
            // being stamped from data of unbounded age.
            auto federatedClient = ClientFactory_->CreateFederatedTopicClient(entryEndpoint, entry.Database);
            clusters = WaitFor(FromArcadiaFuture(federatedClient->GetAllClusterInfo())
                    .WithTimeout(Options_.ClientTimeout))
                .ValueOrThrow();
            THROW_ERROR_EXCEPTION_IF(clusters.empty(), "Federation discovery returned empty cluster list");
            // On BAD_REQUEST/UNIMPLEMENTED from discovery the SDK fabricates a single nameless AVAILABLE
            // cluster. For a raw endpoint or a single-cluster installation that is the legitimate
            // non-federated mode, but a federation can never look like that, so reject the answer
            // instead of letting it prune the real per-DC clusters.
            if (Options_.ExpectFederation &&
                AnyOf(clusters, [] (const auto& cluster) {
                    return cluster.Name.empty();
                }))
            {
                THROW_ERROR_EXCEPTION(
                    "Federation discovery fell back to a single nameless cluster for a federated installation at %Qv",
                    entryEndpoint);
            }
        } catch (const std::exception& ex) {
            discoveryComplete = false;
            discoveryErrors.push_back(TError("Failed to discover topic clusters")
                    .With(ex)
                    .With("endpoint", entryEndpoint));
            YT_TLOG_ERROR("Failed to discover topic clusters")
                .With("Endpoint", entryEndpoint)
                .With(ex);
            continue;
        }

        if (Options_.AllowFederation && AnyOf(clusters, [] (const auto& cluster) {
                return !cluster.Name.empty();
            }))
        {
            federated = true;
            Federated_ = true;
        }

        for (const auto& cluster : clusters) {
            // An empty cluster name is the SDK's single-cluster fallback for a non-federated YDB.
            auto endpoint = cluster.Name.empty() ? entryEndpoint : std::string(cluster.Endpoint);
            discoveredEndpoints.insert(endpoint);
            // Partitions of a degraded cluster must stay listed, so keep the entry (and its count).
            const bool availableForWrite = cluster.IsAvailableForWrite();
            MutateCluster(endpoint, [&] (TYdbClusterSnapshot* clusterInfo) {
                if (!availableForWrite) {
                    clusterInfo->ReadOnlyAt = TInstant::Now();
                }
                return false;
            });

            if (availableForWrite) {
                if (ReadOnlyEndpoints_.erase(endpoint)) {
                    YT_TLOG_INFO("Cluster became available")
                        .With("Endpoint", endpoint);
                    wasUpdated = true;
                }
            } else {
                // Discovery reports READ_ONLY today; any other degraded status it may introduce (such as
                // UNAVAILABLE) means the same here: record the instant and keep the cluster.
                if (ReadOnlyEndpoints_.insert(endpoint).second) {
                    YT_TLOG_INFO("Cluster is reported as not available for write")
                        .With("Endpoint", endpoint)
                        .With("Status", static_cast<int>(cluster.Status));
                    wasUpdated = true;
                }
            }

            try {
                // In the single-cluster fallback |cluster.Path| is our own client setting and carries no
                // per-cluster meaning.
                if (!cluster.Name.empty() && cluster.Path != Options_.Database) {
                    THROW_ERROR_EXCEPTION("Discovered cluster database differs from the configured one")
                        .With("endpoint", endpoint)
                        .With("cluster_database", cluster.Path)
                        .With("configured_database", Options_.Database);
                }
                auto& topicClient = TopicClients_[endpoint];
                if (!topicClient) {
                    topicClient = ClientFactory_->CreateTopicClient(endpoint, Options_.Database);
                }
                auto future = topicClient->DescribeTopic(
                    Options_.Topic,
                    NYdb::NTopic::TDescribeTopicSettings()
                        .IncludeStats(Options_.IncludePartitionStats)
                        .ClientTimeout(Options_.ClientTimeout));
                auto result = WaitFor(FromArcadiaFuture(future)).ValueOrThrow();
                if (!result.IsSuccess()) {
                    THROW_ERROR_EXCEPTION("Bad answer")
                        .With("answer", ToString(result))
                        .With("endpoint", endpoint)
                        .With("database", Options_.Database)
                        .With("topic", Options_.Topic);
                }
                const i64 partitionCount = result.GetTopicDescription().GetTotalPartitionsCount();
                // The state is reread after the wait: the yield may have let the epoch move on.
                wasUpdated |= MutateCluster(endpoint, [&] (TYdbClusterSnapshot* clusterInfo) {
                    if (clusterInfo->PartitionCount == partitionCount) {
                        return false;
                    }
                    clusterInfo->PartitionCount = partitionCount;
                    return true;
                });
                TopicDescribed_.Fire(endpoint, result.GetTopicDescription());
                GetClusterErrorState(endpoint)->ClearError();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to update cluster info")
                    .With(ex)
                    .With("endpoint", endpoint);
                GetClusterErrorState(endpoint)->SetError(error);
                YT_TLOG_ERROR("Failed to update cluster info")
                    .With("Endpoint", endpoint)
                    .With(ex);
            }
        }
    }

    auto state = StateAccess_.Read();
    if (discoveryComplete) {
        if (!federated) {
            Federated_ = false;
        }
        EraseNodesIf(state.Clusters, [&] (const auto& item) {
            if (discoveredEndpoints.contains(item.first)) {
                return false;
            }
            YT_TLOG_INFO("Dropping cluster that disappeared from federation discovery")
                .With("Endpoint", item.first);
            TopicClients_.erase(item.first);
            ReadOnlyEndpoints_.erase(item.first);
            ClusterStatuses_.erase(item.first);
            wasUpdated = true;
            return true;
        });
    }

    // Clusters of one federation are expected to hold the same topic layout, and a cluster that has not
    // answered leans on that to get a count at all. Disagreement is therefore not a degraded mode but a
    // broken assumption, and it blocks the whole source: nothing gets a borrowed count.
    if (!GetBorrowablePartitionCount(state) &&
        AnyOf(state.Clusters, [] (const auto& item) {
            return item.second.PartitionCount.has_value();
        }))
    {
        YT_TLOG_ERROR("Clusters report different partition counts, so a cluster without one cannot borrow it")
            .With("Clusters", FormatYdbClusters(state.Clusters));
    }

    PublishLegacyYdbPartitionCounts(&state);
    StateAccess_.Write(state);

    if (discoveryErrors.empty()) {
        DiscoverClustersErrorState_->ClearError();
    } else {
        DiscoverClustersErrorState_->SetError(
            TError("Failed to discover topic clusters").With(std::move(discoveryErrors)));
    }

    if (wasUpdated) {
        YT_TLOG_INFO("Topic clusters were updated")
            .With("Clusters", FormatYdbClusters(state.Clusters));
    }
}

std::optional<bool> TYdbTopicInfoController::IsFederated() const
{
    return Federated_;
}

const IStatusErrorStatePtr& TYdbTopicInfoController::GetClusterErrorState(const std::string& endpoint)
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Invoker_));

    auto& status = ClusterStatuses_[endpoint];
    if (!status.ErrorState) {
        status.Profiler = StatusProfiler_->WithPrefix(Format("/clusters/%v", endpoint));
        status.Profiler->Mute(UnavailableEndpoints_.contains(endpoint));
        status.ErrorState = status.Profiler->ErrorState("/update_partition_count");
    }
    return status.ErrorState;
}

void TYdbTopicInfoController::SetUnavailableEndpoints(THashSet<std::string> endpoints)
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Invoker_));

    UnavailableEndpoints_ = std::move(endpoints);
    for (const auto& [endpoint, status] : ClusterStatuses_) {
        status.Profiler->Mute(UnavailableEndpoints_.contains(endpoint));
    }
}

THashMap<std::string, std::shared_ptr<NYdb::NTopic::TTopicClient>> TYdbTopicInfoController::GetTopicClients()
{
    return TopicClients_;
}

std::optional<i64> TYdbTopicInfoController::GetBorrowablePartitionCount(const TYdbTopologySnapshot& state)
{
    std::optional<i64> partitionCount;
    for (const auto& [endpoint, cluster] : state.Clusters) {
        if (!cluster.PartitionCount) {
            continue;
        }
        // Disagreement refutes the premise that a topic is uniform across a federation, and guessing
        // below the real count would leave the tail of the range unlisted and unread.
        if (partitionCount && *partitionCount != *cluster.PartitionCount) {
            return std::nullopt;
        }
        partitionCount = cluster.PartitionCount;
    }
    return partitionCount;
}

THashMap<std::string, TYdbClusterSnapshot> TYdbTopicInfoController::GetClusters()
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Invoker_));

    // A cluster that has never reported a count blocks the whole source: #ListKeys yields nothing until
    // every discovered cluster has one. Within a federation a topic has the same partition count
    // everywhere, so such a cluster may go on the count the others reported — until it speaks for itself.
    // The guess is therefore handed out here and never written into the state, so a cluster's own answer
    // stays the only thing that can make a count stick.
    auto clusters = StateAccess_.Read().Clusters;
    for (auto& [endpoint, cluster] : clusters) {
        const auto* status = ClusterStatuses_.FindPtr(endpoint);
        cluster.DescriptionFailed = status && status->ErrorState->GetStatus().IsOK == false;
    }
    auto borrowedPartitionCount = GetBorrowablePartitionCount(TYdbTopologySnapshot{.Clusters = clusters});
    if (!borrowedPartitionCount) {
        return clusters;
    }

    for (auto& [endpoint, cluster] : clusters) {
        if (!cluster.PartitionCount) {
            cluster.PartitionCount = borrowedPartitionCount;
        }
    }
    return clusters;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
