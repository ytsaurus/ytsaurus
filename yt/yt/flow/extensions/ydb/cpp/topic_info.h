#pragma once

#include "public.h"

#include "options.h"
#include "state_access.h"

#include <yt/yt/flow/library/cpp/common/source_controller.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/ytree/public.h>

#include <ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Discovers the clusters behind a topic and their partition counts.
class TYdbTopicInfoController
    : public TRefCounted
{
public:
    TYdbTopicInfoController(
        TYdbTopologyOptions options,
        IYdbClientFactoryPtr clientFactory,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        const IStatusProfilerPtr statusProfiler,
        TYdbStateAccess<TYdbTopologySnapshot> stateAccess);

    void Init(IInitContextPtr initContext);
    void Sync();
    void Commit();

    //! Fired with every successful description of the topic by a cluster.
    DEFINE_SIGNAL(void(const std::string& endpoint, const NYdb::NTopic::TTopicDescription& description), TopicDescribed);

    THashMap<std::string, std::shared_ptr<NYdb::NTopic::TTopicClient>> GetTopicClients();
    THashMap<std::string, TYdbClusterSnapshot> GetClusters();
    std::optional<bool> IsFederated() const;

    const std::string& GetDatabase() const;
    const std::string& GetTopic() const;

    //! Remembers which endpoints are already known to be unavailable and stops publishing their errors:
    //! their failures are noise rather than news. Endpoints discovered later are muted on creation.
    void SetUnavailableEndpoints(THashSet<std::string> endpoints);

private:
    void TryUpdateClusters();
    //! The count every cluster that reported one agrees on, or null if none did or they disagree.
    static std::optional<i64> GetBorrowablePartitionCount(const TYdbTopologySnapshot& state);
    //! The per-endpoint leaf, created on first use under a node muted by #SetUnavailableEndpoints.
    const IStatusErrorStatePtr& GetClusterErrorState(const std::string& endpoint);
    //! Applies |mutator| to the cluster of |endpoint| in the current state, creating the cluster if needed.
    bool MutateCluster(const std::string& endpoint, const std::function<bool(TYdbClusterSnapshot*)>& mutator);

protected:
    const NLogging::TLogger Logger;

private:
    const TYdbTopologyOptions Options_;

    const IYdbClientFactoryPtr ClientFactory_;
    const IInvokerPtr Invoker_;
    const IStatusProfilerPtr StatusProfiler_;
    //! Federation discovery itself, which is per entry point rather than per cluster and is therefore
    //! never silenced: it is how we would learn that a cluster is back.
    const IStatusErrorStatePtr DiscoverClustersErrorState_;
    const TYdbStateAccess<TYdbTopologySnapshot> StateAccess_;

    struct TClusterStatus
    {
        IStatusProfilerPtr Profiler;
        IStatusErrorStatePtr ErrorState;
    };

    THashMap<std::string, TClusterStatus> ClusterStatuses_;
    THashSet<std::string> UnavailableEndpoints_;
    // Filled in by discovery and read by the connectors; both run on |Invoker_|, which must be
    // serialized — this map is otherwise unsynchronized.
    THashMap<std::string, std::shared_ptr<NYdb::NTopic::TTopicClient>> TopicClients_;
    // In-memory mirror of the last discovery answer, kept to log availability transitions once.
    THashSet<std::string> ReadOnlyEndpoints_;
    std::optional<bool> Federated_;

    NConcurrency::TPeriodicExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TYdbTopicInfoController);

////////////////////////////////////////////////////////////////////////////////

//! Adopts partition counts persisted by a binary that predates #TYdbTopologySnapshot::Clusters.
void AdoptLegacyYdbPartitionCounts(TYdbTopologySnapshot* state);
//! Republishes partition counts for a binary that predates #TYdbTopologySnapshot::Clusters.
void PublishLegacyYdbPartitionCounts(TYdbTopologySnapshot* state);

NYson::TYsonString FormatYdbClusters(const THashMap<std::string, TYdbClusterSnapshot>& clusters);

//! Lists every partition of every discovered cluster, or null until each cluster has a partition count.
std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListYdbTopicKeys(
    const THashMap<std::string, TYdbClusterSnapshot>& clusters,
    const std::string& sourceIdentity,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Adds the partitions a count-only binary enumerated as scheduled: Flow may already own them.
void SeedLegacyYdbPartitions(TYdbPartitionGraph* graph, i64 partitionCount);

//! Merges a full description, keeping the history of partitions the description no longer lists.
void MergeYdbTopicDescription(TYdbPartitionGraph* graph, const NYdb::NTopic::TTopicDescription& description);

//! Merges a partition the topic described on its own, marking it present.
void MergeYdbPartitionInfo(TYdbPartitionGraph* graph, const NYdb::NTopic::TPartitionInfo& partition);

//! Records the partitions whose Flow source partition is completed or has read everything, and returns the newly
//! completed ones.
std::vector<std::pair<std::string, i64>> RecordYdbPartitionStatuses(
    TYdbPartitionGraphSnapshot* snapshot,
    const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses,
    const std::string& sourceIdentity);

//! Partitions to ask the topic about because they disappeared before Flow completed them.
std::vector<i64> GetUnresolvedYdbPartitions(const TYdbPartitionGraph& graph);

struct TYdbPartitionKeys
{
    THashMap<TKey, NYTree::IMapNodePtr> Keys;
    //! Listed keys whose reads wait for other partitions of the topic to complete.
    THashSet<TKey> WaitingForOtherPartitions;
};

//! Lists every selected partition Flow has not completed and marks it scheduled. A partition reads only after
//! all of its selected ancestors are completed; a removed one only drains what Flow already read from it.
//! A partition that cannot read yet is listed only if it was scheduled before or it succeeds a partition that
//! has read everything. A partition may complete only while all of its selected successors are listed.
//! While the latest description of the topic by |endpoint| failed, partitions that cannot read are unavailable.
TYdbPartitionKeys ListYdbPartitionKeys(
    const std::string& endpoint,
    TYdbPartitionGraph* graph,
    const std::string& sourceIdentity,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter,
    bool finite,
    bool descriptionFailed);

//! Whether some selected partition was listed: until then data written before now may wait in partitions no
//! listed key represents.
bool HasYdbPartitionListingStarted(
    const TYdbPartitionGraph& graph,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter);

//! Whether every selected partition of a known topology is either completed or scheduled.
bool AreAllYdbPartitionsScheduled(
    const TYdbPartitionGraph& graph,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
