#include "antiaffinity_healer.h"

#include "task.h"
#include "config.h"
#include "disruption_throttler.h"
#include "helpers.h"
#include "private.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/topology_zone.h>

#include <yp/client/api/native/helpers.h>

#include <util/random/shuffle.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NClient::NApi;
using namespace NClient::NApi::NNative;

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TEvictionTask
    : public TTaskBase
{
public:
    TEvictionTask(
        TGuid id,
        TInstant startTime,
        TObjectCompositeId podCompositeId)
        : TTaskBase(std::move(id), startTime)
        , PodCompositeId_(std::move(podCompositeId))
    { }

    virtual std::vector<TObjectId> GetInvolvedPodIds() const override
    {
        return {PodCompositeId_.Id};
    }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        YT_VERIFY(State_ == ETaskState::Active);

        auto* pod = FindPod(cluster, PodCompositeId_);

        if (!pod) {
            YT_LOG_DEBUG("Eviction task is considered finished; pod does not exist");
            State_ = ETaskState::Succeeded;
        } else if (pod->Eviction().state() == NProto::EEvictionState::ES_NONE) {
            YT_LOG_DEBUG("Eviction task is considered finished; pod is in none eviction state");
            State_ = ETaskState::Succeeded;
        } else {
            YT_LOG_DEBUG("Eviction task is considered not finished; pod is not evicted yet");
        }
    }

private:
    const TObjectCompositeId PodCompositeId_;
};

////////////////////////////////////////////////////////////////////////////////

ITaskPtr CreateEvictionTask(const IClientPtr& client, TPod* pod)
{
    auto id = TGuid::Create();
    auto podCompositeId = GetCompositeId(pod);

    YT_LOG_DEBUG("Creating eviction task (TaskId: %v, Pod: %v,)",
        id,
        podCompositeId);

    WaitFor(RequestPodEviction(
        client,
        pod->GetId(),
        Format("Heavy Scheduler antiaffinity healing (TaskId: %v)", id),
        TRequestPodEvictionOptions{
            .ValidateDisruptionBudget = true,
            .Reason = EEvictionReason::Scheduler}))
        .ValueOrThrow();

    return New<TEvictionTask>(
        std::move(id),
        TInstant::Now(),
        std::move(podCompositeId));
}

////////////////////////////////////////////////////////////////////////////////

class TAntiaffinityHealer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TAntiaffinityHealerConfigPtr config,
        IClientPtr client,
        bool verbose)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , Verbose_(verbose)
    { }

    std::vector<ITaskPtr> CreateTasks(
        const TClusterPtr& cluster,
        const TDisruptionThrottlerPtr& disruptionThrottler,
        const THashSet<TObjectId>& ignorePodIds,
        int maxTaskCount,
        int currentTotalTaskCount)
    {
        auto podSets = cluster->GetPodSets();
        Shuffle(podSets.begin(), podSets.end());
        auto podSetEnd = static_cast<int>(podSets.size()) < Config_->PodSetsPerIterationLimit
            ? podSets.end()
            : podSets.begin() + Config_->PodSetsPerIterationLimit;

        std::vector<ITaskPtr> tasks;
        for (auto podSetIt = podSets.begin(); podSetIt < podSetEnd; ++podSetIt) {
            int tasksLeft = maxTaskCount - static_cast<int>(tasks.size());
            int minSuitableNodeCount = Config_->SafeSuitableNodeCount
                + currentTotalTaskCount
                + static_cast<int>(tasks.size());

            auto podSetTasks = CreatePodSetTasks(
                *podSetIt,
                disruptionThrottler,
                ignorePodIds,
                tasksLeft,
                minSuitableNodeCount);

            tasks.insert(tasks.end(),
                std::make_move_iterator(podSetTasks.begin()),
                std::make_move_iterator(podSetTasks.end()));
        }

        return tasks;
    }

private:
    const TAntiaffinityHealerConfigPtr Config_;
    const IClientPtr Client_;
    const bool Verbose_;

    std::vector<ITaskPtr> CreatePodSetTasks(
        TPodSet* podSet,
        const TDisruptionThrottlerPtr& disruptionThrottler,
        const THashSet<TObjectId>& ignorePodIds,
        int maxTaskCount,
        int minSuitableNodeCount)
    {
        auto topologyZonePods = GetTopologyZonePods(podSet);

        std::vector<TTopologyZone*> topologyZones;
        topologyZones.reserve(topologyZonePods.size());
        for (auto const& [zone, pods] : topologyZonePods) {
            topologyZones.push_back(zone);
        }

        // NB: we assume that topology zones are hierarchical (zone A intersects with zone B
        // iff A is a subzone of B or B is a subzone of A). In this case we get slightly less
        // evictions in certain cases if we do the sort (we only evict pods in a topology zone
        // if we failed to fix antiaffinity constraints violation with evictions in subzones).
        std::sort(
            topologyZones.begin(),
            topologyZones.end(),
            [&] (TTopologyZone* lhs, TTopologyZone* rhs) {
                return topologyZonePods[lhs].size() < topologyZonePods[rhs].size();
            });

        std::vector<ITaskPtr> tasks;

        // NB: in case of antiaffinity constraints with shards, we might not add all pods eligible
        // for eviction to `candidates`. This is fine: we request eviction for some of the eligible
        // pods, and once they are evicted we can request eviction for other pods.
        for (auto* zone : topologyZones) {
            int evictingPodCount = 0;
            for (auto* pod : topologyZonePods[zone]) {
                if (disruptionThrottler->IsBeingEvicted(pod->GetId())) {
                    evictingPodCount += 1;
                }
            }

            for (auto* pod : topologyZonePods[zone]) {
                if (ignorePodIds.find(pod->GetId()) != ignorePodIds.end()) {
                    continue;
                }
                int vacancyCount = zone->GetAntiaffinityVacancyCount(pod);
                if (vacancyCount + evictingPodCount < 0) {
                    YT_LOG_DEBUG_IF(Verbose_,
                        "Found pod that overcommits antiaffinity (PodId: %v, PodSetId: %v, TopologyZone: %v)",
                        pod->GetId(),
                        podSet->GetId(),
                        *zone);
                    if (!disruptionThrottler->ThrottleEviction(pod)
                        && HasEnoughSuitableNodes(pod, minSuitableNodeCount, Verbose_))
                    {
                        if (static_cast<int>(tasks.size()) < maxTaskCount) {
                            tasks.push_back(CreateEvictionTask(Client_, pod));
                            disruptionThrottler->RegisterPodEviction(pod);
                            evictingPodCount += 1;
                        } else {
                            YT_LOG_DEBUG("Failed to create eviction task: concurrent task limit reached for antiaffinity healer "
                                "(PodId: %v)",
                                pod->GetId());
                        }
                    }
                }
            }
        }

        return tasks;
    }

    THashMap<TTopologyZone*, std::vector<TPod*>> GetTopologyZonePods(
        TPodSet* podSet)
    {
        THashMap<TTopologyZone*, std::vector<TPod*>> topologyZonePods;
        for (auto* pod : podSet->SchedulablePods()) {
            auto* node = pod->GetNode();
            if (node) {
                for (auto* zone : node->TopologyZones()) {
                    topologyZonePods[zone].push_back(pod);
                }
            }
        }
        return topologyZonePods;
    }
};

////////////////////////////////////////////////////////////////////////////////

TAntiaffinityHealer::TAntiaffinityHealer(
    TAntiaffinityHealerConfigPtr config,
    IClientPtr client,
    bool verbose)
    : Impl_(New<TImpl>(std::move(config), std::move(client), verbose))
{ }

std::vector<ITaskPtr> TAntiaffinityHealer::CreateTasks(
    const TClusterPtr& cluster,
    const TDisruptionThrottlerPtr& disruptionThrottler,
    const THashSet<TObjectId>& ignorePodIds,
    int maxTaskCount,
    int currentTotalTaskCount)
{
    return Impl_->CreateTasks(
        cluster,
        disruptionThrottler,
        ignorePodIds,
        maxTaskCount,
        currentTotalTaskCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
