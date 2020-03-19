#include "antiaffinity_healer.h"

#include "config.h"
#include "disruption_throttler.h"
#include "heavy_scheduler.h"
#include "helpers.h"
#include "private.h"
#include "task.h"
#include "task_manager.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/topology_zone.h>

#include <yp/client/api/native/helpers.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/misc/finally.h>

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

ITaskPtr CreateEvictionTask(const IClientPtr& client, TPod* pod, bool validateDisruptionBudget)
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
            validateDisruptionBudget,
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
        THeavyScheduler* heavyScheduler,
        TAntiaffinityHealerConfigPtr config)
        : HeavyScheduler_(heavyScheduler)
        , Config_(std::move(config))
    { }

    void Run(const TClusterPtr& cluster)
    {
        try {
            GuardedRun(cluster);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error running antiaffinity healer");
        }
    }

private:
    THeavyScheduler* const HeavyScheduler_;
    const TAntiaffinityHealerConfigPtr Config_;

    struct TProfiling
    {
        NProfiling::TSimpleGauge AntiaffinityVacancyOvercommit;
        NProfiling::TSimpleGauge AntiaffinityOvercommittedZones;
    };

    THashMap<TString, TProfiling> ProfilingPerZoneKey_;

    void GuardedRun(const TClusterPtr& cluster)
    {
        auto podSets = cluster->GetPodSets();
        Shuffle(podSets.begin(), podSets.end());
        int podsLeft = Config_->PodsPerIterationSoftLimit;

        CollectProfiling(cluster);

        for (const auto& podSet : podSets) {
            if (podsLeft <= 0) {
                break;
            }
            podsLeft -= static_cast<int>(podSet->SchedulablePods().size());
            CreatePodSetTasks(podSet);
        }
    }

    void CollectProfiling(const TClusterPtr& cluster)
    {
        struct TAntiaffinityOvercommitCounts
        {
            ui64 VacancyOvercommit;
            ui64 OvecommittedZones;
        };

        THashMap<TString, TAntiaffinityOvercommitCounts> countsPerZoneKey;

        for (auto* node : cluster->GetNodes()) {
            for (auto* zone : node->TopologyZones()) {
                auto countsIt = countsPerZoneKey.find(zone->GetKey());
                if (countsIt == countsPerZoneKey.end()) {
                    countsIt = countsPerZoneKey.insert({zone->GetKey(), {0, 0}}).first;
                }
                int vacancyOvercommit = zone->EstimateOvercommittedVacancyCount();
                if (vacancyOvercommit > 0) {
                    auto& counts = countsIt->second;
                    ++counts.OvecommittedZones;
                    counts.VacancyOvercommit += vacancyOvercommit;
                }
            }
        }

        for (const auto& [zoneKey, counts] : countsPerZoneKey) {
            auto profilingIt = ProfilingPerZoneKey_.find(zoneKey);
            if (profilingIt == ProfilingPerZoneKey_.end()) {
                auto tagId = NProfiling::TProfileManager::Get()->RegisterTag("zone_key", zoneKey);
                TProfiling zoneKeyProfiling = {
                    NProfiling::TSimpleGauge("/antiaffinity_vacancy_overcommit", {tagId}),
                    NProfiling::TSimpleGauge("/antiaffinity_overcommitted_zones", {tagId})};
                profilingIt = ProfilingPerZoneKey_.insert({zoneKey, zoneKeyProfiling}).first;
            }
            auto& profiling = profilingIt->second;
            Profiler.Update(profiling.AntiaffinityVacancyOvercommit, counts.VacancyOvercommit);
            Profiler.Update(profiling.AntiaffinityOvercommittedZones, counts.OvecommittedZones);
        }

        std::vector<TString> savedZoneKeys;
        for (const auto& [zoneKey, _] : ProfilingPerZoneKey_) {
            savedZoneKeys.push_back(zoneKey);
        }
        for (const auto& zoneKey : savedZoneKeys) {
            if (countsPerZoneKey.find(zoneKey) == countsPerZoneKey.end()) {
                ProfilingPerZoneKey_.erase(zoneKey);
            }
        }
    }

    void CreatePodSetTasks(TPodSet* podSet)
    {
        auto topologyZonePods = GetTopologyZonePods(podSet);

        std::vector<TTopologyZone*> topologyZones;
        topologyZones.reserve(topologyZonePods.size());
        for (const auto& [zone, pods] : topologyZonePods) {
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

        const auto& taskManager = HeavyScheduler_->GetTaskManager();
        const auto& disruptionThrottler = HeavyScheduler_->GetDisruptionThrottler();

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
                if (taskManager->HasTaskInvolvingPod(pod)) {
                    continue;
                }
                auto optionalVacancyCount = zone->TryEstimateAntiaffinityVacancyCount(pod);
                // If finishing even all currently requested evictions (#evictingPodCount) is not enough to
                // prevent overcommit, try to request yet another eviction.
                if (optionalVacancyCount && *optionalVacancyCount + evictingPodCount < 0) {
                    YT_LOG_DEBUG_IF(HeavyScheduler_->GetVerbose(),
                        "Found pod that overcommits antiaffinity (PodId: %v, PodSetId: %v, TopologyZone: %v)",
                        pod->GetId(),
                        podSet->GetId(),
                        *zone);
                    if (!disruptionThrottler->ThrottleEviction(pod))
                    {
                        if (taskManager->GetTaskSlotCount(ETaskSource::AntiaffinityHealer) > 0) {
                            auto task = CreateEvictionTask(HeavyScheduler_->GetClient(),
                                pod,
                                disruptionThrottler->GetValidatePodDisruptionBudget());
                            taskManager->Add(std::move(task), ETaskSource::AntiaffinityHealer);
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
    }

    THashMap<TTopologyZone*, std::vector<TPod*>> GetTopologyZonePods(TPodSet* podSet)
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
    THeavyScheduler* heavyScheduler,
    TAntiaffinityHealerConfigPtr config)
    : Impl_(New<TImpl>(heavyScheduler, std::move(config)))
{ }

TAntiaffinityHealer::~TAntiaffinityHealer()
{ }

void TAntiaffinityHealer::Run(const TClusterPtr& cluster)
{
    Impl_->Run(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
