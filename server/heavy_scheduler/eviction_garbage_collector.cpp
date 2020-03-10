#include "eviction_garbage_collector.h"

#include "config.h"
#include "heavy_scheduler.h"
#include "private.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_set.h>

#include <yp/client/api/native/client.h>
#include <yp/client/api/native/helpers.h>
#include <yp/client/api/native/request.h>
#include <yp/client/api/native/response.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NClient::NApi::NNative;
using namespace NClient::NApi;

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TEvictionGarbageCollector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        THeavyScheduler* heavyScheduler,
        TEvictionGarbageCollectorConfigPtr config)
        : HeavyScheduler_(heavyScheduler)
        , Config_(std::move(config))
        , Profiler_(TProfiler(NHeavyScheduler::Profiler)
            .AppendPath("/eviction_garbage_collector"))
    { }

    void Run(const TClusterPtr& cluster)
    {
        try {
            GuardedRun(cluster);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error running eviction garbage collector");
        }
    }

private:
    THeavyScheduler* const HeavyScheduler_;
    const TEvictionGarbageCollectorConfigPtr Config_;
    const TProfiler Profiler_;

    TSimpleGauge CandidateCounter_{"/candidate"};
    TSimpleGauge SuccessfullyCollectedCounter_{"/successfully_collected"};
    TSimpleGauge UnsuccessfullyCollectedCounter_{"/unsuccessfully_collected"};

    bool CheckCandidate(const NProto::TPodStatus_TEviction& eviction, TInstant now)
    {
        return eviction.state() == NProto::EEvictionState::ES_REQUESTED
            && eviction.reason() == NProto::EEvictionReason::ER_SCHEDULER
            && TInstant::MicroSeconds(eviction.last_updated()) + Config_->TimeLimit < now;
    }

    void GuardedRun(const TClusterPtr& cluster)
    {
        int candidateCount = 0;
        int successfullyCollectedCount = 0;
        int unsuccessfullyCollectedCount = 0;

        TInstant now = TInstant::Now();

        std::vector<TObjectId> candidateIds;
        for (auto* pod : cluster->GetPods()) {
            if (CheckCandidate(pod->Eviction(), now)) {
                YT_LOG_DEBUG("Found eviction garbage collection candidate (PodSetId: %v, PodId: %v, PodUuid: %v)",
                    pod->GetPodSet()->GetId(),
                    pod->GetId(),
                    pod->Uuid());
                candidateIds.push_back(pod->GetId());
                ++candidateCount;
            }
        }

        for (const auto& candidateId : candidateIds) {
            try {
                auto startTransactionResult = WaitFor(HeavyScheduler_->GetClient()->StartTransaction())
                    .ValueOrThrow();
                const auto& transactionId = startTransactionResult.TransactionId;
                auto startTimestamp = startTransactionResult.StartTimestamp;

                TGetObjectOptions options;
                options.IgnoreNonexistent = true;
                options.Timestamp = startTimestamp;

                auto payloads = WaitFor(HeavyScheduler_->GetClient()->GetObject(
                    candidateId,
                    EObjectType::Pod,
                    {"/status/eviction"},
                    options))
                    .ValueOrThrow()
                    .Result
                    .ValuePayloads;

                if (payloads.empty()) {
                    YT_LOG_DEBUG("Eviction garbage collection candidate no longer exists (PodId: %v)",
                        candidateId);
                    continue;
                }

                NProto::TPodStatus_TEviction eviction;
                ParsePayloads(
                    payloads,
                    &eviction);

                if (!CheckCandidate(eviction, now)) {
                    YT_LOG_DEBUG("Eviction garbage collection candidate is no longer viable (PodId: %v)",
                        candidateId);
                    continue;
                }

                WaitFor(AbortPodEviction(
                    HeavyScheduler_->GetClient(),
                    candidateId,
                    "Garbage collecting Heavy Scheduler eviction",
                    transactionId))
                    .ValueOrThrow();

                WaitFor(HeavyScheduler_->GetClient()->CommitTransaction(transactionId))
                    .ValueOrThrow();

                YT_LOG_DEBUG("Successfully garbage collected eviction (PodId: %v)",
                    candidateId);

                ++successfullyCollectedCount;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error garbage collecting eviction (PodId: %v)",
                    candidateId);
                ++unsuccessfullyCollectedCount;
            }
        }

        Profiler_.Update(CandidateCounter_, candidateCount);
        Profiler_.Update(SuccessfullyCollectedCounter_, successfullyCollectedCount);
        Profiler_.Update(UnsuccessfullyCollectedCounter_, unsuccessfullyCollectedCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEvictionGarbageCollector::TEvictionGarbageCollector(
    THeavyScheduler* heavyScheduler,
    TEvictionGarbageCollectorConfigPtr config)
    : Impl_(New<TEvictionGarbageCollector::TImpl>(heavyScheduler, std::move(config)))
{ }

TEvictionGarbageCollector::~TEvictionGarbageCollector()
{ }

void TEvictionGarbageCollector::Run(const TClusterPtr& cluster)
{
    Impl_->Run(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
