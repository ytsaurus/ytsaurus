#include "pod_disruption_budget_controller.h"

#include "pod_disruption_budget.h"
#include "pod.h"
#include "pod_set.h"
#include "cluster.h"
#include "config.h"

#include <yp/server/objects/pod_disruption_budget.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/master/bootstrap.h>

namespace NYP::NServer::NScheduler {

using namespace NServer::NMaster;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPodAvailabilityStatistics
{
    i32 TotalPodCount = 0;
    i32 AvailablePodCount = 0;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TPodAvailabilityStatistics& statistics,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{TotalPodCount: %v, AvailablePodCount: %v}",
        statistics.TotalPodCount,
        statistics.AvailablePodCount);
}

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudgetUpdateQueue
    : public TRefCounted
{
public:
    struct TItem
        : public NYT::TRefTracked<TItem>
    {
        TItem(TObjectId id, TObjectId uuid)
            : Id(std::move(id))
            , Uuid(std::move(uuid))
        { }

        TObjectId Id;
        TObjectId Uuid;
    };

    bool TryEnqueue(TItem item)
    {
        if (Uuids_.contains(item.Uuid)) {
            return false;
        }
        YT_LOG_DEBUG("Pod disruption budget update queue item enqueued (Id: %v, Uuid: %v)",
            item.Id,
            item.Uuid);
        Uuids_.insert(item.Uuid);
        Items_.push(std::move(item));
        return true;
    }

    std::optional<TItem> TryDequeue()
    {
        if (Items_.empty()) {
            return std::nullopt;
        }
        auto item = std::move(Items_.front());
        Items_.pop();
        YT_LOG_DEBUG("Pod disruption budget update queue item dequeued (Id: %v, Uuid: %v)",
            item.Id,
            item.Uuid);
        YT_VERIFY(Uuids_.erase(item.Uuid) == 1);
        return item;
    }

    int GetSize() const
    {
        return Items_.size();
    }

private:
    std::queue<TItem> Items_;
    THashSet<TObjectId> Uuids_;
};

using TPodDisruptionBudgetUpdateQueuePtr = TIntrusivePtr<TPodDisruptionBudgetUpdateQueue>;

////////////////////////////////////////////////////////////////////////////////

class TRateLimiter
    : public TRefCounted
{
public:
    explicit TRateLimiter(int limit)
        : Limit_(limit)
        , Acquired_(0)
    { }

    bool TryAcquire()
    {
        if (Acquired_ >= Limit_) {
            return false;
        }
        ++Acquired_;
        return true;
    }

    void Reset()
    {
        Acquired_ = 0;
    }

private:
    const int Limit_;
    int Acquired_;
};

using TRateLimiterPtr = TIntrusivePtr<TRateLimiter>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudgetController::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TPodDisruptionBudgetControllerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , UpdateQueue_(New<TPodDisruptionBudgetUpdateQueue>())
        , UpdateRateLimiter_(New<TRateLimiter>(Config_->UpdatesPerIteration))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetCurrentInvoker(), SchedulerLoopThread);
    }

    void Run(const TClusterPtr& cluster)
    {
        VERIFY_THREAD_AFFINITY(SchedulerLoopThread);

        try {
            RunImpl(cluster);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error running pod disruption budget controller");
        }
    }

private:
    TBootstrap* const Bootstrap_;
    const TPodDisruptionBudgetControllerConfigPtr Config_;

    NObjects::TTimestamp StatisticsTimestamp_ = NObjects::NullTimestamp;
    THashMap<TObjectId, TPodAvailabilityStatistics> StatisticsPerPodDisruptionBudgetUuid_;

    TPodDisruptionBudgetUpdateQueuePtr UpdateQueue_;
    TRateLimiterPtr UpdateRateLimiter_;

    DECLARE_THREAD_AFFINITY_SLOT(SchedulerLoopThread);

    static bool IsPodAvailable(TPod* pod)
    {
        // TODO(bidzilya): Use pod integral liveness status.
        return pod->GetNode() != nullptr
            && pod->StatusEtc().eviction().state() == NClient::NApi::NProto::EEvictionState::ES_NONE;
    }

    void RunImpl(const TClusterPtr& cluster)
    {
        YT_LOG_DEBUG("Recalculating pod disruption budget statistics");

        StatisticsTimestamp_ = cluster->GetSnapshotTimestamp();
        StatisticsPerPodDisruptionBudgetUuid_.clear();
        for (auto* podSet : cluster->GetPodSets()) {
            auto* podDisruptionBudget = podSet->GetPodDisruptionBudget();
            if (!podDisruptionBudget) {
                continue;
            }
            const auto& uuid = podDisruptionBudget->MetaEtc().uuid();
            auto& statistics = StatisticsPerPodDisruptionBudgetUuid_[uuid];
            for (auto* pod : podSet->Pods()) {
                statistics.TotalPodCount += 1;
                statistics.AvailablePodCount += IsPodAvailable(pod);
            }
        }

        YT_LOG_DEBUG("Building pod disruption budget update queue");

        int enqueuedItemCount = 0;
        for (auto* podDisruptionBudget : cluster->GetPodDisruptionBudgets()) {
            TPodDisruptionBudgetUpdateQueue::TItem item(
                podDisruptionBudget->GetId(),
                podDisruptionBudget->MetaEtc().uuid());
            enqueuedItemCount += UpdateQueue_->TryEnqueue(std::move(item));
        }

        YT_LOG_DEBUG("Enqueued pod disruption budget update queue items (EnqueuedItemCount: %v, QueueSize: %v)",
            enqueuedItemCount,
            UpdateQueue_->GetSize());

        YT_LOG_DEBUG("Spawning pod disruption budget updaters");

        UpdateRateLimiter_->Reset();

        std::vector<TFuture<void>> asyncResults;
        asyncResults.reserve(Config_->UpdateConcurrency);
        for (int index = 0; index < Config_->UpdateConcurrency; ++index) {
            asyncResults.push_back(BIND(&TImpl::UpdaterMain, MakeWeak(this))
                .AsyncVia(GetCurrentInvoker())
                .Run());
        }

        WaitFor(Combine(asyncResults))
            .ThrowOnError();
    }

    static i32 InferAllowedPodDisruptions(
        TPodAvailabilityStatistics statistics,
        const NClient::NApi::NProto::TPodDisruptionBudgetSpec& spec)
    {
        YT_VERIFY(statistics.TotalPodCount >= statistics.AvailablePodCount);
        auto unavailablePodCount = statistics.TotalPodCount - statistics.AvailablePodCount;
        if (spec.max_pods_unavailable() <= unavailablePodCount) {
            return 0;
        }
        return std::min(
            spec.max_pods_unavailable() - unavailablePodCount,
            std::max(0, spec.max_pod_disruptions_between_syncs()));
    }

    void UpdaterMain()
    {
        VERIFY_THREAD_AFFINITY(SchedulerLoopThread);

        while (true) {
            if (!UpdateRateLimiter_->TryAcquire()) {
                break;
            }

            auto optionalItem = UpdateQueue_->TryDequeue();
            if (!optionalItem) {
                break;
            }
            auto item = std::move(*optionalItem);

            auto statisticsTimestamp = StatisticsTimestamp_;
            TPodAvailabilityStatistics statistics;
            if (auto it = StatisticsPerPodDisruptionBudgetUuid_.find(item.Uuid); it != StatisticsPerPodDisruptionBudgetUuid_.end()) {
                statistics = it->second;
            }

            auto Logger = NLogging::TLogger(::NYP::NServer::NScheduler::Logger)
                .AddTag("ObjectId: %v", item.Id)
                .AddTag("ObjectUuid: %v", item.Uuid)
                .AddTag("Statistics: %v", statistics)
                .AddTag("StatisticsTimestamp: %llx", statisticsTimestamp);

            YT_LOG_DEBUG("Updating pod disruption budget");

            try {
                auto transaction = WaitFor(Bootstrap_->GetTransactionManager()->StartReadWriteTransaction())
                    .ValueOrThrow();

                auto* transactionPodDisruptionBudget = transaction->GetPodDisruptionBudget(item.Id);

                transactionPodDisruptionBudget->MetaEtc().ScheduleLoad();
                transactionPodDisruptionBudget->StatusUpdateTimestamp().ScheduleLoad();
                transactionPodDisruptionBudget->Spec().ScheduleLoad();

                if (!transactionPodDisruptionBudget->DoesExist()) {
                    YT_LOG_DEBUG("Skipping pod disruption budget update because transaction object is missing");
                    continue;
                }

                const auto& transactionObjectUuid = transactionPodDisruptionBudget->MetaEtc().Load().uuid();
                if (transactionObjectUuid != item.Uuid) {
                    YT_LOG_DEBUG("Skipping pod disruption budget update because "
                                 "transaction object has different uuid (TransactionObjectUuid: %v)",
                        transactionObjectUuid);
                    continue;
                }

                auto transactionStatusUpdateTimestamp = transactionPodDisruptionBudget->StatusUpdateTimestamp().Load();
                if (transactionStatusUpdateTimestamp > statisticsTimestamp) {
                    YT_LOG_DEBUG("Skipping pod disruption budget update because of "
                                 "concurrent update (TransactionStatusUpdateTimestamp: %llx)",
                        transactionStatusUpdateTimestamp);
                    continue;
                }

                transactionPodDisruptionBudget->UpdateStatus(
                    InferAllowedPodDisruptions(
                        statistics,
                        transactionPodDisruptionBudget->Spec().Load()),
                    "Pod disruption budget is synchronized by the controller");

                WaitFor(transaction->Commit())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error updating pod disruption budget");
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudgetController::TPodDisruptionBudgetController(
    TBootstrap* bootstrap,
    TPodDisruptionBudgetControllerConfigPtr config)
    : Impl_(New<TPodDisruptionBudgetController::TImpl>(
        bootstrap,
        std::move(config)))
{ }

void TPodDisruptionBudgetController::Run(const TClusterPtr& cluster)
{
    Impl_->Run(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
