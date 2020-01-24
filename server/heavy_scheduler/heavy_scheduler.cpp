#include "heavy_scheduler.h"

#include "antiaffinity_healer.h"
#include "bootstrap.h"
#include "cluster_reader.h"
#include "config.h"
#include "disruption_throttler.h"
#include "helpers.h"
#include "label_filter_evaluator.h"
#include "private.h"
#include "resource_vector.h"
#include "swap_defragmentator.h"
#include "task.h"
#include "yt_connector.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/config.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTaskManager
{
public:
    explicit TTaskManager(TTaskManagerConfigPtr config)
        : Config_(std::move(config))
        , Profiler_(NProfiling::TProfiler(NHeavyScheduler::Profiler)
            .AppendPath("/task_manager"))
    { }

    int GetTaskSlotCount(ETaskSource source) const
    {
        return Config_->TaskSlotsPerSource[source] - static_cast<int>(Tasks_[source].size());
    }

    void ReconcileState(const TClusterPtr& cluster)
    {
        for (const auto& tasks : Tasks_) {
            for (const auto& task : tasks) {
                task->ReconcileState(cluster);
            }
        }
    }

    THashSet<TObjectId> GetInvolvedPodIds() const
    {
        THashSet<TObjectId> podIds;
        for (const auto& tasks : Tasks_) {
            for (const auto& task : tasks) {
                for (auto podId : task->GetInvolvedPodIds()) {
                    YT_VERIFY(podIds.insert(std::move(podId)).second);
                }
            }
        }
        return podIds;
    }

    void RemoveFinishedTasks()
    {
        auto now = TInstant::Now();

        int timedOutCount = 0;
        int succeededCount = 0;
        int failedCount = 0;
        int activeCount = 0;

        for (auto& tasks : Tasks_) {
            auto finishedIt = std::partition(
                tasks.begin(),
                tasks.end(),
                [&] (const ITaskPtr& task) {
                    if (task->GetState() == ETaskState::Succeeded) {
                        ++succeededCount;
                        return false;
                    }
                    if (task->GetState() == ETaskState::Failed) {
                        ++failedCount;
                        return false;
                    }
                    if (task->GetStartTime() + Config_->TaskTimeLimit < now) {
                        ++timedOutCount;
                        YT_LOG_DEBUG("Task time limit exceeded (TaskId: %v, StartTime: %v, TimeLimit: %v)",
                            task->GetId(),
                            task->GetStartTime(),
                            Config_->TaskTimeLimit);
                        return false;
                    }
                    ++activeCount;
                    return true;
                });

            tasks.erase(finishedIt, tasks.end());
        }

        Profiler_.Update(Profiling_.TimedOutCounter, timedOutCount);
        Profiler_.Update(Profiling_.SucceededCounter, succeededCount);
        Profiler_.Update(Profiling_.FailedCounter, failedCount);
        Profiler_.Update(Profiling_.ActiveCounter, activeCount);
    }

    void Add(ITaskPtr task, ETaskSource source)
    {
        Tasks_[source].push_back(std::move(task));
    }

    int TaskCount() const
    {
        int count = 0;
        for (auto& tasks : Tasks_) {
            count += static_cast<int>(tasks.size());
        }
        return count;
    }

private:
    const TTaskManagerConfigPtr Config_;
    const NProfiling::TProfiler Profiler_;

    TEnumIndexedVector<ETaskSource, std::vector<ITaskPtr>> Tasks_;

    struct TProfiling
    {
        NProfiling::TSimpleGauge TimedOutCounter{"/timed_out"};
        NProfiling::TSimpleGauge SucceededCounter{"/succeeded"};
        NProfiling::TSimpleGauge FailedCounter{"/failed"};
        NProfiling::TSimpleGauge ActiveCounter{"/active"};
    };

    TProfiling Profiling_;
};

////////////////////////////////////////////////////////////////////////////////

class THeavyScheduler::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TBootstrap* bootstrap,
        THeavySchedulerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , IterationExecutor_(New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TImpl::RunIteration, MakeWeak(this)),
            Config_->IterationPeriod))
        , Cluster_(New<TCluster>(
            Logger,
            NProfiling::TProfiler(NHeavyScheduler::Profiler)
                .AppendPath("/cluster"),
            CreateClusterReader(
                Config_->ClusterReader,
                Bootstrap_->GetClient()),
            CreateLabelFilterEvaluator()))
        , TaskManager_(Config_->TaskManager)
        , DisruptionThrottler_(New<TDisruptionThrottler>(
            Config_->DisruptionThrottler,
            Config_->Verbose))
        , SwapDefragmentator_(New<TSwapDefragmentator>(
            Config_->SwapDefragmentator,
            Bootstrap_->GetClient(),
            Config_->NodeSegment,
            Config_->Verbose))
        , AntiaffinityHealer_(New<TAntiaffinityHealer>(
            Config_->AntiaffinityHealer,
            Bootstrap_->GetClient(),
            Config_->Verbose))
    { }

    void Initialize()
    {
        IterationExecutor_->Start();
    }

private:
    TBootstrap* const Bootstrap_;
    const THeavySchedulerConfigPtr Config_;

    DECLARE_THREAD_AFFINITY_SLOT(IterationThread);
    const TPeriodicExecutorPtr IterationExecutor_;

    TClusterPtr Cluster_;

    TTaskManager TaskManager_;
    TDisruptionThrottlerPtr DisruptionThrottler_;

    TSwapDefragmentatorPtr SwapDefragmentator_;
    TAntiaffinityHealerPtr AntiaffinityHealer_;

    struct TProfiling
    {
        NProfiling::TSimpleGauge UnhealthyClusterCounter{"/unhealthy_cluster"};
    };

    TProfiling Profiling_;

    void RunIteration()
    {
        VERIFY_THREAD_AFFINITY(IterationThread);

        // This check is just a best-effort. It is possible to have more than one running iteration.
        //
        // Generally mechanism of prerequisite transactions can provide guarantee of no more than one
        // running iteration, but YP master storage does not support it yet.
        if (!Bootstrap_->GetYTConnector()->IsLeading()) {
            YT_LOG_DEBUG("Instance is not leading; skipping Heavy Scheduler iteration");
            return;
        }

        try {
            YT_LOG_DEBUG("Starting Heavy Scheduler iteration");
            GuardedRunIteration();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error running Heavy Scheduler iteration");
        }
    }

    void GuardedRunIteration()
    {
        Cluster_->LoadSnapshot(New<TClusterConfig>());

        DisruptionThrottler_->ReconcileState(Cluster_);

        TaskManager_.ReconcileState(Cluster_);
        TaskManager_.RemoveFinishedTasks();

        if (!CheckClusterHealth()) {
            // NB: CheckClusterHealth() writes to the log, no need to do it here.
            Profiler.Update(Profiling_.UnhealthyClusterCounter, 1);
            return;
        }

        auto ignorePodIds = TaskManager_.GetInvolvedPodIds();

        {
            auto tasks = SwapDefragmentator_->CreateTasks(
                Cluster_,
                DisruptionThrottler_,
                ignorePodIds,
                TaskManager_.GetTaskSlotCount(ETaskSource::SwapDefragmentator),
                TaskManager_.TaskCount());
            for (auto& task : tasks) {
                TaskManager_.Add(std::move(task), ETaskSource::SwapDefragmentator);
            }
        }

        {
            auto tasks = AntiaffinityHealer_->CreateTasks(
                Cluster_,
                DisruptionThrottler_,
                ignorePodIds,
                TaskManager_.GetTaskSlotCount(ETaskSource::AntiaffinityHealer),
                TaskManager_.TaskCount());
            for (auto& task : tasks) {
                TaskManager_.Add(std::move(task), ETaskSource::AntiaffinityHealer);
            }
        }
    }

    bool CheckClusterHealth() const
    {
        auto clusterPodEvictionCount = DisruptionThrottler_->EvictionCount();
        if (clusterPodEvictionCount > Config_->SafeClusterPodEvictionCount) {
            YT_LOG_WARNING("Cluster is unhealthy (EvictionCount: %v, SafeEvictionCount: %v)",
                clusterPodEvictionCount,
                Config_->SafeClusterPodEvictionCount);
            return false;
        }
        YT_LOG_DEBUG("Cluster is healthy (EvictionCount: %v)",
            clusterPodEvictionCount);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

THeavyScheduler::THeavyScheduler(
    TBootstrap* bootstrap,
    THeavySchedulerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void THeavyScheduler::Initialize()
{
    Impl_->Initialize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
