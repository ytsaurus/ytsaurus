#include "heavy_scheduler.h"

#include "antiaffinity_healer.h"
#include "bootstrap.h"
#include "cluster_reader.h"
#include "config.h"
#include "disruption_throttler.h"
#include "eviction_garbage_collector.h"
#include "helpers.h"
#include "label_filter_evaluator.h"
#include "private.h"
#include "resource_vector.h"
#include "swap_defragmentator.h"
#include "task.h"
#include "task_manager.h"
#include "yt_connector.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/config.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NConcurrency;

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
        , TaskManager_(New<TTaskManager>(Config_->TaskManager))
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
        , EvictionGarbageCollector_(New<TEvictionGarbageCollector>(
            Config_->EvictionGarbageCollector,
            Bootstrap_->GetClient()))
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

    TTaskManagerPtr TaskManager_;
    TDisruptionThrottlerPtr DisruptionThrottler_;

    TSwapDefragmentatorPtr SwapDefragmentator_;
    TAntiaffinityHealerPtr AntiaffinityHealer_;
    TEvictionGarbageCollectorPtr EvictionGarbageCollector_;

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

        TaskManager_->ReconcileState(Cluster_);
        TaskManager_->RemoveFinishedTasks();

        EvictionGarbageCollector_->Run(Cluster_);

        if (!CheckClusterHealth()) {
            // NB: CheckClusterHealth() writes to the log, no need to do it here.
            Profiler.Update(Profiling_.UnhealthyClusterCounter, 1);
            return;
        }

        auto ignorePodIds = TaskManager_->GetInvolvedPodIds();

        {
            auto tasks = SwapDefragmentator_->CreateTasks(
                Cluster_,
                DisruptionThrottler_,
                ignorePodIds,
                TaskManager_->GetTaskSlotCount(ETaskSource::SwapDefragmentator),
                TaskManager_->TaskCount());
            for (auto& task : tasks) {
                TaskManager_->Add(std::move(task), ETaskSource::SwapDefragmentator);
            }
        }

        {
            auto tasks = AntiaffinityHealer_->CreateTasks(
                Cluster_,
                DisruptionThrottler_,
                ignorePodIds,
                TaskManager_->GetTaskSlotCount(ETaskSource::AntiaffinityHealer),
                TaskManager_->TaskCount());
            for (auto& task : tasks) {
                TaskManager_->Add(std::move(task), ETaskSource::AntiaffinityHealer);
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
