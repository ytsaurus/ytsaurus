#include "task_manager.h"

#include "private.h"
#include "config.h"
#include "task.h"

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

class TTaskManager::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TTaskManagerConfigPtr config)
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

    bool HasTaskInvolvingPod(TPod* pod) const
    {
        return InvolvedPodIds_.find(pod->GetId()) != InvolvedPodIds_.end();
    }

    void RemoveFinishedTasks()
    {
        auto now = TInstant::Now();

        TEnumIndexedVector<ETaskSource, int> timedOutCount;
        TEnumIndexedVector<ETaskSource, int> succeededCount;
        TEnumIndexedVector<ETaskSource, int> failedCount;
        TEnumIndexedVector<ETaskSource, int> activeCount;

        for (auto source : TEnumTraits<ETaskSource>::GetDomainValues()) {
            auto& tasks = Tasks_[source];
            auto finishedIt = std::partition(
                tasks.begin(),
                tasks.end(),
                [&] (const ITaskPtr& task) {
                    if (task->GetState() == ETaskState::Succeeded) {
                        ++succeededCount[source];
                        return false;
                    }
                    if (task->GetState() == ETaskState::Failed) {
                        ++failedCount[source];
                        return false;
                    }
                    if (task->GetStartTime() + Config_->TaskTimeLimit < now) {
                        ++timedOutCount[source];
                        YT_LOG_DEBUG("Task time limit exceeded (TaskId: %v, StartTime: %v, TimeLimit: %v)",
                            task->GetId(),
                            task->GetStartTime(),
                            Config_->TaskTimeLimit);
                        return false;
                    }
                    ++activeCount[source];
                    return true;
                });

            tasks.erase(finishedIt, tasks.end());
        }

        InvolvedPodIds_.clear();
        for (const auto& tasks : Tasks_) {
            for (const auto& task : tasks) {
                for (auto& podId : task->GetInvolvedPodIds()) {
                    InvolvedPodIds_.insert(std::move(podId));
                }
            }
        }

        for (auto source : TEnumTraits<ETaskSource>::GetDomainValues()) {
            const char* path;
            switch (source) {
                case ETaskSource::AntiaffinityHealer:
                    path = "/antiaffinity_healer";
                    break;
                case ETaskSource::SwapDefragmentator:
                    path = "/swap_defragmentator";
                    break;
                default:
                    YT_ABORT();
            };
            auto profiler = Profiler_.AppendPath(path);
            auto profiling = Profiling_[source];
            profiler.Update(profiling.TimedOutCounter, timedOutCount[source]);
            profiler.Update(profiling.SucceededCounter, succeededCount[source]);
            profiler.Update(profiling.FailedCounter, failedCount[source]);
            profiler.Update(profiling.ActiveCounter, activeCount[source]);
        }
    }

    void Add(ITaskPtr task, ETaskSource source)
    {
        for (auto& podId : task->GetInvolvedPodIds()) {
            InvolvedPodIds_.insert(std::move(podId));
        }
        Tasks_[source].push_back(std::move(task));
    }

    int TaskCount() const
    {
        int count = 0;
        for (const auto& tasks : Tasks_) {
            count += static_cast<int>(tasks.size());
        }
        return count;
    }

private:
    const TTaskManagerConfigPtr Config_;
    const NProfiling::TProfiler Profiler_;

    TEnumIndexedVector<ETaskSource, std::vector<ITaskPtr>> Tasks_;
    THashSet<TObjectId> InvolvedPodIds_;

    struct TProfiling
    {
        NProfiling::TSimpleGauge TimedOutCounter{"/timed_out"};
        NProfiling::TSimpleGauge SucceededCounter{"/succeeded"};
        NProfiling::TSimpleGauge FailedCounter{"/failed"};
        NProfiling::TSimpleGauge ActiveCounter{"/active"};
    };

    TEnumIndexedVector<ETaskSource, TProfiling> Profiling_;
};

////////////////////////////////////////////////////////////////////////////////

TTaskManager::TTaskManager(TTaskManagerConfigPtr config)
    : Impl_(New<TImpl>(std::move(config)))
{ }


void TTaskManager::RemoveFinishedTasks()
{
    Impl_->RemoveFinishedTasks();
}

void TTaskManager::Add(ITaskPtr task, ETaskSource source)
{
    Impl_->Add(std::move(task), source);
}

void TTaskManager::ReconcileState(const TClusterPtr& cluster)
{
    Impl_->ReconcileState(cluster);
}

int TTaskManager::GetTaskSlotCount(ETaskSource source) const
{
    return Impl_->GetTaskSlotCount(source);
}

bool TTaskManager::HasTaskInvolvingPod(TPod* pod) const
{
    return Impl_->HasTaskInvolvingPod(pod);
}

int TTaskManager::TaskCount() const
{
    return Impl_->TaskCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
