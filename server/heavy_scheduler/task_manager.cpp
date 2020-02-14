#include "task_manager.h"

#include "private.h"
#include "config.h"
#include "task.h"

#include <yp/server/lib/cluster/cluster.h>

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
        for (const auto& tasks : Tasks_) {
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

THashSet<TObjectId> TTaskManager::GetInvolvedPodIds() const
{
    return Impl_->GetInvolvedPodIds();
}

int TTaskManager::TaskCount() const
{
    return Impl_->TaskCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
