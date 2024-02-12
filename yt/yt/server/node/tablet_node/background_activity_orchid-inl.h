#ifndef BACKGROUND_ACTIVITY_ORCHID_INL_H_
#error "Direct inclusion of this file is not allowed, include background_activity_orchid.h"
// For the sake of sane code completion.
#include "background_activity_orchid.h"
#endif

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TTaskInfo>
TBackgroundActivityOrchid<TTaskInfo>::TBackgroundActivityOrchid(
    const TStoreBackgroundActivityOrchidConfigPtr& config)
{
    Reconfigure(config);
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::Reconfigure(
    const TStoreBackgroundActivityOrchidConfigPtr& config)
{
    auto guard = Guard(SpinLock_);

    MaxFailedTaskCount_ = config->MaxFailedTaskCount;
    MaxCompletedTaskCount_ = config->MaxCompletedTaskCount;
    ShrinkDeque(&FailedTasks_, MaxFailedTaskCount_);
    ShrinkDeque(&CompletedTasks_, MaxCompletedTaskCount_);
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::ResetPendingTasks(TTaskMap pendingTasks)
{
    auto guard = Guard(SpinLock_);
    PendingTasks_ = std::move(pendingTasks);
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::ClearPendingTasks()
{
    auto guard = Guard(SpinLock_);
    PendingTasks_.clear();
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskStarted(TGuid taskId)
{
    auto guard = Guard(SpinLock_);
    if (auto it = PendingTasks_.find(taskId); it != PendingTasks_.end()) {
        it->second.StartTime = Now();

        RunningTasks_.emplace(taskId, std::move(it->second));
        PendingTasks_.erase(it);
    }
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskFailed(TGuid taskId)
{
    auto guard = Guard(SpinLock_);
    OnTaskFinished(taskId, &FailedTasks_, MaxFailedTaskCount_);
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskCompleted(TGuid taskId)
{
    auto guard = Guard(SpinLock_);
    OnTaskFinished(taskId, &CompletedTasks_, MaxCompletedTaskCount_);
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::Serialize(NYson::IYsonConsumer* consumer) const
{
    auto guard = Guard(SpinLock_);
    auto pendingTasks = GetFromHashMap(PendingTasks_);
    auto runningTasks = GetFromHashMap(RunningTasks_);
    std::vector<TTaskInfo> failedTasks(FailedTasks_.begin(), FailedTasks_.end());
    std::vector<TTaskInfo> completedTasks(CompletedTasks_.begin(), CompletedTasks_.end());
    guard.Release();

    std::stable_sort(
        pendingTasks.begin(),
        pendingTasks.end(),
        [] (const TTaskInfo& lhs, const TTaskInfo& rhs) {
            return lhs.ComparePendingTasks(rhs);
        });

    std::stable_sort(
        runningTasks.begin(),
        runningTasks.end(),
        [] (const TTaskInfo& lhs, const TTaskInfo& rhs) {
            return lhs.StartTime < rhs.StartTime;
        });

    #define ITERATE_TASK_VECTORS(XX) XX(pending) XX(running) XX(failed) XX(completed)
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(kind) .Item(#kind"_task_count").Value(ssize(kind##Tasks))
            ITERATE_TASK_VECTORS(XX)
            #undef XX
            #define XX(kind) .Item(#kind"_tasks").List(kind##Tasks)
            ITERATE_TASK_VECTORS(XX)
            #undef XX
        .EndMap();
    #undef ITERATE_TASK_VECTORS
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskFinished(
    TGuid taskId,
    std::deque<TTaskInfo>* deque,
    i64 maxTaskCount)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (auto it = RunningTasks_.find(taskId); it != RunningTasks_.end()) {
        if (ssize(*deque) == maxTaskCount) {
            deque->pop_front();
        }

        it->second.FinishTime = Now();
        deque->push_back(std::move(it->second));
        RunningTasks_.erase(it);
    }
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::ShrinkDeque(
    std::deque<TTaskInfo>* deque,
    i64 targetSize)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    while (ssize(*deque) > targetSize) {
        deque->pop_front();
    }
}

template <class TTaskInfo>
std::vector<TTaskInfo> TBackgroundActivityOrchid<TTaskInfo>::GetFromHashMap(
    const TTaskMap& source) const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    std::vector<TTaskInfo> output;
    output.reserve(source.size());
    for (const auto& [taskId, task] : source) {
        output.push_back(task);
    }

    return output;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
