#ifndef BACKGROUND_ACTIVITY_ORCHID_INL_H_
#error "Direct inclusion of this file is not allowed, include background_activity_orchid.h"
// For the sake of sane code completion.
#include "background_activity_orchid.h"
#endif

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

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
void TBackgroundActivityOrchid<TTaskInfo>::AddPendingTasks(std::vector<TTaskInfoPtr>&& tasks)
{
    auto guard = Guard(SpinLock_);
    for (auto&& task : tasks) {
        PendingTasks_.emplace(task->TaskId, std::move(task));
    }
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskAborted(TGuid taskId)
{
    auto guard = Guard(SpinLock_);
    if (!PendingTasks_.erase(taskId)) {
        YT_LOG_ALERT("Attempted to abort tablet background activity task which is not pending, ignored (TaskId: %v)",
            taskId);
    }
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::OnTaskStarted(TGuid taskId)
{
    auto guard = Guard(SpinLock_);

    if (auto it = PendingTasks_.find(taskId); it != PendingTasks_.end()) {
        auto&& task = it->second;

        {
            auto taskGuard = Guard(task->RuntimeData.SpinLock);
            task->RuntimeData.StartTime = Now();
            task->RuntimeData.ShowStatistics = true;
        }

        RunningTasks_.emplace(taskId, std::move(task));
        PendingTasks_.erase(it);
    } else {
        YT_LOG_ALERT("Attempted to start tablet background activity task which is not pending, ignored (TaskId: %v)",
            taskId);
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
    std::vector<TTaskInfoPtr> failedTasks(FailedTasks_.begin(), FailedTasks_.end());
    std::vector<TTaskInfoPtr> completedTasks(CompletedTasks_.begin(), CompletedTasks_.end());
    guard.Release();

    std::stable_sort(
        pendingTasks.begin(),
        pendingTasks.end(),
        [] (const TTaskInfoPtr& lhs, const TTaskInfoPtr& rhs) {
            return lhs->ComparePendingTasks(*rhs);
        });

    std::stable_sort(
        runningTasks.begin(),
        runningTasks.end(),
        [] (const TTaskInfoPtr& lhs, const TTaskInfoPtr& rhs) {
            // NB: We can read StartTime without lock since StartTime task will
            // not be changed and we have happens before because of SpinLock_.
            return lhs->RuntimeData.StartTime < rhs->RuntimeData.StartTime;
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
    std::deque<TTaskInfoPtr>* deque,
    int maxTaskCount)
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

    if (auto it = RunningTasks_.find(taskId); it != RunningTasks_.end()) {
        if (ssize(*deque) == maxTaskCount) {
            deque->pop_front();
        }

        auto&& task = it->second;
        {
            auto taskGuard = Guard(task->RuntimeData.SpinLock);
            task->RuntimeData.FinishTime = Now();
        }

        deque->push_back(std::move(task));
        RunningTasks_.erase(it);
    } else {
        YT_LOG_ALERT("Attempted to finish tablet background activity task which is not running, ignored (TaskId: %v)",
            taskId);
    }
}

template <class TTaskInfo>
void TBackgroundActivityOrchid<TTaskInfo>::ShrinkDeque(
    std::deque<TTaskInfoPtr>* deque,
    int targetSize)
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

    while (ssize(*deque) > targetSize) {
        deque->pop_front();
    }
}

template <class TTaskInfo>
std::vector<typename TBackgroundActivityOrchid<TTaskInfo>::TTaskInfoPtr>
TBackgroundActivityOrchid<TTaskInfo>::GetFromHashMap(
    const TTaskMap& source) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

    std::vector<TTaskInfoPtr> output;
    output.reserve(source.size());
    for (const auto& [taskId, task] : source) {
        output.push_back(task);
    }

    return output;
}

////////////////////////////////////////////////////////////////////////////////

template <class TStorePtr>
TBackgroundActivityTaskInfoBase::TReaderStatistics::TReaderStatistics(const std::vector<TStorePtr>& stores)
{
    for (const auto& store : stores) {
        ++ChunkCount;
        UncompressedDataSize += store->GetUncompressedDataSize();
        CompressedDataSize += store->GetCompressedDataSize();
        UnmergedRowCount += store->GetRowCount();
        UnmergedDataWeight += store->GetDataWeight();
    }
}

template <class TTaskInfo>
TGuardedTaskInfo<TTaskInfo>::TGuardedTaskInfo(
    TIntrusivePtr<TTaskInfo> info,
    TIntrusivePtr<TBackgroundActivityOrchid<TTaskInfo>> orchid)
    : Info(std::move(info))
    , Orchid_(std::move(orchid))
{ }

template <class TTaskInfo>
TGuardedTaskInfo<TTaskInfo>::~TGuardedTaskInfo()
{
    switch (FinishStatus_) {
        case EBackgroundActivityTaskFinishStatus::Aborted:
            Orchid_->OnTaskAborted(Info->TaskId);
            break;
        case EBackgroundActivityTaskFinishStatus::Failed:
            Orchid_->OnTaskFailed(Info->TaskId);
            break;
        case EBackgroundActivityTaskFinishStatus::Completed:
            Orchid_->OnTaskCompleted(Info->TaskId);
            break;
        default:
            YT_ABORT();
    }
}

template <class TTaskInfo>
TTaskInfo* TGuardedTaskInfo<TTaskInfo>::operator->()
{
    return Info.Get();
}

template <class TTaskInfo>
void TGuardedTaskInfo<TTaskInfo>::OnStarted()
{
    FinishStatus_ = EBackgroundActivityTaskFinishStatus::Completed;
    Orchid_->OnTaskStarted(Info->TaskId);
}

template <class TTaskInfo>
void TGuardedTaskInfo<TTaskInfo>::OnFailed(TError error)
{
    FinishStatus_ = EBackgroundActivityTaskFinishStatus::Failed;
    auto taskGuard = Guard(Info->RuntimeData.SpinLock);
    Info->RuntimeData.Error = std::move(error);
}

template <class TTaskInfo>
bool TGuardedTaskInfo<TTaskInfo>::IsFailed() const
{
    return FinishStatus_ == EBackgroundActivityTaskFinishStatus::Failed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
