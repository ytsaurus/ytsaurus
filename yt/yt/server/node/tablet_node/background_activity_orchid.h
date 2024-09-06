#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTaskInfoBase
{
    TGuid TaskId;
    TTabletId TabletId;
    NHydra::TRevision MountRevision = NHydra::NullRevision;
    TString TablePath;
    TString TabletCellBundle;
    TInstant StartTime;
    TInstant FinishTime;
};

void Serialize(const TTaskInfoBase& task, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class TTaskInfo>
class TBackgroundActivityOrchid final
{
public:
    using TTaskMap = THashMap<TGuid, TTaskInfo>;

    explicit TBackgroundActivityOrchid(const TStoreBackgroundActivityOrchidConfigPtr& config);

    void Reconfigure(const TStoreBackgroundActivityOrchidConfigPtr& config);

    void ResetPendingTasks(TTaskMap pendingTasks);

    void ClearPendingTasks();

    void OnTaskStarted(TGuid taskId);
    void OnTaskFailed(TGuid taskId);
    void OnTaskCompleted(TGuid taskId);

    void Serialize(NYson::IYsonConsumer* consumer) const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TTaskMap PendingTasks_;
    TTaskMap RunningTasks_;
    std::deque<TTaskInfo> FailedTasks_;
    std::deque<TTaskInfo> CompletedTasks_;
    int MaxFailedTaskCount_;
    int MaxCompletedTaskCount_;

    void OnTaskFinished(TGuid taskId, std::deque<TTaskInfo>* deque, i64 maxTaskCount);

    void ShrinkDeque(std::deque<TTaskInfo>* deque, i64 targetSize);
    std::vector<TTaskInfo> GetFromHashMap(const TTaskMap& source) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define BACKGROUND_ACTIVITY_ORCHID_INL_H_
#include "background_activity_orchid-inl.h"
#undef BACKGROUND_ACTIVITY_ORCHID_INL_H_
