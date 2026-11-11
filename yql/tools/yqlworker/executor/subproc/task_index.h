#pragma once

#ifdef STATUS_PENDING
#undef STATUS_PENDING
#endif
#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <yql/tools/yqlworker/executor/task_executor.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

#include <util/generic/queue.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>


namespace NYql {
namespace NSubprocessTaskExecutor {
//////////////////////////////////////////////////////////////////////////////
// Task info
//////////////////////////////////////////////////////////////////////////////
struct TPendingTaskInfo {
    TInstant PushTime;
    NProto::ETaskAction Action;
    TTaskDataPtr Data;
    ITaskWatcherPtr Watcher;

    TPendingTaskInfo() = default;

    TPendingTaskInfo(
            TInstant pushTime,
            NProto::ETaskAction action,
            TTaskDataPtr data,
            ITaskWatcherPtr watcher)
        : PushTime(pushTime)
        , Action(action)
        , Data(std::move(data))
        , Watcher(std::move(watcher))
    {
    }
};

struct TRunningTaskInfo {
    pid_t Pid;
    TString Username;
    ITaskWatcherPtr Watcher;
    TInstant FinishTime;
    bool IsFinished;
    TString TrackFile;

    TRunningTaskInfo()
        : Pid(-1)
        , IsFinished(false)
    {
    }
};

class TTaskIndex {
private:
    TSet<TString> TaskNames_;
    TQueue<TPendingTaskInfo> PendingTasks_;
    THashMap<pid_t, TString> PidToTaskName_;
    THashMap<TString, TRunningTaskInfo> RunningTasks_;
    THashMap<TString, ui64> User2TaskCount_;
    TString TaskProcessTrackDir_;

public:
    TTaskIndex(TString taskProcessTrackDir);
    bool PushPendingTask(TInstant now, NProto::ETaskAction action, TTaskDataPtr data, ITaskWatcherPtr watcher);
    bool RemovePendingTask(const TString& task);
    bool AdoptOrphanTask(pid_t pid, const TString& task, const TString& username, ITaskWatcherPtr watcher);
    bool MarkNextPendingTaskAsRunning(pid_t pid, TInstant& pushTime, NProto::ETaskAction& action, TTaskDataPtr& data);
    bool MarkRunningTaskAsFinished(TInstant now, pid_t pid, const TString& task);
    bool FindRunningTaskWatcher(const TString& task, ITaskWatcherPtr& watcher, pid_t& pid, bool& isFinished) const;
    bool HasRunningTask(const TString& task, bool& isFinished) const;
    bool RemoveRunningTask(pid_t pid, TString& username, ITaskWatcherPtr& watcher, bool& isFinished);
    ui64 GetPendingSize() const;
    ui64 GetRunningSize() const;
    ui64 GetSize() const;
    i64 GetPendingTaskMaxAgeMs(TInstant now) const;
    ui64 GetSizeByUser(const TString& username) const;
    TVector<ITaskWatcherPtr> ExtractAllUnfinishedTaskWatchers();

private:
    bool AddRunningTaskInfo(pid_t pid, const TString& task, const TString& username, ITaskWatcherPtr watcher);
    void IncreaseUserTasks(const TString& username);
    void DecreaseUserTasks(const TString& username);
};
}
}
