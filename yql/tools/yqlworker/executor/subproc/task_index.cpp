#include "task_index.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/proc_alive.h>

#include <util/generic/yexception.h>
#include <util/system/fs.h>
#include <util/system/file.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

using namespace NYql;
using namespace NSubprocessTaskExecutor;

TTaskIndex::TTaskIndex(TString taskProcessTrackDir)
    : TaskProcessTrackDir_(std::move(taskProcessTrackDir))
{
    YQL_LOG(INFO) << "Track dir: " << TaskProcessTrackDir_;
    if (TaskProcessTrackDir_) {
        if (!NFs::MakeDirectoryRecursive(TaskProcessTrackDir_, NFs::FP_NONSECRET_FILE | NFs::FP_GROUP_EXEC | NFs::FP_ALL_EXEC, false)) {
            YQL_LOG(ERROR) << "Cannot create " << TaskProcessTrackDir_;
            TaskProcessTrackDir_ = {};
        }
        else {
            // Cleanup old files from dead cores
            TVector<TString> names;
            auto dir = TFsPath(TaskProcessTrackDir_);
            dir.ListNames(names);

            for (const TString& name: names) {
                auto childPath = dir / name;
                TFileStat stat(childPath, true);
                ui32 oldPid = 0;
                if (stat.IsFile() && TryFromString(name, oldPid)) {
                    if (!IsProcessAlive(oldPid)) {
                        try {
                            childPath.ForceDelete();
                        } catch (...) {
                            YQL_LOG(WARN) << "Error deleting old track file " << childPath
                                 << ": " << CurrentExceptionMessage();
                        }
                    }
                }
            }
        }
    }
}

bool TTaskIndex::PushPendingTask(TInstant now, NProto::ETaskAction action, TTaskDataPtr data, ITaskWatcherPtr watcher) {
    const TString& taskName = data->GetId();
    if (!TaskNames_.emplace(taskName).second) {
        YQL_LOG(ERROR) << "Pending task with name " << taskName << " already registered";
        return false;
    }

    PendingTasks_.emplace(now, action, data, std::move(watcher));
    IncreaseUserTasks(data->GetUsername());
    return true;
}

bool TTaskIndex::RemovePendingTask(const TString& task) {
    TQueue<TPendingTaskInfo> remainingTasks;
    bool removed = false;

    while (!PendingTasks_.empty()) {
        TPendingTaskInfo info = std::move(PendingTasks_.front());
        PendingTasks_.pop();

        if (!removed && info.Data->GetId() == task) {
            TaskNames_.erase(task);
            DecreaseUserTasks(info.Data->GetUsername());
            removed = true;
        } else {
            remainingTasks.push(std::move(info));
        }
    }

    PendingTasks_ = std::move(remainingTasks);
    return removed;
}

bool TTaskIndex::AdoptOrphanTask(pid_t pid, const TString& task, const TString& username, ITaskWatcherPtr watcher) {
    if (TaskNames_.contains(task)) {
        YQL_LOG(ERROR) << "Can not adopt orphan task " << task << ": task with the same name is already registered";
        return false;
    }

    if (pid <= 0) {
        YQL_LOG(ERROR) << "Orphan task " << task << " could not be assigned to process with invalid pid=" << pid;
        return false;
    }

    auto it = PidToTaskName_.find(pid);
    if (it != PidToTaskName_.end()) {
        YQL_LOG(ERROR) << "Can not adopt orphan task " << task << " with pid=" << pid << ": task " << it->second << " is currently running there";
        return false;
    }

    if (!AddRunningTaskInfo(pid, task, username, watcher)) {
        return false;
    }

    TaskNames_.insert(task);
    PidToTaskName_[pid] = task;
    IncreaseUserTasks(username);
    return true;
}


bool TTaskIndex::MarkNextPendingTaskAsRunning(pid_t pid, TInstant& pushTime, NProto::ETaskAction& action, TTaskDataPtr& data) {
    if (PendingTasks_.empty()) {
        return false;
    }

    if (pid <= 0) {
        YQL_LOG(ERROR) << "Pending task could not be assigned to process with invalid pid=" << pid;
        return false;
    }

    auto it = PidToTaskName_.find(pid);
    if (it != PidToTaskName_.end()) {
        YQL_LOG(ERROR) << "Process with pid=" << pid << " is trying to get second task, already assigned task " << it->second;
        return false;
    }

    TPendingTaskInfo& info = PendingTasks_.front();
    const TString& task = info.Data->GetId();
    const TString& username = info.Data->GetUsername();

    if (!AddRunningTaskInfo(pid, task, username, info.Watcher)) {
        return false;
    }

    PidToTaskName_.emplace(pid, task);

    // fill out parameters
    pushTime = info.PushTime;
    action = info.Action;
    data = std::move(info.Data);

    // finally pop pending task
    PendingTasks_.pop();
    return true;
}

bool TTaskIndex::MarkRunningTaskAsFinished(TInstant now, pid_t pid, const TString& task) {
    // task is used to ensure pid is associated with proper task
    auto it1 = PidToTaskName_.find(pid);
    if (it1 == PidToTaskName_.end()) {
        return false;
    }

    if (it1->second != task) {
        YQL_LOG(ERROR) << "Process with pid=" << pid << " is trying to mark task " << task << " as finished, but it has another task " << it1->second;
        return false;
    }

    auto it2 = RunningTasks_.find(task);
    if (it2 == RunningTasks_.end()) {
        YQL_LOG(WARN) << "Process with pid=" << pid << " is trying to mark task " << task << " as finished, but it is not running";
        return false;
    }

    if (it2->second.IsFinished) {
        return false;
    }

    it2->second.IsFinished = true;
    it2->second.FinishTime = now;
    return true;
}

bool TTaskIndex::FindRunningTaskWatcher(const TString& task, ITaskWatcherPtr& watcher, pid_t& pid, bool& isFinished) const {
    auto it = RunningTasks_.find(task);
    if (it == RunningTasks_.end()) {
        return false;
    }

    watcher = it->second.Watcher;
    pid = it->second.Pid;
    isFinished = it->second.IsFinished;
    return true;
}

bool TTaskIndex::HasRunningTask(const TString& task, bool& isFinished) const {
    auto it = RunningTasks_.find(task);
    if (it == RunningTasks_.end()) {
        return false;
    }

    isFinished = it->second.IsFinished;
    return true;
}

bool TTaskIndex::RemoveRunningTask(pid_t pid, TString& username, ITaskWatcherPtr& watcher, bool& isFinished) {
    auto it1 = PidToTaskName_.find(pid);
    if (it1 == PidToTaskName_.end()) {
        return false;
    }

    const TString& taskName = it1->second;

    auto it2 = RunningTasks_.find(taskName);
    if (it2 == RunningTasks_.end()) {
        YQL_LOG(ERROR) << "Process with pid=" << pid << " is trying to remove not registered task " << taskName;
        return false;
    }

    // fill out parameters
    username = std::move(it2->second.Username);
    watcher = std::move(it2->second.Watcher);
    isFinished = it2->second.IsFinished;
    if (it2->second.TrackFile) {
        if (!NFs::Remove(it2->second.TrackFile)) {
            YQL_LOG(WARN) << "Failed to delete track info file " << it2->second.TrackFile
                           << ": " << LastSystemErrorText();
        }
    }

    // finally remove items
    TaskNames_.erase(taskName);
    PidToTaskName_.erase(it1);
    RunningTasks_.erase(it2);
    DecreaseUserTasks(username);

    return true;
}

ui64 TTaskIndex::GetSize() const {
    return TaskNames_.size();
}

ui64 TTaskIndex::GetPendingSize() const {
    return PendingTasks_.size();
}

ui64 TTaskIndex::GetRunningSize() const {
    return RunningTasks_.size();
}

i64 TTaskIndex::GetPendingTaskMaxAgeMs(TInstant now) const {
    if (PendingTasks_.empty()) {
        return 0;
    }

    return (i64)(now - PendingTasks_.front().PushTime).MilliSeconds();
}

ui64 TTaskIndex::GetSizeByUser(const TString& username) const {
    if (const ui64* count = User2TaskCount_.FindPtr(username)) {
        return *count;
    }

    return 0;
}

TVector<ITaskWatcherPtr> TTaskIndex::ExtractAllUnfinishedTaskWatchers() {
    TVector<ITaskWatcherPtr> watchers;
    watchers.reserve(PendingTasks_.size() + RunningTasks_.size());

    while (!PendingTasks_.empty()) {
        const TPendingTaskInfo& info = PendingTasks_.front();
        watchers.push_back(info.Watcher);
        TaskNames_.erase(info.Data->GetId());
        PendingTasks_.pop();
    }

    for (const auto& [taskName, runInfo] : RunningTasks_) {
        if (!runInfo.IsFinished) {
            watchers.push_back(runInfo.Watcher);
        }
        if (runInfo.TrackFile) {
            if (!NFs::Remove(runInfo.TrackFile)) {
                YQL_LOG(WARN) << "Failed to delete track info file " << runInfo.TrackFile
                              << ": " << LastSystemErrorText();
            }
        }
        TaskNames_.erase(taskName);
    }

    RunningTasks_.clear();
    PidToTaskName_.clear();
    User2TaskCount_.clear();

    return watchers;
}

bool TTaskIndex::AddRunningTaskInfo(pid_t pid, const TString& task, const TString& username, ITaskWatcherPtr watcher) {
    TRunningTaskInfo runInfo;
    runInfo.Pid = pid;
    runInfo.Username = username;
    runInfo.Watcher = watcher;

    auto res = RunningTasks_.emplace(task, std::move(runInfo));
    if (!res.second) {
        YQL_LOG(ERROR) << "Failed to assign task " << task << " to process with pid=" << pid;
        return false;
    }

    if (TaskProcessTrackDir_) {
        try {
            res.first->second.TrackFile = TFsPath(TaskProcessTrackDir_) / ToString(static_cast<i64>(pid));
            TFile outFile(res.first->second.TrackFile, CreateAlways | ARW);
            TUnbufferedFileOutput out(outFile);
            out.Write(username);
            outFile.Close();
        } catch (...) {
            YQL_LOG(ERROR) << "Failed to write task process track info. File=" << res.first->second.TrackFile
                           << ", user=" << username << ", err=" << CurrentExceptionMessage();
        }
    }
    return true;
}

void TTaskIndex::IncreaseUserTasks(const TString& username) {
    ++User2TaskCount_[username];
}

void TTaskIndex::DecreaseUserTasks(const TString& username) {
    auto it = User2TaskCount_.find(username);
    if (it == User2TaskCount_.end()) {
        YQL_LOG(ERROR) << "Error in DecreaseUserTasks: user " << username << " have no tasks";
        return;
    }

    Y_ENSURE(it->second > 0);
    --it->second;

    if (it->second == 0) {
        User2TaskCount_.erase(it);
    }
}
