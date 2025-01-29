#include <library/cpp/yt/assert/assert.h>
#include <thread>
#include "yql_yt_coordinator_impl.h"

namespace NYql {

namespace {

struct TCoordinatorTaskInfo {
    TTask::TPtr Task;
    ETaskStatus TaskStatus;
    TString OperationId;
};

struct TOperationInfo {
    std::unordered_set<TString> TaskIds; // for now each operation consists only of one task, until paritioner is implemented
    EOperationStatus OperationStatus;
};

struct TIdempotencyKeyInfo {
    TString operationId;
    TInstant OperationCreationTime;
};

class TFmrCoordinator: public IFmrCoordinator {
public:
    TFmrCoordinator(const TFmrCoordinatorSettings& settings)
        : WorkersNum_(settings.WorkersNum),
        RandomProvider_(settings.RandomProvider),
        StopCoordinator_(false),
        TimeToSleepBetweenClearKeyRequests_(settings.TimeToSleepBetweenClearKeyRequests),
        IdempotencyKeyStoreTime_(settings.IdempotencyKeyStoreTime)
    {
        StartClearingIdempotencyKeys();
    }

    ~TFmrCoordinator() {
        StopCoordinator_ = true;
        ClearIdempotencyKeysThread_.join();
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        TMaybe<TString> IdempotencyKey = request.IdempotencyKey;
        if (IdempotencyKey && IdempotencyKeys_.contains(*IdempotencyKey)) {
            auto operationId = IdempotencyKeys_[*IdempotencyKey].operationId;
            auto& operationInfo = Operations_[operationId];
            return NThreading::MakeFuture(TStartOperationResponse(operationInfo.OperationStatus, operationId));
        }
        auto operationId = GenerateId();
        if (IdempotencyKey) {
            IdempotencyKeys_[*IdempotencyKey] = TIdempotencyKeyInfo{.operationId = operationId, .OperationCreationTime=TInstant::Now()};
        }

        TString taskId = GenerateId();
        TTask::TPtr createdTask = MakeTask(request.TaskType, taskId, request.TaskParams);

        Tasks_[taskId] = TCoordinatorTaskInfo{.Task = createdTask, .TaskStatus = ETaskStatus::Accepted, .OperationId = operationId};

        Operations_[operationId] = {.TaskIds = {taskId}, .OperationStatus = EOperationStatus::Accepted};
        return NThreading::MakeFuture(TStartOperationResponse(EOperationStatus::Accepted, operationId));
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        auto operationStatus = Operations_.contains(operationId) ? Operations_[operationId].OperationStatus : EOperationStatus::NotFound;
        return NThreading::MakeFuture(TGetOperationResponse(operationStatus));
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) override {
        TGuard<TMutex> guard(Mutex_);
        auto operationId = request.OperationId;
        if (!Operations_.contains(operationId)) {
            return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::NotFound));
        }
        auto taskIds = Operations_[operationId].TaskIds;
        YT_VERIFY(taskIds.size() == 1);
        auto taskId = *taskIds.begin();
        YT_VERIFY(Tasks_.contains(taskId));

        auto taskStatus = Tasks_[taskId].TaskStatus;
        if (taskStatus == ETaskStatus::InProgress) {
            TaskToDeleteIds_.insert(taskId); // Task is currently running, send signal to worker to cancel
        } else {
            ClearTask(taskId); // Task either hasn't begun running or finished, remove info
        }

        return NThreading::MakeFuture(TDeleteOperationResponse(EOperationStatus::Aborted));
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) override {
        TGuard<TMutex> guard(Mutex_);

        for (auto& requestTaskState: request.TaskStates) {
            auto taskId = requestTaskState->TaskId;
            YT_VERIFY(Tasks_.contains(taskId));
            auto taskStatus = requestTaskState->TaskStatus;
            YT_VERIFY(taskStatus != ETaskStatus::Accepted);
            SetTaskStatus(taskId, taskStatus);
            if (TaskToDeleteIds_.contains(taskId) && Tasks_[taskId].TaskStatus != ETaskStatus::InProgress) {
                ClearTask(taskId); // Task finished, so we don't need to cancel it, just remove info
            }
        }

        std::vector<TTask::TPtr> tasksToRun;
        for (auto& taskToRunInfo: Tasks_) {
            if (taskToRunInfo.second.TaskStatus == ETaskStatus::Accepted) {
                SetTaskStatus(taskToRunInfo.first, ETaskStatus::InProgress);
                tasksToRun.emplace_back(taskToRunInfo.second.Task);
            }
        }
        return NThreading::MakeFuture(THeartbeatResponse{.TasksToRun = tasksToRun, .TaskToDeleteIds = TaskToDeleteIds_});
    }

private:

    void StartClearingIdempotencyKeys() {
        auto ClearIdempotencyKeysFunc = [&] () {
            while (!StopCoordinator_) {
                with_lock(Mutex_) {
                    auto currentTime = TInstant::Now();
                    for (auto it = IdempotencyKeys_.begin(); it != IdempotencyKeys_.end();) {
                        auto operationCreationTime = it->second.OperationCreationTime;
                        auto operationId = it->second.operationId;
                        if (currentTime - operationCreationTime > IdempotencyKeyStoreTime_) {
                            it = IdempotencyKeys_.erase(it);
                            if (Operations_.contains(operationId)) {
                                auto& operationInfo = Operations_[operationId];
                                auto operationStatus = operationInfo.OperationStatus;
                                auto& taskIds = operationInfo.TaskIds;
                                YT_VERIFY(taskIds.size() == 1);
                                auto taskId = *operationInfo.TaskIds.begin();
                                if (operationStatus != EOperationStatus::Accepted && operationStatus != EOperationStatus::InProgress) {
                                    ClearTask(taskId);
                                }
                            }
                        } else {
                            ++it;
                        }
                    }
                }
                Sleep(TimeToSleepBetweenClearKeyRequests_);
            }
        };
        ClearIdempotencyKeysThread_ = std::thread(ClearIdempotencyKeysFunc);
    }

    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    void ClearTask(const TString& taskId) {
        YT_VERIFY(Tasks_.contains(taskId));
        auto& taskInfo = Tasks_[taskId];
        TaskToDeleteIds_.erase(taskId);
        Operations_.erase(taskInfo.OperationId);
        Tasks_.erase(taskId);
    }

    void SetTaskStatus(const TString& taskId, ETaskStatus newTaskStatus) {
        auto& taskInfo = Tasks_[taskId];
        YT_VERIFY(Operations_.contains(taskInfo.OperationId));
        auto& operationInfo = Operations_[taskInfo.OperationId];
        taskInfo.TaskStatus = newTaskStatus;
        operationInfo.OperationStatus = GetOperationStatus(taskInfo.OperationId);
    }

    EOperationStatus GetOperationStatus(const TString& operationId) {
        if (! Operations_.contains(operationId)) {
            return EOperationStatus::NotFound;
        }
        std::unordered_set<TString> taskIds = Operations_[operationId].TaskIds;
        YT_VERIFY(taskIds.size() == 1);

        auto taskId = *taskIds.begin();
        ETaskStatus taskStatus = Tasks_[taskId].TaskStatus;
        return static_cast<EOperationStatus>(taskStatus);
    }

    std::unordered_map<TString, TCoordinatorTaskInfo> Tasks_; // TaskId -> current info about it
    std::unordered_set<TString> TaskToDeleteIds_; // TaskIds we want to pass to worker for deletion
    std::unordered_map<TString, TOperationInfo> Operations_; // OperationId -> current info about it
    std::unordered_map<TString, TIdempotencyKeyInfo> IdempotencyKeys_; // IdempotencyKey -> current info about it

    TMutex Mutex_;
    [[maybe_unused]] ui32 WorkersNum_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    std::thread ClearIdempotencyKeysThread_;
    std::atomic<bool> StopCoordinator_;
    TDuration TimeToSleepBetweenClearKeyRequests_;
    TDuration IdempotencyKeyStoreTime_;
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings) {
    return MakeIntrusive<TFmrCoordinator>(settings);
}

} // namespace NYql
