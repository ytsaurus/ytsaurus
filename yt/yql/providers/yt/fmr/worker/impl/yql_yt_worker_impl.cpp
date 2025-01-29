#include <library/cpp/yt/assert/assert.h>
#include <thread>
#include <util/system/mutex.h>
#include "yql_yt_worker_impl.h"

namespace NYql {

namespace {

struct TFmrWorkerState {
    TMutex Mutex;
    std::unordered_map<TString, ETaskStatus> TaskStatuses;
};

class TFmrWorker: public IFmrWorker {
public:
    TFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings)
        : Coordinator_(coordinator),
        JobFactory_(jobFactory),
        WorkerState_(std::make_shared<TFmrWorkerState>(TMutex(), std::unordered_map<TString, ETaskStatus>{})),
        StopWorker_(false),
        RandomProvider_(settings.RandomProvider),
        WorkerId_(settings.WorkerId),
        TimeToSleepBetweenRequests_(settings.TimeToSleepBetweenRequests)
{
}

    ~TFmrWorker() {
        Stop();
    }

    void Start() override {
        auto mainThreadFunc = [&] () {
            while (!StopWorker_) {
                std::vector<TTaskState::TPtr> taskStates;
                std::vector<TString> taskIdsToErase;
                with_lock(WorkerState_->Mutex) {
                    for (auto& [taskId, taskStatus]: WorkerState_->TaskStatuses) {
                        if (taskStatus != ETaskStatus::InProgress) {
                            taskIdsToErase.emplace_back(taskId);
                        }
                        taskStates.emplace_back(MakeTaskState(taskStatus, taskId));
                    }
                    for (auto& taskId: taskIdsToErase) {
                        WorkerState_->TaskStatuses.erase(taskId);
                        TasksCancelStatus_.erase(taskId);
                    }
                }

                auto heartbeatRequest = THeartbeatRequest(
                    GetWorkerId(),
                    GetVolatileId(),
                    taskStates,
                    TStatistics()
                );
                auto heartbeatResponseFuture = Coordinator_->SendHeartbeatResponse(heartbeatRequest);
                auto heartbeatResponse = heartbeatResponseFuture.GetValueSync();
                std::vector<TTask::TPtr> tasksToRun = heartbeatResponse.TasksToRun;
                std::unordered_set<TString> taskToDeleteIds = heartbeatResponse.TaskToDeleteIds;

                with_lock(WorkerState_->Mutex) {
                    for (auto task: tasksToRun) {
                        auto taskId = task->TaskId;
                        YT_VERIFY(!WorkerState_->TaskStatuses.contains(taskId));
                        WorkerState_->TaskStatuses[taskId] = ETaskStatus::InProgress;
                        TasksCancelStatus_[taskId] = std::make_shared<std::atomic<bool>>(false);
                    }
                    for (auto& taskToDeleteId: taskToDeleteIds) {
                        YT_VERIFY(TasksCancelStatus_.contains(taskToDeleteId));
                        TasksCancelStatus_[taskToDeleteId]->store(true);
                    }
                }

                for (auto task: tasksToRun) {
                    auto taskId = task->TaskId;
                    auto future = JobFactory_->StartJob(task, TasksCancelStatus_[taskId]);
                    future.Subscribe([weakState = std::weak_ptr(WorkerState_), task](const auto& jobFuture) {
                        auto finalTaskStatus = jobFuture.GetValue();
                        std::shared_ptr<TFmrWorkerState> state = weakState.lock();
                        if (state) {
                            with_lock(state->Mutex) {
                                YT_VERIFY(state->TaskStatuses.contains(task->TaskId));
                                state->TaskStatuses[task->TaskId] = finalTaskStatus;
                            }
                        }
                    });
                }
                Sleep(TimeToSleepBetweenRequests_);
            }
        };
        MainThread_ = std::thread(mainThreadFunc);
    }

    void Stop() override {
        with_lock(WorkerState_->Mutex) {
            for (auto& taskInfo: TasksCancelStatus_) {
                taskInfo.second->store(true);
            }
            StopWorker_ = true;
        }
        if (MainThread_.joinable()) {
            MainThread_.join();
        }
    }

private:
    TString GetWorkerId() {
        return WorkerId_;
    }
    TString GetVolatileId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    IFmrCoordinator::TPtr Coordinator_;
    IFmrJobFactory::TPtr JobFactory_;
    std::unordered_map<TString, std::shared_ptr<std::atomic<bool>>> TasksCancelStatus_;
    std::shared_ptr<TFmrWorkerState> WorkerState_;
    std::atomic<bool> StopWorker_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    TString WorkerId_;
    std::thread MainThread_;
    TDuration TimeToSleepBetweenRequests_;
};

} // namespace

IFmrWorker::TPtr MakeFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings) {
    return MakeIntrusive<TFmrWorker>(coordinator, jobFactory, settings);
}

} // namspace NYql
