#include <yql/essentials/utils/log/tls_backend.h>
#include <yql/tools/yqlworker/executor/common/runnable_tasks.h>
#include <yql/tools/yqlworker/executor/task_executor.h>

#include <library/cpp/logger/null.h>

#include <util/thread/pool.h>
#include <util/system/spinlock.h>
#include <util/generic/hash.h>


namespace NYql {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TInprocessTaskExecutor
//////////////////////////////////////////////////////////////////////////////
class TInprocessTaskExecutor final: public ITaskExecutor
{
    class TCleanupTaskWrapper final: public IObjectInQueue {
    public:
        TCleanupTaskWrapper(
                TInprocessTaskExecutor* taskExecutor,
                IYqlRunnableTaskPtr task,
                const TString& taskId)
            : TaskExecutor_(taskExecutor)
            , Task_(task)
            , TaskId_(taskId)
        {
        }

        ~TCleanupTaskWrapper() {
            with_lock (TaskExecutor_->RunningTasksLock_) {
                TaskExecutor_->RunningTasks_.erase(TaskId_);
            }
        }

        void Process(void* /* threadSpecificResource */) override {
            THolder<TCleanupTaskWrapper> self(this);
            Task_->Run();
        }

    private:
        TInprocessTaskExecutor* TaskExecutor_;
        IYqlRunnableTaskPtr Task_;
        const TString TaskId_;
    };

    class TSuppressLogsWrapper final: public IObjectInQueue {
    public:
        TSuppressLogsWrapper(THolder<IObjectInQueue>&& slave)
            : Slave_(std::move(slave))
        {
        }

        void Process(void* threadSpecificResource) override {
            NLog::TScopedBackend<TNullLogBackend> logSupressor;
            Slave_->Process(threadSpecificResource);
        }

    private:
        THolder<IObjectInQueue> Slave_;
    };

public:
    TInprocessTaskExecutor(
            const NProto::TTaskExecutorConfig& executorConfig,
            const TMaybe<NProto::TDqConfig>& dqConfig,
            const TGatewaysConfig& gatewaysConfig,
            const TStaticGatewaysConfig& staticGatewaysConfig,
            const TFileStorageConfig& fsConfig,
            IMetricsRegistryPtr metrics)
        : RunnableTaskFactory_(gatewaysConfig, staticGatewaysConfig, fsConfig, executorConfig, dqConfig, std::move(metrics))
        , ThreadsCount(executorConfig.GetThreads())
        , MaxQueueSize(executorConfig.GetQueueMaxSize())
    {
        RunnableTaskFactory_.InitGateways();

        if (ThreadsCount > 0) {
            TasksQueue_.Reset(new TThreadPool(TThreadPoolParams{"TasksQueue"}));
        } else {
            TasksQueue_.Reset(new TAdaptiveThreadPool(TThreadPoolParams{"TasksQueue"}));
        }
    }

    void Startup(TOrphanTaskWatcherFactory factory) override {
        Y_UNUSED(factory);
        TasksQueue_->Start(ThreadsCount, MaxQueueSize);
    }

    void Shutdown(std::function<void()> onIdle) override {
        Y_UNUSED(onIdle);
        TasksQueue_->Stop();
    }

private:
    EProcessTaskStatus ProcessTask(
                NProto::ETaskAction action,
                TTaskDataPtr data,
                ITaskWatcherPtr watcher) override
    {
        const TString taskId = data->GetId();
        IYqlRunnableTaskPtr runnableTask = RunnableTaskFactory_.Create(
                action, std::move(data), std::move(watcher));

        bool added = false;
        with_lock (RunningTasksLock_) {
            auto it = RunningTasks_.emplace(taskId, runnableTask.Get());
            added = it.second;
        }

        if (added) {
            THolder<IObjectInQueue> queueItem(
                    new TCleanupTaskWrapper(this, runnableTask, taskId));

            if (IsHealthCheckTask(taskId)) {
                queueItem.Reset(new TSuppressLogsWrapper(std::move(queueItem)));
            }

            if (TasksQueue_->Add(queueItem.Get())) {
                Y_UNUSED(queueItem.Release());
                return NProto::TProcessTaskResponse::SUBMITED;
            }
            return NProto::TProcessTaskResponse::WORKING_QUEUE_IS_FULL;
        }
        return NProto::TProcessTaskResponse::ALREADY_RUNNING;
    }

    EKillTaskStatus KillTask(const TString& taskId) override {
        with_lock (RunningTasksLock_) {
            auto it = RunningTasks_.find(taskId);
            if (it == RunningTasks_.end()) {
                return NProto::TKillTaskResponse::NOT_RUNNING;
            }

            IYqlRunnableTask* task = it->second;
            task->Abort();
        }

        return NProto::TKillTaskResponse::KILLED;
    }

    bool HasRunningTask(const TString& taskId) const override {
        with_lock (RunningTasksLock_) {
            return RunningTasks_.contains(taskId);
        }
    }

    bool IsReady() override {
        return true;
    }

    ITaskWatcherPtr GetRunningTaskWatcher(const TString& taskId) const override {
        with_lock (RunningTasksLock_) {
            auto it = RunningTasks_.find(taskId);
            if (it != RunningTasks_.end()) {
                return it->second->GetWatcher();
            }
            return {};
        }
    }

    ui32 GetRunningTasksCount() const override {
        with_lock (RunningTasksLock_) {
            return RunningTasks_.size();
        }
    }

private:
    TYqlRunnableTaskFactory RunnableTaskFactory_;
    TAdaptiveLock RunningTasksLock_;
    THashMap<TString, IYqlRunnableTask*> RunningTasks_;

    // must be last field in this struct to prevent live threads access
    // already destroyed data structures
    THolder<IThreadPool> TasksQueue_;
    const size_t ThreadsCount;
    const size_t MaxQueueSize;

};

} // namespace

ITaskExecutorPtr CreateInprocessTaskExecutor(
        const NProto::TTaskExecutorConfig& executorConfig,
        const TMaybe<NProto::TDqConfig>& dqConfig,
        const TGatewaysConfig& gatewaysConfig,
        const TStaticGatewaysConfig& staticGatewaysConfig,
        const TFileStorageConfig& fsConfig,
        IMetricsRegistryPtr metrics)
{
    return MakeIntrusive<TInprocessTaskExecutor>(
                executorConfig, dqConfig, gatewaysConfig, staticGatewaysConfig, fsConfig,
                std::move(metrics));
}

} // namespace NYql
