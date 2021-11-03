#include "quantized_executor.h"

#include "private.h"
#include "action_queue.h"
#include "delayed_executor.h"
#include "nonblocking_queue.h"
#include "scheduler_api.h"
#include "suspendable_action_queue.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/variant.h>

namespace NYT::NConcurrency {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TSuspendableWorker
    : public TRefCounted
{
public:
    TSuspendableWorker(
        TString threadName,
        TCallback<void()> initializer)
        : Queue_(CreateSuspendableActionQueue(std::move(threadName)))
        , Invoker_(Queue_->GetInvoker())
    {
        if (initializer) {
            BIND(initializer)
                .AsyncVia(Queue_->GetInvoker())
                .Run()
                .Get();
        }

        Queue_->Suspend(/*immediately*/ true)
            .Get()
            .ThrowOnError();
    }

    void Start()
    {
        Terminated_ = BIND(&TSuspendableWorker::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Execute(TCallback<void()> callback)
    {
        YT_VERIFY(callback);

        TExecuteCommand command{
            .Callback = std::move(callback)
        };

        auto future = command.Executed.ToFuture();

        CommandQueue_.Enqueue(std::move(command));

        return future;
    }

    TFuture<void> Suspend(bool immediately)
    {
        return Queue_->Suspend(immediately);
    }

    void Resume()
    {
        Queue_->Resume();
    }

    TFuture<void> Terminate()
    {
        CommandQueue_.Enqueue(TTerminateCommand{});

        return Terminated_;
    }

private:
    const ISuspendableActionQueuePtr Queue_;
    const IInvokerPtr Invoker_;

    struct TExecuteCommand
    {
        TCallback<void()> Callback;

        TPromise<void> Executed = NewPromise<void>();
    };

    struct TTerminateCommand
    { };

    using TWorkerCommand = std::variant<
        TExecuteCommand,
        TTerminateCommand
    >;

    TNonblockingQueue<TWorkerCommand> CommandQueue_;

    TFuture<void> Terminated_;

    void DoRun()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        while (true) {
            auto command = WaitFor(CommandQueue_.Dequeue())
                .ValueOrThrow();

            bool terminate = false;
            Visit(command,
                [&] (const TExecuteCommand& executeCommand) {
                    Invoker_->Invoke(BIND([executeCommand] {
                        const auto& callback = executeCommand.Callback;
                        callback.Run();
                        executeCommand.Executed.Set();
                    }));
                },
                [&] (TTerminateCommand) {
                    terminate = true;
                });

            if (terminate) {
                return;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQuantizedExecutor
    : public IQuantizedExecutor
{
public:
    TQuantizedExecutor(
        TString name,
        ICallbackProviderPtr callbackProvider,
        int workerCount)
        : Name_(std::move(name))
        , Logger(ConcurrencyLogger.WithTag("Executor: %v", Name_))
        , ControlQueue_(New<TActionQueue>(Format("%v:C", Name_)))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , CallbackProvider_(std::move(callbackProvider))
        , DesiredWorkerCount_(workerCount)
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("QuantizedExecutor(%v)", Name_),
            BIND(&TQuantizedExecutor::Shutdown, MakeWeak(this)),
            /*priority*/ 200))
    { }

    ~TQuantizedExecutor()
    {
        Shutdown();
    }

    void Initialize(TCallback<void()> workerInitializer) override
    {
        WorkerInitializer_ = std::move(workerInitializer);

        BIND(&TQuantizedExecutor::DoReconfigure, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run()
            .Get()
            .ThrowOnError();
    }

    TFuture<void> Run(TDuration timeout) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        BIND(&TQuantizedExecutor::StartQuantum, MakeStrong(this), timeout)
            .AsyncVia(ControlInvoker_)
            .Run()
            .Get()
            .ThrowOnError();

        return QuantumFinished_.ToFuture();
    }

    void Reconfigure(int workerCount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DesiredWorkerCount_ = workerCount;
    }

private:
    const TString Name_;

    const TLogger Logger;

    const TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;

    ICallbackProviderPtr CallbackProvider_;

    TCallback<void()> WorkerInitializer_;

    std::vector<TIntrusivePtr<TSuspendableWorker>> Workers_;
    int ActiveWorkerCount_ = 0;
    std::atomic<int> DesiredWorkerCount_ = 0;

    int QuantumIndex_ = 0;

    bool FinishingQuantum_ = false;

    bool Running_ = false;
    TPromise<void> QuantumFinished_;

    const TShutdownCookie ShutdownCookie_;

    void Shutdown()
    {
        if (!Running_) {
            for (const auto& worker : Workers_) {
                worker->Resume();
            }
        }

        for (const auto& worker : Workers_) {
            worker->Terminate();
        }
    }

    void DoReconfigure()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);
        YT_VERIFY(!Running_);

        int desiredWorkerCount = DesiredWorkerCount_.load();
        if (ActiveWorkerCount_ == desiredWorkerCount) {
            return;
        }

        int currentWorkerCount = std::ssize(Workers_);

        YT_LOG_DEBUG("Updating worker count (WorkerCount: %v -> %v)",
            currentWorkerCount,
            desiredWorkerCount);

        if (desiredWorkerCount > currentWorkerCount) {
            Workers_.reserve(desiredWorkerCount);
            for (int index = currentWorkerCount; index < desiredWorkerCount; ++index) {
                Workers_.push_back(New<TSuspendableWorker>(Format("%v:%v", Name_, index), WorkerInitializer_));
                Workers_.back()->Start();
            }
        }

        YT_VERIFY(std::ssize(Workers_) >= desiredWorkerCount);
        ActiveWorkerCount_ = desiredWorkerCount;
    }

    void StartQuantum(TDuration timeout)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        DoReconfigure();

        ++QuantumIndex_;

        YT_LOG_TRACE("Starting quantum (Index: %v, Timeout: %v)",
            QuantumIndex_,
            timeout);

        YT_VERIFY(!Running_);
        Running_ = true;

        QuantumFinished_ = NewPromise<void>();
        QuantumFinished_
            .ToFuture()
            .Subscribe(BIND(&TQuantizedExecutor::OnQuantumFinished, MakeWeak(this), QuantumIndex_)
                .Via(ControlInvoker_));

        TDelayedExecutor::Submit(
            BIND(&TQuantizedExecutor::OnTimeoutReached, MakeWeak(this), QuantumIndex_),
            timeout,
            ControlInvoker_);

        ResumeWorkers();

        for (int index = 0; index < std::ssize(Workers_); ++index) {
            OnWorkerReady(index);
        }
    }

    void FinishQuantum(bool immediately)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        if (FinishingQuantum_ && !immediately) {
            return;
        }

        YT_LOG_TRACE("Finishing quantum (Index: %v, Immediately: %v)",
            QuantumIndex_,
            immediately);

        Running_ = false;
        FinishingQuantum_ = true;

        QuantumFinished_.TrySetFrom(SuspendWorkers(immediately));
    }

    TFuture<void> SuspendWorkers(bool immediately)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(Workers_.size());
        for (const auto& worker : Workers_) {
            futures.push_back(worker->Suspend(immediately));
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> TerminateWorkers()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(Workers_.size());
        for (const auto& worker : Workers_) {
            futures.push_back(worker->Terminate());
        }

        return AllSucceeded(std::move(futures));
    }

    void ResumeWorkers()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        for (const auto& worker : Workers_) {
            worker->Resume();
        }
    }

    void OnExecutionCompleted(int workerIndex, const TError& /*error*/)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        OnWorkerReady(workerIndex);
    }

    void OnWorkerReady(int workerIndex)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        // Worker is disabled, do not schedule new callbacks to it.
        if (workerIndex >= ActiveWorkerCount_) {
            return;
        }

        auto callback = CallbackProvider_->ExtractCallback();
        if (!callback) {
            FinishQuantum(/*immediate*/ false);
            return;
        }

        const auto& worker = Workers_[workerIndex];
        worker->Execute(std::move(callback))
            .Subscribe(BIND(&TQuantizedExecutor::OnExecutionCompleted, MakeWeak(this), workerIndex)
                .Via(ControlInvoker_));
    }

    void OnTimeoutReached(int quantumIndex)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        if (quantumIndex != QuantumIndex_) {
            return;
        }

        YT_LOG_TRACE("Quantum timeout reached (Index: %v)",
            quantumIndex);

        FinishQuantum(/*immediate*/ true);
    }

    void OnQuantumFinished(int quantumIndex, const TError& /*error*/)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        FinishingQuantum_ = false;

        YT_LOG_TRACE("Quantum finished (Index: %v)",
            quantumIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

IQuantizedExecutorPtr CreateQuantizedExecutor(
    TString name,
    ICallbackProviderPtr callbackProvider,
    int workerCount)
{
    return New<TQuantizedExecutor>(
        std::move(name),
        std::move(callbackProvider),
        workerCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
