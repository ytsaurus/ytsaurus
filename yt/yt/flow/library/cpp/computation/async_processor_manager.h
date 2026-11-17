#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/computation_statistics.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A task that #IOutputCollector::Add() sent to an async processor in an epoch.
struct TAsyncTaskRequest
{
    TAsyncProcessorDescriptorPtr Processor;
    TYsonMessagePtr Task;
    //! The swift timer the task descends from.
    TInputTimerConstPtr RootTimer;
};

////////////////////////////////////////////////////////////////////////////////

//! Runs the tasks of the async processors of a computation and keeps their results until an epoch takes them.
//! The methods run on the serialized invoker of the job.
class TAsyncProcessorManager
    : public TRefCounted
{
public:
    TAsyncProcessorManager(IInvokerPtr serializedInvoker, NProfiling::TProfiler profiler, NLogging::TLogger logger);

    //! Throws if a processor with the same name is registered. The processor has the default limits
    //! until #Reconfigure().
    void Register(TAsyncProcessorDescriptorPtr descriptor);

    //! Sets the limits of the processors by name, the default ones if absent, and starts the waiting tasks
    //! that fit. Throws if |specs| names an unregistered processor.
    void Reconfigure(const THashMap<std::string, TDynamicAsyncProcessorSpecPtr>& specs);

    //! The names of the processors that reached their limits, sorted; the swift timers must wait
    //! if there is one.
    std::vector<std::string> GetSaturatedProcessors() const;

    //! Takes the results of the root timers of |allowedRootStreams| that came back, oldest first, within
    //! |maxRows| and |maxByteSize|; the first result is taken even if it alone exceeds |maxByteSize|.
    std::vector<TUntypedCompletedAsyncTaskPtr> TakeReady(
        const THashSet<TStreamId>& allowedRootStreams,
        i64 maxRows,
        i64 maxByteSize);

    //! The results that wait for an epoch, by the streams of their root timers.
    const THashMap<TStreamId, TBatchStatistics>& GetReadyByRootStream() const;

    //! Starts |requests| and the waiting tasks within the limits; call after every commit.
    void Start(std::vector<TAsyncTaskRequest> requests);

    //! Cancels the running tasks and drops the waiting tasks, the results and the callbacks of the processors.
    void Stop();

private:
    struct TProcessor
    {
        TAsyncProcessorDescriptorPtr Descriptor;
        TDynamicAsyncProcessorSpecPtr Spec;
        i64 Running = 0;
        i64 ReadyCount = 0;
        i64 ReadyByteSize = 0;
        //! The tasks that wait for a slot, oldest first.
        std::deque<TAsyncTaskRequest> Pending;
        NProfiling::TCounter StartedCounter;
        NProfiling::TCounter CompletedCounter;
        NProfiling::TCounter FailedCounter;
        NProfiling::TGauge RunningGauge;
        NProfiling::TGauge PendingGauge;
        NProfiling::TGauge ReadyCountGauge;
        NProfiling::TGauge ReadyByteSizeGauge;
    };

    struct TRunningTask
    {
        TAsyncTaskRequest Request;
        TFuture<TYsonMessagePtr> Future;
    };

    const IInvokerPtr SerializedInvoker_;
    const NProfiling::TProfiler Profiler_;
    const NLogging::TLogger Logger;
    const TDynamicAsyncProcessorSpecPtr DefaultSpec_;

    THashMap<std::string, TProcessor> Processors_;
    //! The running tasks by their sequence numbers.
    THashMap<i64, TRunningTask> Running_;
    i64 NextSequence_ = 0;
    //! The results in the order they came back.
    std::deque<TUntypedCompletedAsyncTaskPtr> Ready_;
    THashMap<TStreamId, TBatchStatistics> ReadyByRootStream_;
    bool Stopped_ = false;

    void StartTask(TProcessor& processor, TAsyncTaskRequest request);
    void StartPending(TProcessor& processor);
    void OnCompleted(i64 sequence, const TErrorOr<TYsonMessagePtr>& result);
    //! Adds |task| to the ready statistics of its processor and its root stream, or removes it if |sign| is -1.
    void AccountReady(TProcessor& processor, const TUntypedCompletedAsyncTask& task, int sign);
    static bool CanStart(const TProcessor& processor);
    static void UpdateGauges(const TProcessor& processor);
};

DEFINE_REFCOUNTED_TYPE(TAsyncProcessorManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
