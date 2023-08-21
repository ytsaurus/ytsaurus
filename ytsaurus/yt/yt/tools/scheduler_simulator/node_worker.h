#pragma once

#include "shared_data.h"
#include "config.h"
#include "scheduling_context.h"
#include "control_thread.h"

#include <yt/yt/server/scheduler/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NSchedulerSimulator {

class TSimulatorNodeWorker
    : public NYT::TRefCounted
{
public:
    TSimulatorNodeWorker(
        int id,
        TSharedEventQueue* events,
        TSharedJobAndOperationCounter* jobAndOperationCounter,
        IInvokerPtr commonNodeWorkerInvoker,
        const std::vector<TSimulatorNodeShardPtr>& nodeShards);

    TFuture<void> AsyncRun();

private:
    const int Id_;
    TSharedEventQueue* const Events_;
    TSharedJobAndOperationCounter* const JobAndOperationCounter_;

    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    const std::vector<TSimulatorNodeShardPtr>& NodeShards_;

    void Run();
    void RunOnce();

    void OnHeartbeat(const TNodeEvent& event);
    void OnJobFinished(const TNodeEvent& event);
};

DEFINE_REFCOUNTED_TYPE(TSimulatorNodeWorker)

} // namespace NYT::NSchedulerSimulator
