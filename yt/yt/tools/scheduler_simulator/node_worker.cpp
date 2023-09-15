#include "node_worker.h"

#include "node_shard.h"
#include "event_log.h"
#include "operation_controller.h"

namespace NYT::NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NConcurrency;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TSimulatorNodeWorker::TSimulatorNodeWorker(
    int id,
    TSharedEventQueue* events,
    TSharedJobAndOperationCounter* jobAndOperationCounter,
    IInvokerPtr commonNodeWorkerInvoker,
    const std::vector<TSimulatorNodeShardPtr>& nodeShards)
    : Id_(id)
    , Events_(events)
    , JobAndOperationCounter_(jobAndOperationCounter)
    , Invoker_(CreateSerializedInvoker(std::move(commonNodeWorkerInvoker)))
    , Logger(SchedulerSimulatorLogger.WithTag("NodeWorkerId: %v", Id_))
    , NodeShards_(nodeShards)
{ }

TFuture<void> TSimulatorNodeWorker::AsyncRun()
{
    return BIND(&TSimulatorNodeWorker::Run, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TSimulatorNodeWorker::Run()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    while (JobAndOperationCounter_->HasUnfinishedOperations()) {
        RunOnce();
        Yield();
    }

    Events_->OnNodeWorkerSimulationFinished(Id_);
}

void TSimulatorNodeWorker::RunOnce()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto maybeEvent = Events_->PopNodeEvent(Id_);
    if (!maybeEvent) {
        return;
    }

    auto event = *maybeEvent;
    switch (event.Type) {
        case ENodeEventType::Heartbeat: {
            OnHeartbeat(event);
            break;
        }

        case ENodeEventType::JobFinished: {
            OnJobFinished(event);
            break;
        }
    }
}

void TSimulatorNodeWorker::OnHeartbeat(const TNodeEvent& event)
{
    YT_LOG_DEBUG("Processing heartbeat event (NodeId: %v, VirtualTimestamp: %v, ScheduledOutOfBand: %v)",
        event.NodeId,
        event.Time,
        event.ScheduledOutOfBand);

    int shardId = TSimulatorNodeShard::GetNodeShardId(event.NodeId, std::ssize(NodeShards_));
    const auto& nodeShard = NodeShards_[shardId];
    auto future = BIND(&TSimulatorNodeShard::OnHeartbeat, nodeShard, event)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
    WaitFor(future)
        .ThrowOnError();
}

void TSimulatorNodeWorker::OnJobFinished(const TNodeEvent& event)
{
    YT_LOG_DEBUG("Processing job finished event (NodeId: %v, VirtualTimestamp: %v, JobId: %v)",
        event.NodeId,
        event.Time,
        event.Job->GetId());

    int shardId = TSimulatorNodeShard::GetNodeShardId(event.NodeId, std::ssize(NodeShards_));
    const auto& nodeShard = NodeShards_[shardId];
    auto future = BIND(&TSimulatorNodeShard::OnJobFinished, nodeShard, event)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
    WaitFor(future)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
