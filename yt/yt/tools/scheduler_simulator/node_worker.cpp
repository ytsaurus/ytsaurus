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
    , Logger(SchedulerSimulatorLogger().WithTag("NodeWorkerId", Id_))
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
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    while (JobAndOperationCounter_->HasUnfinishedOperations()) {
        RunOnce();
        Yield();
    }

    Events_->OnNodeWorkerSimulationFinished(Id_);
}

void TSimulatorNodeWorker::RunOnce()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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

        case ENodeEventType::AllocationFinished: {
            OnAllocationFinished(event);
            break;
        }
    }
}

void TSimulatorNodeWorker::OnHeartbeat(const TNodeEvent& event)
{
    YT_TLOG_DEBUG("Processing heartbeat event")
        .With("NodeId", event.NodeId)
        .With("VirtualTimestamp", event.Time)
        .With("ScheduledOutOfBand", event.ScheduledOutOfBand);

    int shardId = TSimulatorNodeShard::GetNodeShardId(event.NodeId, std::ssize(NodeShards_));
    const auto& nodeShard = NodeShards_[shardId];
    auto future = BIND(&TSimulatorNodeShard::OnHeartbeat, nodeShard, event)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
    WaitFor(future)
        .ThrowOnError();
}

void TSimulatorNodeWorker::OnAllocationFinished(const TNodeEvent& event)
{
    YT_TLOG_DEBUG("Processing allocation finished event")
        .With("NodeId", event.NodeId)
        .With("VirtualTimestamp", event.Time)
        .With("AllocationId", event.Allocation->GetId());

    int shardId = TSimulatorNodeShard::GetNodeShardId(event.NodeId, std::ssize(NodeShards_));
    const auto& nodeShard = NodeShards_[shardId];
    auto future = BIND(&TSimulatorNodeShard::OnAllocationFinished, nodeShard, event)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
    WaitFor(future)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
