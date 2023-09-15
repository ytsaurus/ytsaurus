#include "control_thread.h"

#include "operation_controller.h"
#include "node_shard.h"
#include "node_worker.h"

#include <yt/yt/server/scheduler/fair_share_strategy.h>
#include <yt/yt/server/scheduler/fair_share_tree.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using std::placeholders::_1;

////////////////////////////////////////////////////////////////////////////////

TControlThreadEvent::TControlThreadEvent(EControlThreadEventType type, TInstant time)
    : Type(type)
    , Time(time)
    , OperationId()
{ }

TControlThreadEvent TControlThreadEvent::OperationStarted(TInstant time, TOperationId id)
{
    TControlThreadEvent event(EControlThreadEventType::OperationStarted, time);
    event.OperationId = id;
    return event;
}

TControlThreadEvent TControlThreadEvent::FairShareUpdateAndLog(TInstant time)
{
    return TControlThreadEvent(EControlThreadEventType::FairShareUpdateAndLog, time);
}

TControlThreadEvent TControlThreadEvent::LogNodes(TInstant time)
{
    return TControlThreadEvent(EControlThreadEventType::LogNodes, time);
}

bool operator<(const TControlThreadEvent& lhs, const TControlThreadEvent& rhs)
{
    if (lhs.Time != rhs.Time) {
        return lhs.Time < rhs.Time;
    }
    return lhs.Type < rhs.Type;
}

////////////////////////////////////////////////////////////////////////////////

TSimulatorControlThread::TSimulatorControlThread(
    const std::vector<TExecNodePtr>* execNodes,
    IOutputStream* eventLogOutputStream,
    IOperationStatisticsOutput* operationStatisticsOutput,
    const TSchedulerSimulatorConfigPtr& config,
    const NScheduler::TSchedulerConfigPtr& schedulerConfig,
    const std::vector<TOperationDescription>& operations,
    TInstant earliestTime)
    : Initialized_(false)
    , FairShareUpdateAndLogPeriod_(schedulerConfig->FairShareUpdatePeriod)
    , NodesInfoLoggingPeriod_(schedulerConfig->NodesInfoLoggingPeriod)
    , Config_(config)
    , ActionQueue_(New<TActionQueue>(Format("ControlThread")))
    , ExecNodes_(execNodes)
    , NodeEventQueue_(
        *execNodes,
        config->HeartbeatPeriod,
        earliestTime,
        config->NodeWorkerCount,
        /*maxAllowedOutrunning*/ FairShareUpdateAndLogPeriod_ + FairShareUpdateAndLogPeriod_)
    , NodeWorkerThreadPool_(CreateThreadPool(config->NodeWorkerThreadCount, "NodeWorkerPool"))
    , StrategyHost_(execNodes, eventLogOutputStream, config->RemoteEventLog, NodeShardInvokers_)
    , SchedulerStrategy_(CreateFairShareStrategy(schedulerConfig, &StrategyHost_, {ActionQueue_->GetInvoker()}))
    , SharedSchedulerStrategy_(SchedulerStrategy_, StrategyHost_, ActionQueue_->GetInvoker())
    , OperationStatistics_(operations)
    , JobAndOperationCounter_(operations.size())
    , Logger(SchedulerSimulatorLogger.WithTag("ControlThread"))
{
    for (const auto& operation : operations) {
        InsertControlThreadEvent(TControlThreadEvent::OperationStarted(operation.StartTime, operation.Id));
    }
    InsertControlThreadEvent(TControlThreadEvent::FairShareUpdateAndLog(earliestTime));
    InsertControlThreadEvent(TControlThreadEvent::LogNodes(earliestTime + TDuration::MilliSeconds(123)));

    for (int shardId = 0; shardId < config->NodeShardCount; ++shardId) {
        auto nodeShard = New<TSimulatorNodeShard>(
            shardId,
            &NodeEventQueue_,
            &StrategyHost_,
            &SharedSchedulerStrategy_,
            &OperationStatistics_,
            operationStatisticsOutput,
            &RunningOperationsMap_,
            &JobAndOperationCounter_,
            earliestTime,
            Config_,
            schedulerConfig);
        NodeShardInvokers_.push_back(nodeShard->GetInvoker());
        NodeShards_.push_back(std::move(nodeShard));
    }

    for (int workerId = 0; workerId < config->NodeWorkerCount; ++workerId) {
        auto nodeWorker = New<TSimulatorNodeWorker>(
            workerId,
            &NodeEventQueue_,
            &JobAndOperationCounter_,
            NodeWorkerThreadPool_->GetInvoker(),
            NodeShards_);
        NodeWorkers_.push_back(nodeWorker);
    }
}

void TSimulatorControlThread::Initialize(const TYsonString& poolTreesYson)
{
    YT_VERIFY(!Initialized_.load());

    SchedulerStrategy_->InitPersistentState(New<TPersistentStrategyState>());
    WaitFor(BIND(&ISchedulerStrategy::UpdatePoolTrees, SchedulerStrategy_, poolTreesYson)
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run())
        .ThrowOnError();

    for (const auto& execNode : *ExecNodes_) {
        WaitFor(BIND(&ISchedulerStrategy::RegisterOrUpdateNode, SchedulerStrategy_)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(execNode->GetId(), execNode->GetDefaultAddress(), execNode->Tags()))
            .ThrowOnError();

        const auto& nodeShard = NodeShards_[TSimulatorNodeShard::GetNodeShardId(execNode->GetId(), std::ssize(NodeShards_))];
        WaitFor(BIND(&TSimulatorNodeShard::RegisterNode, nodeShard, execNode)
            .AsyncVia(nodeShard->GetInvoker())
            .Run())
            .ThrowOnError();
    }

    Initialized_.store(true);
}

bool TSimulatorControlThread::IsInitialized() const
{
    return Initialized_.load();
}

TFuture<void> TSimulatorControlThread::AsyncRun()
{
    YT_VERIFY(Initialized_.load());
    return BIND(&TSimulatorControlThread::Run, MakeStrong(this))
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run();
}

void TSimulatorControlThread::Run()
{
    YT_LOG_INFO("Simulation started (NodeWorkerCount: %v, NodeWorkerThreadCount %v, NodeShardCount: %v)",
        Config_->NodeWorkerCount,
        Config_->NodeWorkerThreadCount,
        Config_->NodeShardCount);

    std::vector<TFuture<void>> asyncWorkerResults;
    for (const auto& nodeWorker : NodeWorkers_) {
        asyncWorkerResults.emplace_back(nodeWorker->AsyncRun());
    }

    int iteration = 0;
    while (JobAndOperationCounter_.HasUnfinishedOperations()) {
        ++iteration;
        if (iteration % Config_->CyclesPerFlush == 0) {
            YT_LOG_INFO(
                "Simulated %v cycles (FinishedOperations: %v, RunningOperation: %v, "
                "TotalOperations: %v, RunningJobs: %v)",
                iteration,
                JobAndOperationCounter_.GetFinishedOperationCount(),
                JobAndOperationCounter_.GetStartedOperationCount(),
                JobAndOperationCounter_.GetTotalOperationCount(),
                JobAndOperationCounter_.GetRunningJobCount());

            RunningOperationsMap_.ApplyRead([this] (const auto& pair) {
                const auto& operation = pair.second;
                YT_LOG_INFO("%v, (OperationId: %v)",
                    operation->GetController()->GetLoggingProgress(),
                    operation->GetId());
            });
        }

        RunOnce();
        Yield();
    }

    WaitFor(AllSucceeded(asyncWorkerResults))
        .ThrowOnError();

    std::vector<TFuture<void>> nodeShardFinalizationFutures;
    for (const auto& nodeShard : NodeShards_) {
        nodeShardFinalizationFutures.push_back(BIND(&TSimulatorNodeShard::OnSimulationFinished, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    WaitFor(AllSucceeded(nodeShardFinalizationFutures))
        .ThrowOnError();

    SchedulerStrategy_->OnMasterDisconnected();
    StrategyHost_.CloseEventLogger();

    YT_LOG_INFO("Simulation finished");
}


void TSimulatorControlThread::RunOnce()
{
    auto event = PopControlThreadEvent();

    switch (event.Type) {
        case EControlThreadEventType::OperationStarted: {
            OnOperationStarted(event);
            break;
        }

        case EControlThreadEventType::FairShareUpdateAndLog: {
            OnFairShareUpdateAndLog(event);
            break;
        }

        case EControlThreadEventType::LogNodes: {
            OnLogNodes(event);
            break;
        }
    }
}

void TSimulatorControlThread::OnOperationStarted(const TControlThreadEvent& event)
{
    const auto& description = OperationStatistics_.GetOperationDescription(event.OperationId);

    auto runtimeParameters = New<TOperationRuntimeParameters>();
    SchedulerStrategy_->InitOperationRuntimeParameters(
        runtimeParameters,
        NYTree::ConvertTo<TOperationSpecBasePtr>(description.Spec),
        description.AuthenticatedUser,
        description.Type,
        event.OperationId);
    auto operation = New<NSchedulerSimulator::TOperation>(description, runtimeParameters);

    auto operationController = CreateSimulatorOperationController(operation.Get(), &description,
        Config_->ScheduleJobDelay);
    operation->SetController(operationController);

    RunningOperationsMap_.Insert(operation->GetId(), operation);
    OperationStatistics_.OnOperationStarted(operation->GetId());
    YT_LOG_INFO("Operation started (VirtualTimestamp: %v, OperationId: %v)", event.Time, operation->GetId());

    // Notify scheduler.
    std::vector<TString> unknownTreeIds;
    TPoolTreeControllerSettingsMap poolTreeControllerSettingsMap;
    SchedulerStrategy_->RegisterOperation(operation.Get(), &unknownTreeIds, &poolTreeControllerSettingsMap);
    YT_VERIFY(unknownTreeIds.empty());
    StrategyHost_.LogEventFluently(&SchedulerStructuredLogger, ELogEventType::OperationStarted)
        .Item("operation_id").Value(operation->GetId())
        .Item("operation_type").Value(operation->GetType())
        .Item("spec").Value(operation->GetSpecString())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Do(std::bind(&ISchedulerStrategy::BuildOperationInfoForEventLog, SchedulerStrategy_, operation.Get(), _1));
    // TODO(eshcherbin): Init operation scheduling segments. Got to think of a way to get min needed resources at this point.
    SchedulerStrategy_->EnableOperation(operation.Get());

    JobAndOperationCounter_.OnOperationStarted();
}

void TSimulatorControlThread::OnFairShareUpdateAndLog(const TControlThreadEvent& event)
{
    auto updateTime = event.Time;

    YT_LOG_INFO("Started waiting for struggling node workers (VirtualTimestamp: %v)", event.Time);

    NodeEventQueue_.WaitForStrugglingNodeWorkers(updateTime);

    YT_LOG_INFO("Finished waiting for struggling node workers (VirtualTimestamp: %v)", event.Time);

    SchedulerStrategy_->OnFairShareUpdateAt(updateTime);
    SchedulerStrategy_->OnFairShareProfilingAt(updateTime);
    if (Config_->EnableFullEventLog) {
        SchedulerStrategy_->OnFairShareLoggingAt(updateTime);
    } else {
        SchedulerStrategy_->OnFairShareEssentialLoggingAt(updateTime);
    }

    NodeEventQueue_.UpdateControlThreadTime(updateTime);
    InsertControlThreadEvent(TControlThreadEvent::FairShareUpdateAndLog(event.Time + FairShareUpdateAndLogPeriod_));
}

void TSimulatorControlThread::OnLogNodes(const TControlThreadEvent& event)
{
    YT_LOG_INFO("Started logging nodes info (VirtualTimestamp: %v)", event.Time);

    std::vector<TFuture<TYsonString>> nodeListFutures;
    for (const auto& nodeShard : NodeShards_) {
        nodeListFutures.push_back(
            BIND([nodeShard] () {
                return BuildYsonStringFluently<EYsonType::MapFragment>()
                    .Do(BIND(&TSimulatorNodeShard::BuildNodesYson, nodeShard))
                    .Finish();
            })
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    auto nodeLists = WaitFor(AllSucceeded(nodeListFutures))
        .ValueOrThrow();

    StrategyHost_.LogEventFluently(StrategyHost_.GetEventLogger(), ELogEventType::NodesInfo, event.Time)
        .Item("nodes")
        .DoMapFor(nodeLists, [](TFluentMap fluent, const auto& nodeList) {
            fluent.Items(nodeList);
        });

    InsertControlThreadEvent(TControlThreadEvent::LogNodes(event.Time + NodesInfoLoggingPeriod_));
    YT_LOG_INFO("Finished logging nodes info (VirtualTimestamp: %v)", event.Time);
}

void TSimulatorControlThread::InsertControlThreadEvent(TControlThreadEvent event)
{
    ControlThreadEvents_.insert(event);
}

TControlThreadEvent TSimulatorControlThread::PopControlThreadEvent()
{
    YT_VERIFY(!ControlThreadEvents_.empty());
    auto beginIt = ControlThreadEvents_.begin();
    auto event = *beginIt;
    ControlThreadEvents_.erase(beginIt);
    return event;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator

