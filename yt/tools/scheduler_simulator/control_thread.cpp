#include "control_thread.h"
#include "operation_controller.h"
#include "node_shard.h"

#include <yt/server/scheduler/fair_share_strategy.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>


namespace NYT {
namespace NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NConcurrency;

static const auto& Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

TControlThreadEvent::TControlThreadEvent(EControlThreadEventType type, TInstant time)
    : Type(type)
    , Time(time)
    , OperationId(TGuid())
{ }

TControlThreadEvent TControlThreadEvent::OperationStarted(TInstant time, NScheduler::TOperationId id)
{
    TControlThreadEvent event(EControlThreadEventType::OperationStarted, time);
    event.OperationId = id;
    return event;
}

TControlThreadEvent TControlThreadEvent::FairShareUpdateAndLog(TInstant time)
{
    return TControlThreadEvent(EControlThreadEventType::FairShareUpdateAndLog, time);
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
    const std::vector<NScheduler::TExecNodePtr>* execNodes,
    IOutputStream* eventLogOutputStream,
    const TSchedulerSimulatorConfigPtr& config,
    const NScheduler::TSchedulerConfigPtr& schedulerConfig,
    const std::vector<TOperationDescription>& operations,
    TInstant earliestTime)
    : Initialized_(false)
    , FairShareUpdateAndLogPeriod_(schedulerConfig->FairShareUpdatePeriod)
    , Config_(config)
    , ActionQueue_(New<TActionQueue>(Format("ControlThread")))
    , StrategyHost_(execNodes, eventLogOutputStream)
    , SchedulerStrategy_(CreateFairShareStrategy(schedulerConfig, &StrategyHost_, {ActionQueue_->GetInvoker()}))
    , SchedulerStrategyForNodeShards_(SchedulerStrategy_, StrategyHost_, ActionQueue_->GetInvoker())
    , NodeShardEvents_(
        config->HeartbeatPeriod,
        earliestTime,
        execNodes->size(),
        config->ThreadCount,
        /* maxAllowedOutrunning */ FairShareUpdateAndLogPeriod_ + FairShareUpdateAndLogPeriod_)
    , OperationStatistics_(operations)
    , OperationStatisticsOutput_(config->OperationsStatsFilename)
    , JobAndOperationCounter_(operations.size())
    , Logger(TLogger(NSchedulerSimulator::Logger).AddTag("ControlThread"))
{
    for (const auto& operation : operations) {
        InsertControlThreadEvent(TControlThreadEvent::OperationStarted(operation.StartTime, operation.Id));
    }
    InsertControlThreadEvent(TControlThreadEvent::FairShareUpdateAndLog(earliestTime));

    for (int shardId = 0; shardId < config->ThreadCount; ++shardId) {
        auto nodeShard = New<TSimulatorNodeShard>(
            execNodes,
            &NodeShardEvents_,
            &SchedulerStrategyForNodeShards_,
            &OperationStatistics_,
            &OperationStatisticsOutput_,
            &RunningOperationsMap_,
            &JobAndOperationCounter_,
            Config_,
            schedulerConfig,
            earliestTime,
            shardId);
        NodeShards_.push_back(nodeShard);
    }
}

void TSimulatorControlThread::Init(const NYTree::INodePtr& poolTreesNode)
{
    YCHECK(!Initialized_.load());
    WaitFor(
        BIND(&ISchedulerStrategy::UpdatePoolTrees, SchedulerStrategy_, poolTreesNode)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run())
        .ThrowOnError();
    Initialized_.store(true);
}

bool TSimulatorControlThread::IsInitialized() const
{
    return Initialized_.load();
}

TFuture<void> TSimulatorControlThread::AsyncRun()
{
    YCHECK(Initialized_.load());
    return BIND(&TSimulatorControlThread::Run, MakeStrong(this))
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run();
}

void TSimulatorControlThread::Run()
{
    LOG_INFO("Simulation started using (%v) threads", Config_->ThreadCount);
    OperationStatisticsOutput_.PrintHeader();

    std::vector<TFuture<void>> asyncWorkerResults;
    for (const auto& nodeShard : NodeShards_) {
        asyncWorkerResults.emplace_back(nodeShard->AsyncRun());
    }

    int iter = 0;
    while (JobAndOperationCounter_.HasUnfinishedOperations()) {
        iter += 1;
        if (iter % Config_->CyclesPerFlush == 0) {
            LOG_INFO(
                "Simulated (%v) cycles (FinishedOperations: %v, RunningOperation: %v, "
                "TotalOperations: %v, RunningJobs: %v)",
                iter,
                JobAndOperationCounter_.GetFinishedOperationCount(),
                JobAndOperationCounter_.GetStartedOperationCount(),
                JobAndOperationCounter_.GetTotalOperationCount(),
                JobAndOperationCounter_.GetRunningJobCount());

            RunningOperationsMap_.ApplyRead([this] (const auto& pair) {
                const auto& operation = pair.second;
                LOG_INFO("%v, (OperationId: %v)",
                    operation->GetController()->GetLoggingProgress(),
                    operation->GetId());
            });
        }

        RunOnce();
        Yield();
    }

    WaitFor(Combine(asyncWorkerResults))
        .ThrowOnError();

    SchedulerStrategy_->OnMasterDisconnected();

    LOG_INFO("Simulation finished");
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
    }
}

void TSimulatorControlThread::OnOperationStarted(const TControlThreadEvent& event)
{
    const auto& description = OperationStatistics_.GetOperationDescription(event.OperationId);

    auto runtimeParameters = New<TOperationRuntimeParameters>();
    SchedulerStrategy_->InitOperationRuntimeParameters(
        runtimeParameters,
        NYTree::ConvertTo<NScheduler::TOperationSpecBasePtr>(description.Spec),
        description.AuthenticatedUser,
        description.Type);
    auto operation = New<NSchedulerSimulator::TOperation>(description, runtimeParameters);

    auto operationController = CreateSimulatorOperationController(operation.Get(), &description);
    operation->SetController(operationController);

    RunningOperationsMap_.Insert(operation->GetId(), operation);
    OperationStatistics_.OnOperationStarted(operation->GetId());
    LOG_INFO("Operation started (OperationId: %v)", operation->GetId());

    // Notify scheduler.
    SchedulerStrategy_->RegisterOperation(operation.Get());
    SchedulerStrategy_->EnableOperation(operation.Get());

    JobAndOperationCounter_.OnOperationStarted();
}

void TSimulatorControlThread::OnFairShareUpdateAndLog(const TControlThreadEvent& event)
{
    auto updateTime = event.Time;

    LOG_INFO("Started waiting for struggling node shards");
    NodeShardEvents_.WaitForStrugglingNodeShards(updateTime);
    LOG_INFO("Finished waiting for struggling node shards");

    SchedulerStrategy_->OnFairShareUpdateAt(updateTime);
    if (Config_->EnableFullEventLog) {
        SchedulerStrategy_->OnFairShareLoggingAt(updateTime);
    } else {
        SchedulerStrategy_->OnFairShareEssentialLoggingAt(updateTime);
    }

    NodeShardEvents_.UpdateControlThreadTime(updateTime);
    InsertControlThreadEvent(TControlThreadEvent::FairShareUpdateAndLog(event.Time + FairShareUpdateAndLogPeriod_));
}

void TSimulatorControlThread::InsertControlThreadEvent(TControlThreadEvent event)
{
    ControlThreadEvents_.insert(event);
}

TControlThreadEvent TSimulatorControlThread::PopControlThreadEvent()
{
    YCHECK(!ControlThreadEvents_.empty());
    auto beginIt = ControlThreadEvents_.begin();
    auto event = *beginIt;
    ControlThreadEvents_.erase(beginIt);
    return event;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT

