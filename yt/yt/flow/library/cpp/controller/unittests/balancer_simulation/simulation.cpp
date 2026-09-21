#include "simulation.h"

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/env.h>

#include <cmath>

namespace NYT::NFlow::NBalancer::NTesting {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("BalancerSimulation");

namespace {

void ConfigureSimulationLog()
{
    static const bool Configured = [] {
        auto directory = GetOutputPath().GetPath();
        auto config = NLogging::TLogManagerConfig::CreateYTServer("unittester", directory);

        auto writer = New<NLogging::TFileLogWriterConfig>();
        writer->FileName = directory + "/balancer_simulation.log";
        config->Writers.emplace("balancer_simulation", NYTree::ConvertTo<NYTree::IMapNodePtr>(writer));

        auto rule = New<NLogging::TRuleConfig>();
        rule->MinLevel = NLogging::ELogLevel::Info;
        rule->IncludeCategories = {Logger.GetCategory()->Name};
        rule->Writers.push_back("balancer_simulation");
        config->Rules.push_back(rule);

        config->Postprocess();
        NLogging::TLogManager::Get()->Configure(config);
        return true;
    }();
    Y_UNUSED(Configured);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSimulation::TSimulation(TScenario scenario)
    : Scenario_(std::move(scenario))
    , Group_("l40")
    , FlowView_(MakeEmptyFlowView())
{
    ConfigureSimulationLog();
    for (int minute : Scenario_.CheckpointMinutes) {
        YT_VERIFY(minute > 0 && minute * 60 % StepSeconds == 0);
        YT_VERIFY(minute * 60 / StepSeconds <= Scenario_.TotalSteps);
    }
    Build();
}

void TSimulation::Run()
{
    YT_TLOG_INFO("Scenario started")
        .With("Scenario", Scenario_.Name)
        .With("Workers", Scenario_.WorkerCount)
        .With("Computations", Scenario_.ComputationCount)
        .With("PreloadedWorkersAtStart", Scenario_.PreloadedWorkersAtStart)
        .With("ModelGpuMemory", Scenario_.ModelGpuMemory)
        .With("WorkerGpuMemory", Scenario_.WorkerGpuMemory)
        .With("TotalSteps", Scenario_.TotalSteps);
    THashSet<int> checkpointSteps;
    for (int minute : Scenario_.CheckpointMinutes) {
        checkpointSteps.insert(minute * 60 / StepSeconds);
    }
    int movesAtLastCheckpoint = 0;

    for (Step_ = 0; Step_ < Scenario_.TotalSteps; ++Step_) {
        Publish();
        auto result = DoBalanceResourceQueue(FlowView_, BalancerSpec_, Group_, Now());
        Apply(result);
        Step();
        if (checkpointSteps.contains(Step_ + 1)) {
            auto cp = TakeCheckpoint((Step_ + 1) * StepSeconds / 60, std::ssize(Moves_) - movesAtLastCheckpoint);
            movesAtLastCheckpoint = std::ssize(Moves_);
            Checkpoints_.push_back(cp);
            Print(cp);
        }
    }
}

const std::vector<TMove>& TSimulation::Moves() const
{
    return Moves_;
}

const std::vector<TPreloadEvent>& TSimulation::PreloadEvents() const
{
    return PreloadEvents_;
}

const std::vector<TCheckpoint>& TSimulation::Checkpoints() const
{
    return Checkpoints_;
}

const TEnumIndexedArray<EViolation, int>& TSimulation::Violations() const
{
    return Violations_;
}

int TSimulation::StrayCount() const
{
    int n = 0;
    for (const auto& p : Partitions_) {
        n += p.Worker.empty();
    }
    return n;
}

int TSimulation::PreloadCancelledWhileLoading() const
{
    return PreloadCancelledWhileLoading_;
}

TInstant TSimulation::Now() const
{
    return Start_ + TDuration::Seconds(Step_ * StepSeconds);
}

TInstant TSimulation::StepEnd() const
{
    return Start_ + TDuration::Seconds((Step_ + 1) * StepSeconds);
}

double TSimulation::DemandAt(int step) const
{
    int minute = step * StepSeconds / 60;
    bool dip = Scenario_.DemandDipStartMinute <= minute && minute < Scenario_.DemandDipEndMinute;
    return Scenario_.DemandPerPartition * (dip ? Scenario_.DemandDipMultiplier : 1.);
}

void TSimulation::SetSpecs(const TPipelineSpecPtr& pipelineSpec)
{
    FlowView_->CurrentSpec->TrySetValue(pipelineSpec, TestVersionProvider());
    FlowView_->State->ExecutionSpec->PipelineSpec->TrySetValue(pipelineSpec, TestVersionProvider());
    FlowView_->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(pipelineSpec), TestVersionProvider());
}

void TSimulation::Build()
{
    BalancerSpec_ = MakeBalancerSpec(Scenario_.PlanningHorizonSeconds, Scenario_.ZeroQueueLatencySeconds);
    BalancerSpec_->RebalanceTargetDeviation = Scenario_.RebalanceTargetDeviation;

    auto pipelineSpec = FlowView_->CurrentSpec->GetValue();
    for (int c = 0; c < Scenario_.ComputationCount; ++c) {
        auto computationId = TComputationId(Format("ModelInferer_%v", c));
        auto resourceId = TResourceId(Format("ModelInferenceQueue_%v", c));
        Computations_.push_back(computationId);
        Resources_.push_back(resourceId);
        pipelineSpec->Resources[resourceId] = MakeResourceSpec({{"gpu_memory", Scenario_.ModelGpuMemory}}, /*preloadRequired*/ true);
        pipelineSpec->Computations[computationId] = MakeComputationSpec(Group_, {resourceId});
    }
    SetSpecs(pipelineSpec);

    for (int w = 0; w < Scenario_.WorkerCount; ++w) {
        TWorkerModel worker;
        worker.Address = Format("l40-%v", w + 1);
        worker.Capacity = Scenario_.WorkerCapacity;
        worker.Queues.resize(Scenario_.ComputationCount);
        if (w < Scenario_.PreloadedWorkersAtStart) {
            for (const auto& resourceId : Resources_) {
                worker.Preloaded.insert(resourceId);
                // The controller had issued these preloads before the spec apply.
                SetPreloadIssued(FlowView_, worker.Address, resourceId);
            }
        }
        WorkerIndex_[worker.Address] = w;
        AddWorker(FlowView_, worker.Address, Group_, {{"gpu_memory", Scenario_.WorkerGpuMemory}});
        Workers_.push_back(std::move(worker));
    }

    int index = 0;
    for (int c = 0; c < Scenario_.ComputationCount; ++c) {
        for (int p = 0; p < Scenario_.PartitionsPerComputation; ++p) {
            TPartitionModel partition;
            partition.Index = index;
            partition.Id = MakePartitionId(index + 1); // 0 would be a null id.
            partition.Computation = c;
            partition.Demand = DemandAt(0);
            PartitionIndex_[partition.Id] = index;
            Partitions_.push_back(partition);
            // Stray: no job. The job status is published by Publish().
            AddPartition(FlowView_, partition.Id, Computations_[c], partition.Demand, std::nullopt);
            ++index;
        }
    }

    YT_VERIFY(std::ssize(Scenario_.InitialShares) <= Scenario_.WorkerCount);
    for (int c = 0; c < Scenario_.ComputationCount; ++c) {
        int next = c * Scenario_.PartitionsPerComputation;
        int end = next + Scenario_.PartitionsPerComputation;
        for (int w = 0; w < std::ssize(Scenario_.InitialShares); ++w) {
            int count = static_cast<int>(std::lround(Scenario_.InitialShares[w] * Scenario_.PartitionsPerComputation));
            YT_VERIFY(count == 0 || Workers_[w].Preloaded.contains(Resources_[c]));
            YT_VERIFY(next + count <= end);
            for (; count > 0; --count, ++next) {
                CreateJob(Partitions_[next], Workers_[w].Address);
                Partitions_[next].StartTime = Epoch_;
                // A placed partition is serving its demand before the run starts; without this
                // the balancer sees rps 0 on the first round and the group looks empty.
                Partitions_[next].MeasuredRps = Partitions_[next].Demand;
            }
        }
    }
}

void TSimulation::Step()
{
    const double dt = StepSeconds;
    const auto stepEnd = StepEnd();
    const double demand = DemandAt(Step_);
    for (auto& partition : Partitions_) {
        partition.Demand = demand;
    }
    for (auto& worker : Workers_) {
        for (auto& q : worker.Queues) {
            q.Load = 0.;
        }
        for (const auto& partition : Partitions_) {
            if (partition.Worker == worker.Address) {
                worker.Queues[partition.Computation].Load += partition.Demand;
            }
        }
        worker.Load = 0.;
        double wanted = 0.;
        for (const auto& q : worker.Queues) {
            worker.Load += q.Load;
            wanted += q.Load * dt + q.Queue;
        }

        // Capacity is shared between computations in proportion to new demand plus the
        // standing queue, so a queue left behind by moved partitions still drains.
        worker.Processed = 0.;
        for (auto& q : worker.Queues) {
            double want = q.Load * dt + q.Queue;
            double served = wanted > 0. ? std::min(want, worker.Capacity * dt * want / wanted) : 0.;
            double oldQueue = q.Queue;
            double newQueue = want - served;
            // The cap limits growth only; a reduced cap must not erase the accumulated queue.
            double cap = worker.Load > 0. ? Scenario_.QueueCap * q.Load / worker.Load : 0.;
            if (newQueue > oldQueue) {
                newQueue = std::min(newQueue, std::max(cap, oldQueue));
            }
            q.Queue = newQueue;
            q.ServedRate = served / dt;
            worker.Processed += q.ServedRate;
            double fetched = served;
            // Derived from the actual queue change so that PushedTotal - FetchedTotal == Queue.
            double pushed = fetched + (newQueue - oldQueue);
            q.PushedTotal += pushed;
            q.FetchedTotal += fetched;
            q.Push.Update(q.PushedTotal, stepEnd);
            q.Fetch.Update(q.FetchedTotal, stepEnd);
            q.Size.Set(q.PushedTotal - q.FetchedTotal, stepEnd);
        }

        std::vector<TResourceId> done;
        for (const auto& [resourceId, at] : worker.PreloadCompletesAtStep) {
            if (Step_ + 1 >= at) {
                done.push_back(resourceId);
            }
        }
        for (const auto& resourceId : done) {
            worker.PreloadCompletesAtStep.erase(resourceId);
            worker.Preloaded.insert(resourceId);
        }
    }
    for (auto& partition : Partitions_) {
        if (partition.Worker.empty()) {
            partition.MeasuredRps = 0.;
            continue;
        }
        const auto& q = Workers_[WorkerIndex_[partition.Worker]].Queues[partition.Computation];
        partition.MeasuredRps = q.Load > 0. ? q.ServedRate * partition.Demand / q.Load : 0.;
    }
}

void TSimulation::Publish()
{
    auto now = Now();
    auto& feedback = FlowView_->Feedback;

    for (const auto& partition : Partitions_) {
        if (partition.Worker.empty()) {
            feedback->PartitionJobStatuses.erase(partition.Id);
            continue;
        }
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobStatus = New<TJobStatus>();
        status->CurrentJobStatus->StartTime = partition.StartTime;
        auto inputMetrics = New<TNodeInputMetrics>();
        inputMetrics->Global.MessagesPerSecond = partition.MeasuredRps;
        status->CurrentJobStatus->InputMetrics = inputMetrics;
        feedback->PartitionJobStatuses[partition.Id] = status;
    }

    for (const auto& worker : Workers_) {
        auto status = New<TWorkerStatus>();
        for (const auto& resourceId : worker.Preloaded) {
            status->PreloadedResourceStates[resourceId] = EPreloadedResourceState::Preloaded;
        }
        // Same eight fields as TResourceStatus::Collect(); a window that has not filled
        // yet stays nullopt and the balancer falls back to the shorter one.
        for (int c = 0; c < Scenario_.ComputationCount; ++c) {
            const auto& q = worker.Queues[c];
            auto resourceStatus = New<TWorkerResourceStatus>();
            resourceStatus->QueueSize30s = q.Size.Average()[ThirtySecondWindow];
            resourceStatus->QueueSize10m = q.Size.Average()[TenMinuteWindow];
            resourceStatus->QueueGrowthRate30s = q.Size.GrowthRate()[ThirtySecondWindow];
            resourceStatus->QueueGrowthRate10m = q.Size.GrowthRate()[TenMinuteWindow];
            resourceStatus->QueuePushRate30s = q.Push.GetRate(ThirtySecondWindow, now);
            resourceStatus->QueuePushRate10m = q.Push.GetRate(TenMinuteWindow, now);
            resourceStatus->QueueFetchRate30s = q.Fetch.GetRate(ThirtySecondWindow, now);
            resourceStatus->QueueFetchRate10m = q.Fetch.GetRate(TenMinuteWindow, now);
            status->ResourceStatuses[Resources_[c]] = resourceStatus;
        }
        feedback->WorkerStatuses[worker.Address] = status;
    }
}

void TSimulation::RemoveJob(TPartitionModel& partition)
{
    if (!partition.JobId) {
        return;
    }
    FlowView_->State->StartMutation();
    FlowView_->State->ExecutionSpec->Layout->RemoveJob(*partition.JobId, EJobFinishReason::Rebalanced);
    FlowView_->State->CommitMutation();
    partition.JobId.reset();
}

void TSimulation::CreateJob(TPartitionModel& partition, const std::string& worker)
{
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::FromString(Format("%08x-%08x-%08x-%08x", 0, 0, 3, JobCounter_++)));
    job->PartitionId = partition.Id;
    job->WorkerAddress = worker;
    FlowView_->State->StartMutation();
    FlowView_->State->ExecutionSpec->Layout->CreateJob(job);
    FlowView_->State->CommitMutation();
    partition.JobId = job->JobId;
    partition.Worker = worker;
    partition.StartTime = Now();
}

bool TSimulation::HasPartitionsOf(const TWorkerModel& worker, int computation) const
{
    for (const auto& partition : Partitions_) {
        if (partition.Worker == worker.Address && partition.Computation == computation) {
            return true;
        }
    }
    return false;
}

void TSimulation::Apply(const TRebalanceResult& result)
{
    THashMap<TPartitionId, std::string> adds;
    THashMap<TPartitionId, std::string> dels;
    for (const auto& action : result.Actions) {
        if (action.Type == ERebalanceActionType::Add) {
            adds[action.PartitionId] = action.WorkerAddress;
        } else {
            dels[action.PartitionId] = action.WorkerAddress;
        }
    }
    // Deterministic order: by partition index.
    std::vector<int> touched;
    for (const auto& [partitionId, _] : adds) {
        touched.push_back(GetOrCrash(PartitionIndex_, partitionId));
    }
    for (const auto& [partitionId, _] : dels) {
        if (!adds.contains(partitionId)) {
            touched.push_back(GetOrCrash(PartitionIndex_, partitionId));
        }
    }
    std::sort(touched.begin(), touched.end());
    for (int index : touched) {
        auto& partition = Partitions_[index];
        auto from = partition.Worker;
        if (auto delIt = dels.find(partition.Id); delIt != dels.end() && delIt->second != partition.Worker) {
            ++Violations_[EViolation::DelWorkerMismatch];
        }
        auto addIt = adds.find(partition.Id);
        if (addIt == adds.end()) {
            RemoveJob(partition);
            partition.Worker.clear();
            Moves_.push_back({Step_, index, from, ""});
            continue;
        }
        if (partition.Worker == addIt->second) {
            continue;
        }
        if (!partition.Worker.empty() && !dels.contains(partition.Id)) {
            ++Violations_[EViolation::AddWithoutDel];
        }
        const auto& target = Workers_[GetOrCrash(WorkerIndex_, addIt->second)];
        if (!target.Preloaded.contains(Resources_[partition.Computation])) {
            ++Violations_[EViolation::AddWithoutModel];
        }
        RemoveJob(partition);
        CreateJob(partition, addIt->second);
        Moves_.push_back({Step_, index, from, addIt->second});
    }

    for (const auto& action : result.PreloadResourceActions) {
        auto& worker = Workers_[GetOrCrash(WorkerIndex_, action.WorkerAddress)];
        PreloadEvents_.push_back(TPreloadEvent{Step_, action.WorkerAddress, Format("%v", action.ResourceId), action.Type == ERebalanceActionType::Add});
        if (action.Type == ERebalanceActionType::Add) {
            SetPreloadIssued(FlowView_, action.WorkerAddress, action.ResourceId);
            if (!worker.Preloaded.contains(action.ResourceId) && !worker.PreloadCompletesAtStep.contains(action.ResourceId)) {
                worker.PreloadCompletesAtStep[action.ResourceId] = Step_ + Scenario_.PreloadDelaySteps;
            }
            continue;
        }
        int computation = static_cast<int>(std::find(Resources_.begin(), Resources_.end(), action.ResourceId) - Resources_.begin());
        if (HasPartitionsOf(worker, computation)) {
            // A real worker cannot drop a model its jobs are using; count it and keep the model.
            ++Violations_[EViolation::UnloadInUse];
            continue;
        }
        if (worker.PreloadCompletesAtStep.contains(action.ResourceId)) {
            ++PreloadCancelledWhileLoading_;
        }
        worker.Preloaded.erase(action.ResourceId);
        worker.PreloadCompletesAtStep.erase(action.ResourceId);
        ClearPreloadIssued(FlowView_, action.WorkerAddress, action.ResourceId);
    }
}

TCheckpoint TSimulation::TakeCheckpoint(int minute, int movesSinceLast) const
{
    TCheckpoint cp;
    cp.Minute = minute;
    cp.MovesSinceLast = movesSinceLast;
    cp.Stray = StrayCount();
    cp.Placement.resize(Computations_.size());
    for (const auto& partition : Partitions_) {
        if (!partition.Worker.empty()) {
            cp.Placement[partition.Computation][partition.Worker] += 1;
        }
    }
    for (const auto& worker : Workers_) {
        double queue = 0.;
        for (const auto& q : worker.Queues) {
            queue += q.Queue;
        }
        cp.Queue[worker.Address] = queue;
        cp.Load[worker.Address] = worker.Load;
        cp.PreloadedCount[worker.Address] = std::ssize(worker.Preloaded);
    }
    for (const auto& placement : cp.Placement) {
        for (const auto& [_, count] : placement) {
            cp.MaxShare = std::max(cp.MaxShare, static_cast<double>(count) / Scenario_.PartitionsPerComputation);
        }
    }
    return cp;
}

void TSimulation::Print(const TCheckpoint& cp) const
{
    YT_TLOG_INFO("Checkpoint")
        .With("Scenario", Scenario_.Name)
        .With("Minute", cp.Minute)
        .With("MovesSinceLast", cp.MovesSinceLast)
        .With("MaxShare", cp.MaxShare)
        .With("Stray", cp.Stray);
    for (const auto& worker : Workers_) {
        std::vector<int> partitions;
        for (const auto& placement : cp.Placement) {
            auto it = placement.find(worker.Address);
            partitions.push_back(it == placement.end() ? 0 : it->second);
        }
        YT_TLOG_INFO("Checkpoint worker")
            .With("Scenario", Scenario_.Name)
            .With("Minute", cp.Minute)
            .With("Worker", worker.Address)
            .With("Partitions", partitions)
            .With("ModelsPreloaded", cp.PreloadedCount.at(worker.Address))
            .With("Load", cp.Load.at(worker.Address))
            .With("Queue", cp.Queue.at(worker.Address));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer::NTesting
