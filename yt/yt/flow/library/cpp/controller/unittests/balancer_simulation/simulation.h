#pragma once

#include <yt/yt/flow/library/cpp/controller/unittests/mock/resource_balancer_helpers.h>

#include <yt/yt/flow/library/cpp/misc/ema.h>

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NFlow::NBalancer::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Closed-loop simulation of DoBalanceResourceQueue: each round the balancer's actions
//! are applied to the TFlowView and a worker model computes what the next round observes.
//!
//! Worker model: each step a worker serves up to capacity * dt requests, shared between
//! its computations in proportion to their new demand plus standing queue; what is not
//! served queues up, growth capped per computation; a partition's measured Rps is its
//! share of what its computation got served; the 30s and 10m statistics come from the
//! same EMA structures the worker uses; a move resets the partition age, a preload
//! completes after a fixed delay. Time is simulated (one round = StepSeconds) and passed to the
//! balancer explicitly, so runs are deterministic.
//!
//! The environment counts violations of what any correct balancer must keep (EViolation):
//! a partition is added only to a worker with its model, Del names the worker the
//! partition is on, a model is not unloaded while partitions use it.
//!
//! Checkpoints (placement, load and queue per worker, moves) are logged with the
//! BalancerSimulation logger into balancer_simulation.log next to the unittester logs;
//! by timestamp they line up with the balancer's own log in unittester.debug.log.
//!
//! Only the algorithm is simulated. Execution of the actions (job manager, async
//! balancer) is covered by async_balancer_ut.cpp; the worker model is linear, so only
//! the shape of the trajectory is meaningful, not absolute times.

constexpr int StepSeconds = 10;

////////////////////////////////////////////////////////////////////////////////

struct TScenario
{
    std::string Name;
    int WorkerCount = 0;
    double WorkerCapacity = 0.; // requests per second per worker
    // Resource queue cap per worker. Chosen above ZeroQueueLatency * rate so that
    // both the Underloaded and the saturated branches of the capacity estimate
    // are exercised.
    double QueueCap = 0.;
    int ComputationCount = 0;
    int PartitionsPerComputation = 0;
    double DemandPerPartition = 0.;  // requests per second per partition
    int PreloadedWorkersAtStart = 0; // workers that already have every model at step 0
    ssize_t ModelGpuMemory = 0;      // required_capabilities.gpu_memory of every model
    ssize_t WorkerGpuMemory = 0;     // gpu_memory capability of every worker
    int PreloadDelaySteps = 0;       // steps between a preload request and its completion
    int TotalSteps = 0;
    std::vector<int> CheckpointMinutes;

    double PlanningHorizonSeconds = 0.;
    double ZeroQueueLatencySeconds = 0.;
    double RebalanceTargetDeviation = 0.;

    // Whether every worker is expected to end the run within its capacity.
    bool ExpectLoadWithinCapacity = false;

    int TotalPartitions() const
    {
        return ComputationCount * PartitionsPerComputation;
    }
};

struct TMove
{
    int Step = 0;
    int Partition = 0;
    std::string From; // empty == stray
    std::string To;   // empty == removed
};

struct TPreloadEvent
{
    int Step = 0;
    std::string Worker;
    std::string Resource;
    bool Add = false;
};

struct TCheckpoint
{
    int Minute = 0;
    // computation index -> worker -> partition count
    std::vector<THashMap<std::string, int>> Placement;
    THashMap<std::string, double> Queue;
    THashMap<std::string, double> Load;
    THashMap<std::string, int> PreloadedCount;
    int MovesSinceLast = 0;
    int Stray = 0;
    double MaxShare = 0.; // max over computations of max worker share.
};

DEFINE_ENUM(EViolation,
    (AddWithoutModel)   // partition added to a worker that has not preloaded its model
    (DelWorkerMismatch) // Del names a worker the partition is not on
    (UnloadInUse)       // preload Del for a model whose partitions run on the worker
    (AddWithoutDel)     // Add for a partition that runs elsewhere, without the paired Del
);

////////////////////////////////////////////////////////////////////////////////

class TSimulation
{
public:
    explicit TSimulation(TScenario scenario);

    void Run();

    const std::vector<TMove>& Moves() const;

    const std::vector<TPreloadEvent>& PreloadEvents() const;

    const std::vector<TCheckpoint>& Checkpoints() const;

    const TEnumIndexedArray<EViolation, int>& Violations() const;

    int StrayCount() const;

private:
    static constexpr int WindowCount = 2;
    static constexpr std::array<TDuration, WindowCount> WindowDurations = {TDuration::Seconds(30), TDuration::Minutes(10)};
    static constexpr int ThirtySecondWindow = 0;
    static constexpr int TenMinuteWindow = 1;

    //! Resource queue of one computation on one worker, tracked the way
    //! TResourceStatus in common/resource_manager.cpp tracks it.
    struct TQueueStats
    {
        double Queue = 0.;
        double Load = 0.;       // demand of the computation's partitions on the worker, rps
        double ServedRate = 0.; // what the worker served for the computation last step, rps
        double PushedTotal = 0.;
        double FetchedTotal = 0.;
        TEmaCounter<double, WindowCount> Push{{WindowDurations.begin(), WindowDurations.end()}};
        TEmaCounter<double, WindowCount> Fetch{{WindowDurations.begin(), WindowDurations.end()}};
        TMultiWindowEma<double, WindowCount, true> Size{WindowDurations};
    };

    struct TWorkerModel
    {
        std::string Address;
        double Capacity = 0.;
        double Load = 0.;
        double Processed = 0.;
        std::vector<TQueueStats> Queues; // by computation index
        THashMap<TResourceId, int> PreloadCompletesAtStep;
        THashSet<TResourceId> Preloaded;
    };

    struct TPartitionModel
    {
        int Index = 0;
        TPartitionId Id;
        int Computation = 0;
        std::string Worker; // empty == stray
        std::optional<TJobId> JobId;
        double Demand = 0.;
        double MeasuredRps = 0.;
        int StartedAtStep = 0;
    };

    TScenario Scenario_;
    TWorkerGroupId Group_;
    TFlowViewPtr FlowView_;
    TDynamicJobBalancerSpecPtr BalancerSpec_;

    // Fixed epoch: the simulated clock never touches the wall clock.
    const TInstant Epoch_ = TInstant::Seconds(1'700'000'000);

    std::vector<TComputationId> Computations_;
    std::vector<TResourceId> Resources_;
    std::vector<TWorkerModel> Workers_;
    std::vector<TPartitionModel> Partitions_;
    THashMap<TPartitionId, int> PartitionIndex_;
    THashMap<std::string, int> WorkerIndex_;

    int Step_ = 0;
    int JobCounter_ = 1;
    std::vector<TMove> Moves_;
    std::vector<TPreloadEvent> PreloadEvents_;
    std::vector<TCheckpoint> Checkpoints_;
    TEnumIndexedArray<EViolation, int> Violations_;

    TInstant Now() const;

    TInstant StepEnd() const;

    void SetSpecs(const TPipelineSpecPtr& pipelineSpec);

    void Build();

    //! Worker response to the current placement: one simulated step.
    void Step();

    //! Write what the balancer will observe this round into the TFlowView.
    void Publish();

    void RemoveJob(TPartitionModel& partition);

    void CreateJob(TPartitionModel& partition, const std::string& worker);

    bool HasPartitionsOf(const TWorkerModel& worker, int computation) const;

    //! Apply the balancer's decisions: partition moves and preload requests.
    void Apply(const TRebalanceResult& result);

    TCheckpoint TakeCheckpoint(int minute, int movesSinceLast) const;

    void Print(const TCheckpoint& cp) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer::NTesting
