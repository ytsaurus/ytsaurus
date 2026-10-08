#pragma once

#include "public.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERebalanceActionType,
    (Add)
    (Del)
);

////////////////////////////////////////////////////////////////////////////////

struct TRebalanceResultAction
{
    ERebalanceActionType Type{};
    TPartitionId PartitionId;
    std::string WorkerAddress;
};

struct TWorkerPreloadResultAction
{
    ERebalanceActionType Type;
    TResourceId ResourceId;
    std::string WorkerAddress;
};

////////////////////////////////////////////////////////////////////////////////

//! How the resource_queue balancer saw a worker in its last round.
struct TResourceQueueWorkerStats
{
    //! Sum of the queues of the resources the worker's jobs consume.
    double QueueSize = 0.;
    //! Average queue projected over PlanningHorizon; the value the balancer equalizes.
    double ProjectedQueue = 0.;
    double Load = 0.;
    double Capacity = 0.;
    bool Underloaded = false;
    //! The worker takes part in the balance metric (in a plan and preload-ready).
    bool Enrolled = false;
};

//! The criterion of the resource_queue balancer in its last round (Steps 7-9).
struct TResourceQueueRoundStats
{
    THashMap<std::string, TResourceQueueWorkerStats> Workers;

    //! Mean and standard deviation of the projected queues over the enrolled workers.
    double Mean = 0.;
    double Deviation = 0.;
    //! RebalanceTargetDeviation.
    double TargetDeviation = 0.;
    //! Some enrolled worker projects more than ZeroQueueLatency worth of its own load.
    bool AboveZeroLevel = false;
    //! Deviation / Mean when there is a queue to balance, otherwise 0.
    //! Imbalance >= TargetDeviation exactly when the group is not balanced.
    double Imbalance = 0.;
    //! The group is balanced by the balancer's criterion: Step 8 was not attempted.
    bool Balanced = true;

    int TentativeMoves = 0;
    double NewDeviation = 0.;
    bool Accepted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TRebalanceResult
{
    std::vector<TRebalanceResultAction> Actions;
    std::vector<TWorkerPreloadResultAction> PreloadResourceActions;
    std::optional<TSequenceId> SequenceId;
    TResourceQueueRoundStats ResourceQueueStats;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
