#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

bool ComputationBelongsToGroup(const TComputationSpecPtr& computationSpec, const TWorkerGroupId& workerGroup);

bool WorkerBelongsToGroup(const TWorkerPtr& worker, const TWorkerGroupId& workerGroup);

//! Whether the job reports a 10-minute rate of steady-state work, i.e. not of its first iteration.
bool AreJobMetricsSteady(const TJobStatusPtr& status);

//! Share of the job's own 10-minute rate in its blend with the partition's history: 0 until the
//! rate exists, then growing linearly over one more window; 1 means the metrics are mature.
double GetJobMetricsMaturity(const TJobStatusPtr& status, TInstant now);

//! Whether the job started so recently that moving it wastes nothing yet: partitions placed
//! before every worker had registered must still be spread by the count kick.
bool IsJobFreshlyStarted(const TJobStatusPtr& status, TInstant now);

//! Whether the balancer may move the partition: a partition without a job always, a running job
//! only while it is freshly started or once its metrics are mature, unless the group's warm-up
//! protection is off (see #IsWarmupProtectionActive).
bool IsPartitionMovable(const TFlowViewPtr& flowView, const TPartitionId& partitionId, bool warmupProtectionActive);

//! Share of the group's workers that run no job of the group.
double GetIdleWorkerShare(const TFlowViewPtr& flowView, const TWorkerGroupId& workerGroup);
//! Whether warm-up protection applies to the group now: it is lifted while at least
//! |balance_warmup_idle_worker_share| of the group's workers are idle.
bool IsWarmupProtectionActive(const TFlowViewPtr& flowView, const TDynamicJobBalancerSpecPtr& balancerSpec, const TWorkerGroupId& workerGroup);

//! The only way to remove a job from the layout: saves the windowed metrics of the job's last
//! status as its partition's history first, since the removal drops the status. A job whose
//! metrics are not mature yet leaves the history as it was. A pipeline stop saves nothing:
//! the history serves moves, and after a restart too much changes to draw conclusions from it.
void RemoveJobKeepingMetrics(const TFlowViewPtr& flowView, const TJobId& jobId, EJobFinishReason jobFinishReason);

//! Saves the history within |partition_history_limit|; when the limit is reached a history
//! heavier than the lightest one stored replaces it, a lighter one is dropped.
void StorePartitionHistory(const TFlowViewPtr& flowView, const TPartitionId& partitionId, TPartitionMetricsHistoryPtr history);

//! Drops the histories of the group's partitions that nobody needs anymore: the partition is
//! gone, the group is not balanced by CPU, or the partition's job has matured and no worker
//! coefficient observation is pending for it.
void PrunePartitionHistories(const TFlowViewPtr& flowView, const TDynamicJobBalancerSpecPtr& balancerSpec, const TWorkerGroupId& workerGroup);

//! Drops the persisted state of worker groups that are not in |groups|: a group without
//! computations has no balancer to age its observations.
void PruneBalancerGroups(const TFlowViewPtr& flowView, const THashSet<TWorkerGroupId>& groups);

////////////////////////////////////////////////////////////////////////////////

//! Remove redundant Add+Del action pairs for the same partition on the same worker.
//!
//! A partition may have been assigned to worker wA earlier (Add),
//! and then moved away later (Del from wA + Add to wB).
//! The Add(p, wA) + Del(p, wA) pair is redundant and is collapsed,
//! leaving only Add(p, wB).
//!
//! Processes actions in order. For each Del action that follows an Add for the
//! same partition on the same worker, both actions are removed from the result.
//! Modifies the vector in place.
void CompactRebalanceActions(std::vector<TRebalanceResultAction>& actions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
