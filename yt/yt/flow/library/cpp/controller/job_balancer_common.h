#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

bool ComputationBelongsToGroup(const TComputationSpecPtr& computationSpec, const TWorkerGroupId& workerGroup);

bool WorkerBelongsToGroup(const TWorkerPtr& worker, const TWorkerGroupId& workerGroup);

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
