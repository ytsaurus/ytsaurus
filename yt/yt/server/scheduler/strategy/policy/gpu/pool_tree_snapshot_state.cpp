#include "pool_tree_snapshot_state.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl::TPoolTreeSnapshotStateImpl(
    TOperationSnapshotStateMap operationStates,
    TNodeSnapshotStateMap nodeStates,
    TAllocationSnapshotStateMap allocationStates,
    TInstant snapshotTime)
    : TPoolTreeSnapshotState(EPolicyKind::Gpu)
    , OperationStates_(std::move(operationStates))
    , NodeStates_(std::move(nodeStates))
    , AllocationStates_(std::move(allocationStates))
    , SnapshotTime_(snapshotTime)
{ }

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl* GetPoolTreeSnapshotState(const TPoolTreeSnapshotPtr& treeSnapshot)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);
    YT_ASSERT(state->PolicyKind == EPolicyKind::Gpu);
    return static_cast<TPoolTreeSnapshotStateImpl*>(state.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
