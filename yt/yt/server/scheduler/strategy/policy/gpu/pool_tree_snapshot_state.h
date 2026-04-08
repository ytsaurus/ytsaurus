#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeSnapshotStateImpl
    : public TPoolTreeSnapshotState
{
    TPoolTreeSnapshotStateImpl()
        : TPoolTreeSnapshotState(EPolicyKind::Gpu)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
