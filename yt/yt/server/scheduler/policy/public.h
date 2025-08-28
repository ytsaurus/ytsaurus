#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NPolicy {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicyHost;

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshotState)

DECLARE_REFCOUNTED_STRUCT(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NPolicy
