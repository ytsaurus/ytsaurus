#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow::NPartitioning {

DECLARE_REFCOUNTED_STRUCT(TComputationPartitioningState);
DECLARE_REFCOUNTED_STRUCT(TPartitioningCoordinatorState);
DECLARE_REFCOUNTED_CLASS(TPartitioningCoordinator);

} // namespace NYT::NFlow::NPartitioning
