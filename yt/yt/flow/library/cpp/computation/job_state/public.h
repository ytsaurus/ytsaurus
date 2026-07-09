#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TJobStateClientContext);
DECLARE_REFCOUNTED_STRUCT(TJobStateManagerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobStateManagerContext);
DECLARE_REFCOUNTED_CLASS(TJobStateManager);
DECLARE_REFCOUNTED_STRUCT(IJobInitContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
