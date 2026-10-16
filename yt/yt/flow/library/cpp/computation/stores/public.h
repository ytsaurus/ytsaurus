#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInputStoreContext);
DECLARE_REFCOUNTED_STRUCT(IInputStore);

DECLARE_REFCOUNTED_STRUCT(TOutputStoreContext);
DECLARE_REFCOUNTED_STRUCT(IOutputStore);

DECLARE_REFCOUNTED_STRUCT(TCompactOutputStoreContext);

DECLARE_REFCOUNTED_STRUCT(TTimerStoreContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicTimerStoreContext);
DECLARE_REFCOUNTED_STRUCT(ITimerStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
