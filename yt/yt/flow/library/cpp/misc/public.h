#pragma once

#include <yt/yt/flow/library/cpp/client/public.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TVersionedValue;

template <class... TConfigs>
class TReconfigurable;

DECLARE_REFCOUNTED_STRUCT(TBacktraceEnricherState);
DECLARE_REFCOUNTED_STRUCT(TBacktraceEnricherSpec);
DECLARE_REFCOUNTED_STRUCT(TBacktraceEnricherDynamicSpec);

DECLARE_REFCOUNTED_CLASS(IRetryableClient)
DECLARE_REFCOUNTED_CLASS(TRetryableErrorHolder);
DECLARE_REFCOUNTED_STRUCT(IStatusErrorState);
DECLARE_REFCOUNTED_STRUCT(IStatusProfiler);
DECLARE_REFCOUNTED_STRUCT(TDynamicRetryableClientSpec)

DECLARE_REFCOUNTED_CLASS(IRetryableTransaction)

DECLARE_REFCOUNTED_STRUCT(TLoadThroughputThrottlerSpec);
DECLARE_REFCOUNTED_CLASS(TLoadThroughputThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
