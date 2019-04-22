#pragma once

#include <yt/core/misc/public.h>
#include <yt/core/misc/guid.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTraceManagerConfig)

DECLARE_REFCOUNTED_CLASS(TTraceContext)

using TTraceId = TGuid;
constexpr TTraceId InvalidTraceId = {};

using TSpanId = ui64;
constexpr TSpanId InvalidSpanId = 0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
