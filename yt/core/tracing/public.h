#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTraceManagerConfig)

class TTraceContext;

using TTraceId = ui64;
constexpr TTraceId InvalidTraceId = 0;

using TSpanId = ui64;
constexpr TSpanId InvalidSpanId = 0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT
