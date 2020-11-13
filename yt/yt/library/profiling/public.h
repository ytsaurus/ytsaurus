#pragma once

#include <yt/core/misc/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ICounterImpl)
DECLARE_REFCOUNTED_STRUCT(ITimeCounterImpl)
DECLARE_REFCOUNTED_STRUCT(IGaugeImpl)
DECLARE_REFCOUNTED_STRUCT(ISummaryImpl)
DECLARE_REFCOUNTED_STRUCT(ITimerImpl)
DECLARE_REFCOUNTED_STRUCT(IRegistryImpl)
DECLARE_REFCOUNTED_STRUCT(ISensorProducer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
