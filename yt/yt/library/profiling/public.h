#pragma once

#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct ISummaryImplBase;

using ISummaryImpl = ISummaryImplBase<double>;
using ITimerImpl = ISummaryImplBase<TDuration>;

DECLARE_REFCOUNTED_TYPE(ISummaryImpl)
DECLARE_REFCOUNTED_TYPE(ITimerImpl)

DECLARE_REFCOUNTED_STRUCT(ICounterImpl)
DECLARE_REFCOUNTED_STRUCT(ITimeCounterImpl)
DECLARE_REFCOUNTED_STRUCT(IGaugeImpl)
DECLARE_REFCOUNTED_STRUCT(ITimeGaugeImpl)
DECLARE_REFCOUNTED_STRUCT(ITimeHistogramImpl)
DECLARE_REFCOUNTED_STRUCT(IRegistryImpl)
DECLARE_REFCOUNTED_STRUCT(ISensorProducer)
DECLARE_REFCOUNTED_CLASS(TBufferedProducer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
