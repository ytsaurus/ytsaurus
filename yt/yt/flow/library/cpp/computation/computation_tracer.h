#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class IComputationTracer
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicPartitionTracerSpec>
{
public:
    struct TPartState
    {
        TDuration TotalDuration = TDuration::Zero();
        TDuration MaxDuration = TDuration::Zero();
        TDuration WallTimeEma = TDuration::Zero();
        TInstant WallTimeUpdateTime = TInstant::Zero();

        NProfiling::TEventTimer Timer = {};
    };

public:
    //! These methods are not thread-safe.
    //! Though reconfiguring and getting metrics are thread-safe.
    virtual NTracing::TTraceContextPtr CreateInitTraceContext() = 0;
    virtual NTracing::TTraceContextPtr StartEpochTraceContext(i64 epochId) = 0;
    virtual NTracing::TTraceContextPtr CreateEpochPartTraceContext(TStringBuf partName) = 0;

    //! Thread-safe.
    virtual THashMap<std::string, TPartState> GetPartStates() = 0;
};

DEFINE_REFCOUNTED_TYPE(IComputationTracer);

IComputationTracerPtr CreateComputationTracer(
    TComputationContextPtr context,
    TComputationSpecPtr spec,
    TDynamicPartitionTracerSpecPtr dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
