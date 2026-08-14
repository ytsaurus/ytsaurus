#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/process_function/host/computation_runtime_context.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCompanionRuntimeContext);

//! Companion-side IRuntimeContext: the production context minus worker-only
//! facilities (distributed throttlers, the epoch timestamp) and minus
//! #TComputationStreamSpecStorage::ComputeKey() for a computed group-by schema:
//! process functions take the key from the input, which arrives with it.
class TCompanionRuntimeContext
    : public TComputationRuntimeContext
{
public:
    using TComputationRuntimeContext::TComputationRuntimeContext;

    NConcurrency::IThroughputThrottlerPtr GetThrottler(const TThrottlerId& throttlerId) override;

    //! The epoch timestamp does not travel to the companion; throwing beats
    //! silently returning zero (which would misdate every relative timer).
    TSystemTimestamp GetCurrentTimestamp() const override;

    //! Likewise the epoch sequence number: it is minted by the worker's epoch loop and
    //! never crosses the wire.
    TUniqueSeqNo GetEpochUniqueSeqNo() const override;
};

DEFINE_REFCOUNTED_TYPE(TCompanionRuntimeContext);

////////////////////////////////////////////////////////////////////////////////

//! Builds the epoch watermark snapshot from the wire watermarks. The wire carries
//! one (event) watermark per stream and no epoch timestamp.
TWatermarkStatePtr BuildWatermarkState(const THashMap<TStreamId, TSystemTimestamp>& watermarks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
