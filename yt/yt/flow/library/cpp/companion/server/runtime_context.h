#pragma once

#include "public.h"

#include "codec.h"

#include <yt/yt/flow/library/cpp/process_function/host/computation_runtime_context.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCompanionRuntimeContext);

//! Companion-side IRuntimeContext: the production context minus worker-only
//! facilities (distributed throttlers) and minus
//! #TComputationStreamSpecStorage::ComputeKey() for a computed group-by schema:
//! process functions take the key from the input, which arrives with it.
class TCompanionRuntimeContext
    : public TComputationRuntimeContext
{
public:
    using TComputationRuntimeContext::TComputationRuntimeContext;

    //! Installs the per-batch state the worker sent with |input| and the latest dynamic
    //! `function_parameters` node.
    void RefreshBatchState(const TBatchInput& input, NYTree::IMapNodePtr dynamicParametersNode);

    NConcurrency::IThroughputThrottlerPtr GetThrottlerOrThrow(const TThrottlerId& throttlerId) override;
    NConcurrency::IThroughputThrottlerPtr TryGetThrottler(const TThrottlerId& throttlerId) override;

    //! Throws when the batch carried no epoch timestamp: throwing beats silently
    //! returning zero, which would misdate every relative timer.
    TSystemTimestamp GetCurrentTimestamp() const override;

private:
    bool HasCurrentTimestamp_ = false;
};

DEFINE_REFCOUNTED_TYPE(TCompanionRuntimeContext);

////////////////////////////////////////////////////////////////////////////////

//! Builds the epoch watermark snapshot from the wire watermarks. The wire carries
//! one (event) watermark per stream and the epoch timestamp.
TWatermarkStatePtr BuildWatermarkState(
    const THashMap<TStreamId, TSystemTimestamp>& watermarks,
    std::optional<TSystemTimestamp> currentTimestamp = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
