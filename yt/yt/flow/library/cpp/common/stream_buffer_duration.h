#pragma once

#include "public.h"
#include "spec.h"

#include <functional>
#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStreamBufferDurationEstimate
{
    double ChainSeconds = 0;
};

struct TStreamBufferDurationSnapshot
{
    THashMap<TStreamId, TStreamBufferDurationEstimate> Streams;
    THashMap<TComputationId, TStreamBufferDurationEstimate> InputDurations;
};

using TStreamBufferDurationSnapshotPtr = std::shared_ptr<const TStreamBufferDurationSnapshot>;
using TInputAcknowledgementPolicyLookup = std::function<EInputAcknowledgementPolicy(const std::string&)>;

TStreamBufferDurationSnapshotPtr BuildStreamBufferDurationSnapshot(
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec,
    TInputAcknowledgementPolicyLookup lookup = {});

bool IsAutomaticBufferDurationEnabled(
    const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec);

//! Returns the configured upper cap when automatic sizing is unavailable or disabled.
double ComputeBufferDurationSeconds(
    const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec,
    const TStreamBufferDurationEstimate* estimate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
