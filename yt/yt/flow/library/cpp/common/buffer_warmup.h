#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The `warmup_refresh_period` spec default, shared with hosts that have no
//! spec to read it from.
constexpr TDuration DefaultWarmupRefreshPeriod = TDuration::Seconds(30);

//! Converged buffer sizing of one partition's job, persisted by the job in its
//! partition state so a restarted job can start from the previous steady state
//! instead of warming up.
struct TPartitionBufferWarmup
    : public NYTree::TYsonStructLite
{
    //! Estimated stream speeds, inflated bytes per second.
    THashMap<TStreamId, double> InputSpeeds;
    THashMap<TStreamId, double> OutputSpeeds;
    double EpochCycleSeconds{};

    REGISTER_YSON_STRUCT_LITE(TPartitionBufferWarmup);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Whether the warmup changed enough (>25% on any component) to be worth
//! rewriting wherever it is stored.
bool WarmupDiffers(const TPartitionBufferWarmup& oldWarmup, const TPartitionBufferWarmup& newWarmup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
