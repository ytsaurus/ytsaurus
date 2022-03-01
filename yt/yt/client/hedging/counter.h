#pragma once

#include <yt/yt/core/misc/ref_counted.h>
#include <yt/yt/library/profiling/sensor.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYT::NClient::NHedging::NRpc {

// ! Counters which will be collected from yt-client
struct TCounter final {
    TCounter(const TString& clusterName);
    TCounter(const NYT::NProfiling::TTagSet& tagSet);
    TCounter(const NYT::NProfiling::TRegistry& registry);

    NYT::NProfiling::TCounter SuccessRequestCount;
    NYT::NProfiling::TCounter CancelRequestCount;
    NYT::NProfiling::TCounter ErrorRequestCount;
    NYT::NProfiling::TTimeGauge EffectivePenalty;
    NYT::NProfiling::TTimeGauge ExternalPenalty;
    NYT::NProfiling::TEventTimer RequestDuration;
};

DEFINE_REFCOUNTED_TYPE(TCounter);

// ! Counters for TReplicaionLagPenaltyProvider
struct TLagPenaltyProviderCounters final {
    TLagPenaltyProviderCounters(const NYT::NProfiling::TRegistry& registry, const TVector<TString>& clusters);
    TLagPenaltyProviderCounters(const TString& tablePath, const TVector<TString>& replicaClusters);

    NYT::NProfiling::TCounter SuccessRequestCount;
    NYT::NProfiling::TCounter ErrorRequestCount;
    THashMap<TString, NYT::NProfiling::TGauge> LagTabletsCount; // cluster -> # of tablets
    NYT::NProfiling::TGauge TotalTabletsCount;
};

DEFINE_REFCOUNTED_TYPE(TLagPenaltyProviderCounters);

} // namespace NYT::NClient::NHedging::NRpc
