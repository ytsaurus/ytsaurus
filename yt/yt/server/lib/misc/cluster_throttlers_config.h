#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TClusterThrottlersConfig
    : public NYTree::TYsonStruct
{
public:
    NDistributedThrottler::TDistributedThrottlerConfigPtr DistributedThrottler;
    THashMap<TString, TClusterLimitsConfigPtr> ClusterLimits;
    //! Rate might be too high if maximum estimated time required to read pending bytes is higher than this value.
    TDuration MaxEstimatedTimeToReadPendingBytesThreshold;
    //! Rate is too high if minumum estimated time required to read pending bytes is higher than this value.
    TDuration MinEstimatedTimeToReadPendingBytesThreshold;
    //! Rate is too high if rate limit ratio is higher than this value.
    double RateLimitRatioHardThreshold;
    //! Rate might be too high if rate limit ratio is higher than this value.
    double RateLimitRatioSoftThreshold;
    //! How often cluster throttlers config is updated.
    TDuration UpdatePeriod;
    bool Enabled;

    REGISTER_YSON_STRUCT(TClusterThrottlersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterThrottlersConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterLimitsConfig
    : public NYTree::TYsonStruct
{
public:
    NConcurrency::TThroughputThrottlerConfigPtr Bandwidth;
    NConcurrency::TThroughputThrottlerConfigPtr Rps;

    REGISTER_YSON_STRUCT(TClusterLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

//! Get //sys/cluster_throttlers configuration file as YSON.
TFuture<NYT::NYson::TYsonString> GetClusterThrottlersYson(NApi::NNative::IClientPtr client);
//! Return true if two configs are the same.
bool AreClusterThrottlersConfigsEqual(TClusterThrottlersConfigPtr lhs, TClusterThrottlersConfigPtr rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
