#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

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
    TLimitConfigPtr Bandwidth;
    TLimitConfigPtr Rps;

    REGISTER_YSON_STRUCT(TClusterLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TLimitConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<i64> Limit;

    REGISTER_YSON_STRUCT(TLimitConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLimitConfig)

////////////////////////////////////////////////////////////////////////////////

//! Get //sys/cluster_throttlers configuration file as YSON.
std::optional<NYT::NYson::TYsonString> GetClusterThrottlersYson(NApi::NNative::IClientPtr client);
//! Make cluster throttlers config from YSON file.
TClusterThrottlersConfigPtr MakeClusterThrottlersConfig(const NYT::NYson::TYsonString& yson);
//! Get //sys/cluster_throttlers configuration file as config.
TClusterThrottlersConfigPtr GetClusterThrottlersConfig(NApi::NNative::IClientPtr client);
//! Return true if two configs are the same.
bool AreClusterThrottlersConfigsEqual(TClusterThrottlersConfigPtr lhs, TClusterThrottlersConfigPtr rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
