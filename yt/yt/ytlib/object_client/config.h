#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectAttributeCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    NApi::TSerializableMasterReadOptionsPtr MasterReadOptions;

    REGISTER_YSON_STRUCT(TObjectAttributeCacheConfig);

    static void Register(TRegistrar registrar);

private:
    // COMPAT(dakovalkov): following options are deprecated.
    // Use MasterReadOptions instead.
    // TODO(dakovalkov): delete them after elimination from all configs.
    std::optional<NApi::EMasterChannelKind> ReadFrom_;
    std::optional<TDuration> MasterCacheExpireAfterSuccessfulUpdateTime_;
    std::optional<TDuration> MasterCacheExpireAfterFailedUpdateTime_;
    std::optional<int> MasterCacheStickyGroupSize_;
};

DEFINE_REFCOUNTED_TYPE(TObjectAttributeCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheConfig
    : public TSlruCacheConfig
{
public:
    REGISTER_YSON_STRUCT(TObjectServiceCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
public:
    i64 EntryByteRateLimit;
    i64 TopEntryByteRateThreshold;
    TDuration AggregationPeriod;
    int MinAdvisedStickyGroupSize;
    int MaxAdvisedStickyGroupSize;

    REGISTER_YSON_STRUCT(TObjectServiceCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TObjectServiceCacheConfig
{
public:
    REGISTER_YSON_STRUCT(TCachingObjectServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectServiceDynamicConfig
    : public NRpc::TThrottlingChannelDynamicConfig
    , public TObjectServiceCacheDynamicConfig
{
public:
    double CacheTtlRatio;

    REGISTER_YSON_STRUCT(TCachingObjectServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TReqExecuteBatchWithRetriesConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration StartBackoff;
    TDuration MaxBackoff;
    double BackoffMultiplier;
    int RetryCount;

    REGISTER_YSON_STRUCT(TReqExecuteBatchWithRetriesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReqExecuteBatchWithRetriesConfig)

////////////////////////////////////////////////////////////////////////////////

class TAbcConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int Id;
    std::optional<TString> Name;
    TString Slug;

    REGISTER_YSON_STRUCT(TAbcConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAbcConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
