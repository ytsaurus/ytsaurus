#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TObjectAttributeCacheConfig
    : public TAsyncExpiringCacheConfig
{
    NApi::TSerializableMasterReadOptionsPtr MasterReadOptions;
    //! User for executing requests to master.
    std::string UserName;

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

struct TObjectServiceCacheConfig
    : public TSlruCacheConfig
{
    REGISTER_YSON_STRUCT(TObjectServiceCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TObjectServiceCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
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

struct TCachingObjectServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TObjectServiceCacheConfig
{
    REGISTER_YSON_STRUCT(TCachingObjectServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingObjectServiceDynamicConfig
    : public NRpc::TThrottlingChannelDynamicConfig
    , public TObjectServiceCacheDynamicConfig
{
    double CacheTtlRatio;

    REGISTER_YSON_STRUCT(TCachingObjectServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReqExecuteBatchWithRetriesConfig
    : public NYTree::TYsonStruct
{
    TDuration StartBackoff;
    TDuration MaxBackoff;
    double BackoffMultiplier;
    int RetryCount;

    REGISTER_YSON_STRUCT(TReqExecuteBatchWithRetriesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReqExecuteBatchWithRetriesConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAbcConfig
    : public virtual NYTree::TYsonStruct
{
    int Id;
    std::optional<TString> Name;
    TString Slug;

    REGISTER_YSON_STRUCT(TAbcConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAbcConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
