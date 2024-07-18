#pragma once

#include "private.h"

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryCacheConfig
    : public NYTree::TYsonStruct
{
public:
    //! Base config for SlruCache.
    TSlruCacheConfigPtr CacheBase;

    //! Update of data in discovery will be scheduled if data is older than this age threshold.
    //! Update is asynchronous, current request will be processed with data from the cache.
    TDuration SoftAgeThreshold;

    //! Proxy will never use cached data if it is older than this age threshold. Will wait for update instead.
    TDuration HardAgeThreshold;

    //! Is used for updating discovery from master cache.
    TDuration MasterCacheExpireTime;

    //! How long the proxy will not send new requests to the instance after connection error to it.
    TDuration UnavailableInstanceBanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TStaticClickHouseConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ProfilingPeriod;

    //! Operation cache (for alias resolution and operation ACL fetching).
    TAsyncExpiringCacheConfigPtr OperationCache;

    //! Permission cache (for authorizing against operation ACL).
    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;

    //! Clique discovery cache.
    TDiscoveryCacheConfigPtr DiscoveryCache;

    TDuration OperationIdUpdatePeriod;

    //! For testing.
    bool PopulateUserWithToken;

    NYPath::TYPath ChytStrawberryPath;

    REGISTER_YSON_STRUCT(TStaticClickHouseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicClickHouseConfig
    : public NYTree::TYsonStruct
{
public:
    //! Path to folder in Cypress which contains general information about all cliques.
    TString DiscoveryPath;

    NHttp::TClientConfigPtr HttpClient;

    //! Prevent throwing an error if the request does not contain an authorization header.
    //! If authorization is disabled in proxy config, this flag will be set to true automatically.
    bool IgnoreMissingCredentials;

    //! How many times the proxy will retry sending the request to the randomly chosen instance
    //! when the chosen instance does not respond or respond with MovedPermanently status code.
    int DeadInstanceRetryCount;

    //! How many times the proxy can retry sending the request with old discovery from the cache.
    //! If this limit is exceeded, next retry will be performed after force update of discovery.
    int RetryWithoutUpdateLimit;

    //! Force update can be skipped by discovery if the data is younger than this age threshold.
    TDuration ForceDiscoveryUpdateAgeThreshold;

    //! Timeout to resolve alias.
    TDuration AliasResolutionTimeout;

    //! If set, force enable or disable tracing for requests from DataLens.
    std::optional<bool> DatalensTracingOverride;

    REGISTER_YSON_STRUCT(TDynamicClickHouseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
