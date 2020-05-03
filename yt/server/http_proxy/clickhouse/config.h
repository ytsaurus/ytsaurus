#pragma once

#include "private.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/http/config.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TCliqueCacheConfig
    : public NYTree::TYsonSerializable
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

    TCliqueCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TCliqueCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TClickHouseConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Path to folder in cypress which contains general information about all cliques.
    TString DiscoveryPath;

    NHttp::TClientConfigPtr HttpClient;
    TDuration ProfilingPeriod;

    //! Cache for cliques's discovery.
    TCliqueCacheConfigPtr CliqueCache;

    //! Prevent throwing an error if the request does not contain an authorization header.
    //! If authorization is disabled in proxy config, this flag will be set to true automatically.
    bool IgnoreMissingCredentials;

    //! How many times the proxy will retry sending the request to the randomly chosen instance
    //! when the chosen instance does not respond or respond with MovedPermanently status code.
    int DeadInstanceRetryCount;

    //! How many times the proxy can retry sending the request with old discovery from the cache.
    //! If this limit is exсeeded, next retry will be performed after force update of discovery.
    int RetryWithoutUpdateLimit;

    //! Force update can be skipped by discovery if the data is younger than this age threshold.
    TDuration ForceDiscoveryUpdateAgeThreshold;

    //! Timeout to resolve alias.
    TDuration AliasResolutionTimeout;

    //! If set to true, profiler won't wait a second to update a counter.
    //! It is useful for testing.
    bool ForceEnqueueProfiling;

    TClickHouseConfig();
};

DEFINE_REFCOUNTED_TYPE(TClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
