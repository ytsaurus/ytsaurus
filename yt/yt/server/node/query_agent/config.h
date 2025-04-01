#pragma once

#include "public.h"

#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct TQueryAgentConfig
    : public NQueryClient::TExecutorConfig
{
    int QueryThreadPoolSize;
    int LookupThreadPoolSize;
    int FetchThreadPoolSize;
    int TableRowFetchThreadPoolSize;
    int MaxSubsplitsPerTablet;
    int MaxSubqueries;
    int MaxQueryRetries;
    size_t DesiredUncompressedResponseBlockSize;

    TSlruCacheConfigPtr FunctionImplCache;

    TAsyncExpiringCacheConfigPtr PoolWeightCache;

    bool RejectUponThrottlerOverdraft;
    bool AccountUserBackendOutTraffic;
    bool UseQueryPoolForLookups;
    bool UseQueryPoolForInMemoryLookups;

    i64 MaxPullQueueResponseDataWeight;
    i64 PullRowsReadDataWeightLimit;
    TDuration PullRowsTimeoutSlack;

    REGISTER_YSON_STRUCT(TQueryAgentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryAgentDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<int> QueryThreadPoolSize;
    std::optional<int> LookupThreadPoolSize;
    std::optional<int> FetchThreadPoolSize;
    std::optional<int> TableRowFetchThreadPoolSize;

    std::optional<bool> RejectUponThrottlerOverdraft;
    bool RejectInMemoryRequestsUponThrottlerOverdraft;

    std::optional<i64> MaxPullQueueResponseDataWeight;
    std::optional<bool> AccountUserBackendOutTraffic;
    std::optional<bool> UseQueryPoolForLookups;

    REGISTER_YSON_STRUCT(TQueryAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

