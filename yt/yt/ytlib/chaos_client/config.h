#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/config.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TChaosCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! Splay for directory updates period.
    TDuration SyncPeriodSplay;

    bool SyncAllChaosCells;

    REGISTER_YSON_STRUCT(TChaosCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosResidencyCacheConfig
    : public TAsyncExpiringCacheConfig
{
    bool EnableClientMode;

    REGISTER_YSON_STRUCT(TChaosResidencyCacheConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosResidencyCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardChannelConfig
    : public NRpc::TRetryingChannelConfig
{
    TDuration RpcAcknowledgementTimeout;

    REGISTER_YSON_STRUCT(TReplicationCardChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardsWatcherConfig
    : public NYTree::TYsonStruct
{
    TDuration PollExpirationTime;
    TDuration GoneCardsExpirationTime;
    TDuration ExpirationSweepPeriod;

    REGISTER_YSON_STRUCT(TReplicationCardsWatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardsWatcherConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
