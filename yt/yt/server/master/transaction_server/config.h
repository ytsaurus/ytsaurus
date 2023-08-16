#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionPresenceCacheConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration FinishedTransactionEvictionDelay;
    TDuration EvictionCheckPeriod;
    int MaxEvictedTransactionsPerCheck;

    REGISTER_YSON_STRUCT(TTransactionPresenceCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionPresenceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TBoomerangTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration StuckBoomerangWaveExpirationTime;
    TDuration StuckBoomerangWaveExpirationCheckPeriod;
    int MaxExpiredBoomerangWaveRemovalsPerCheck;

    REGISTER_YSON_STRUCT(TBoomerangTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBoomerangTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTransactionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(1000);

    TDuration MaxTransactionTimeout;
    int MaxTransactionDepth;

    // COMPAT(shakurov): remove this once all (anticipated at the moment of this
    // writing) problems with lazy tx replication have been ironed out.
    bool EnableLazyTransactionReplication;

    TTransactionPresenceCacheConfigPtr TransactionPresenceCache;
    TBoomerangTrackerConfigPtr BoomerangTracker;

    TDuration ProfilingPeriod;

    bool IgnoreCypressTransactions;

    REGISTER_YSON_STRUCT(TDynamicTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
