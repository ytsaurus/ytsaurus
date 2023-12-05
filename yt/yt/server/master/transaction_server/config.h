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

    TTransactionPresenceCacheConfigPtr TransactionPresenceCache;
    TBoomerangTrackerConfigPtr BoomerangTracker;

    TDuration ProfilingPeriod;

    bool IgnoreCypressTransactions;

    bool CheckTransactionIsCompatibleWithMethod;

    // NB: If type is not present in this map, then all methods are allowed.
    THashMap<NObjectClient::EObjectType, THashSet<TString>> TransactionTypeToMethodWhitelist;

    // COMPAT(h0pless): This is a panic button in case new types cause issues for users.
    bool EnableDedicatedTypesForSystemTransactions;

    // COMPAT(kvk1920): Remove after enabling on every cluster.
    bool ForbidTransactionActionsForCypressTransactions;

    // Testing option.
    bool ThrowOnLeaseRevokation;

    REGISTER_YSON_STRUCT(TDynamicTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
