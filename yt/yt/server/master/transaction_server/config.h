#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPresenceCacheConfig
    : public NYTree::TYsonStruct
{
    TDuration FinishedTransactionEvictionDelay;
    TDuration EvictionCheckPeriod;
    int MaxEvictedTransactionsPerCheck;

    REGISTER_YSON_STRUCT(TTransactionPresenceCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionPresenceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBoomerangTrackerConfig
    : public NYTree::TYsonStruct
{
    TDuration StuckBoomerangWaveExpirationTime;
    TDuration StuckBoomerangWaveExpirationCheckPeriod;
    int MaxExpiredBoomerangWaveRemovalsPerCheck;

    REGISTER_YSON_STRUCT(TBoomerangTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBoomerangTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTransactionManagerConfig
    : public NYTree::TYsonStruct
{
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(1000);

    TDuration MaxTransactionTimeout;
    int MaxTransactionDepth;

    TTransactionPresenceCacheConfigPtr TransactionPresenceCache;
    TBoomerangTrackerConfigPtr BoomerangTracker;

    TDuration ProfilingPeriod;

    bool IgnoreCypressTransactions;

    bool CheckTransactionIsCompatibleWithMethod;
    bool AlertTransactionIsNotCompatibleWithMethod;

    // NB: If type is not present in this map, then all methods are allowed.
    THashMap<NObjectClient::EObjectType, THashSet<TString>> TransactionTypeToMethodWhitelist;

    // COMPAT(kvk1920): Remove after enabling on every cluster.
    bool ForbidTransactionActionsForCypressTransactions;

    // COMPAT(kvk1920)
    // Allows to use native transaction ID instead of externalized one and vice versa.
    bool EnableNonStrictExternalizedTransactionUsage;

    // Testing option.
    bool ThrowOnLeaseRevocation;

    // COMPAT(shakurov)
    bool EnableStartForeignTransactionFixes;

    // COMPAT(cherepashka)
    bool EnableCypressMirroredToSequoiaPrerequisiteTransactionValidationViaLeases;

    REGISTER_YSON_STRUCT(TDynamicTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
