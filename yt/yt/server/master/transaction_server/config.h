#pragma once

#include "public.h"

#include <yt/yt/core/misc/config.h>

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

struct TTransactionFinisherConfig
    : public NYTree::TYsonStruct
{
    TExponentialBackoffOptions Retries;
    TDuration ScanPeriod;
    int MaxTransactionsPerScan;
    bool AlertOnTooManyRetries;

    REGISTER_YSON_STRUCT(TTransactionFinisherConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionFinisherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTransactionManagerTestingConfig
    : public NYTree::TYsonStruct
{
    bool ThrowOnLeaseRevocation;

    std::optional<TDuration> PreparedTransactionsBarrierDelay;

    THashSet<TTransactionId> PrerequisiteCheckFailureDuringCommitOfTransactions;

    TDuration ArtificialParticipantCommitDelay;

    REGISTER_YSON_STRUCT(TDynamicTransactionManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTransactionManagerConfig
    : public NYTree::TYsonStruct
{
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(1000);
    static constexpr auto DefaultAlertFlushPeriod = TDuration::Seconds(10);

    TDuration MaxTransactionTimeout;
    int MaxTransactionDepth;

    TTransactionPresenceCacheConfigPtr TransactionPresenceCache;
    TBoomerangTrackerConfigPtr BoomerangTracker;

    TTransactionFinisherConfigPtr TransactionFinisher;

    TDuration ProfilingPeriod;
    TDuration AlertFlushPeriod;

    bool IgnoreCypressTransactions;

    bool CheckTransactionIsCompatibleWithMethod;
    bool AlertTransactionIsNotCompatibleWithMethod;

    bool EnableWaitUntilPreparedTransactionsFinished;

    // NB: If type is not present in this map, then all methods are allowed.
    THashMap<NObjectClient::EObjectType, THashSet<std::string>> TransactionTypeToMethodWhitelist;

    // COMPAT(kvk1920): Remove after enabling on every cluster.
    bool ForbidTransactionActionsForCypressTransactions;

    // COMPAT(kvk1920)
    // Allows to use native transaction ID instead of externalized one and vice versa.
    bool EnableNonStrictExternalizedTransactionUsage;

    TDynamicTransactionManagerTestingConfigPtr Testing;

    // COMPAT(shakurov)
    bool EnableStartForeignTransactionFixes;

    // Alert if a user has too many active native transactions on a cell.
    // NB: Alert may not be triggered if threshold is exceeded by concurrent requests.
    int ActiveTransactionCountAlertThreshold;

    // Deny starting transactions if the user has too many active native transactions on the cell.
    // NB: May be exceeded by concurrent requests.
    int ActiveTransactionCountLimit;

    bool EnforceActiveTransactionCountLimit;

    REGISTER_YSON_STRUCT(TDynamicTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
