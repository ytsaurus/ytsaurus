#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionPresenceCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration FinishedTransactionEvictionDelay;
    TDuration EvictionCheckPeriod;
    int MaxEvictedTransactionsPerCheck;

    TTransactionPresenceCacheConfig()
    {
        RegisterParameter("finished_transaction_eviction_delay", FinishedTransactionEvictionDelay)
            .Default(TDuration::Minutes(5));
        RegisterParameter("eviction_check_period", EvictionCheckPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_evicted_transactions_per_check", MaxEvictedTransactionsPerCheck)
            .Default(25000)
            .GreaterThanOrEqual(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionPresenceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TBoomerangTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration StuckBoomerangWaveExpirationTime;
    TDuration StuckBoomerangWaveExpirationCheckPeriod;
    int MaxExpiredBoomerangWaveRemovalsPerCheck;

    TBoomerangTrackerConfig()
    {
        RegisterParameter("stuck_boomerang_wave_expiration_time", StuckBoomerangWaveExpirationTime)
            .Default(TDuration::Minutes(3));
        RegisterParameter("stuck_boomerang_wave_expiration_check_period", StuckBoomerangWaveExpirationCheckPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_expired_boomerang_wave_removals_per_check", MaxExpiredBoomerangWaveRemovalsPerCheck)
            .Default(1000)
            .GreaterThanOrEqual(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TBoomerangTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    int MaxTransactionDepth;

    // COMPAT(shakurov): remove this once all (anticipated at the moment of this
    // writing) problems with lazy tx replication have been ironed out.
    bool EnableLazyTransactionReplication;

    bool EnableDedicatedUploadTransactionObjectTypes;

    TTransactionPresenceCacheConfigPtr TransactionPresenceCache;
    TBoomerangTrackerConfigPtr BoomerangTracker;

    TDynamicTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .Default(TDuration::Minutes(60));
        RegisterParameter("max_transaction_depth", MaxTransactionDepth)
            .GreaterThan(0)
            .Default(32);
        RegisterParameter("enable_lazy_transaction_replication", EnableLazyTransactionReplication)
            .Default(true);
        RegisterParameter("transaction_presence_cache", TransactionPresenceCache)
            .DefaultNew();
        RegisterParameter("boomerang_tracker", BoomerangTracker)
            .DefaultNew();

        // COMPAT(shakurov): this is an emergency button for unforeseen circumstances.
        // To be removed once sharded transactions (a.k.a. v. 20.3) are stabilized.
        RegisterParameter("enable_dedicated_upload_transaction_object_types", EnableDedicatedUploadTransactionObjectTypes)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
