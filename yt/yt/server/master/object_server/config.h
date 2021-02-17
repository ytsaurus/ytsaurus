#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/ytlib/object_client/config.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TMutationIdempotizerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enabled;
    TDuration ExpirationTime;
    TDuration ExpirationCheckPeriod;
    int MaxExpiredMutationIdRemovalsPerCommit;

    TMutationIdempotizerConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(true);
        RegisterParameter("expiration_time", ExpirationTime)
            .Default(TDuration::Minutes(5));
        RegisterParameter("expiration_check_period", ExpirationCheckPeriod)
            .Default(TDuration::Seconds(10));
        RegisterParameter("max_expired_mutation_id_removals_per_commit", MaxExpiredMutationIdRemovalsPerCommit)
            .Default(50000);
    }
};

DEFINE_REFCOUNTED_TYPE(TMutationIdempotizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYTree::TYsonSerializable
{ };

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicObjectManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum total weight of objects processed per a single GC mutation.
    int MaxWeightPerGCSweep;

    //! Period between subsequent GC queue checks.
    TDuration GCSweepPeriod;

    //! Period between pairwise secondary cells sync, which enables
    //! advancing from |RemovalAwaitingCellsSync| to |RemovalCommitted| life stage.
    TDuration ObjectRemovalCellsSyncPeriod;

    TMutationIdempotizerConfigPtr MutationIdempotizer;

    //! Per-type list of attributes which will become interned in future versions
    //! and thus should not be set. Maps attribute names to error messages.
    THashMap<EObjectType, THashMap<TString, TString>> ReservedAttributes;

    //! Minimum length of YSON strings that will be interned during mutations.
    //! Outside mutations DefaultYsonStringInternLengthThreshold is always used.
    int YsonStringInternLengthThreshold;

    TDynamicObjectManagerConfig()
    {
        RegisterParameter("max_weight_per_gc_sweep", MaxWeightPerGCSweep)
            .Default(100000);
        RegisterParameter("gc_sweep_period", GCSweepPeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("object_removal_cells_sync_period", ObjectRemovalCellsSyncPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("mutation_idempotizer", MutationIdempotizer)
            .DefaultNew();
        RegisterParameter("reserved_attributes", ReservedAttributes)
            .Default();
        RegisterParameter("yson_string_intern_length_threshold", YsonStringInternLengthThreshold)
            .Default(DefaultYsonStringInternLengthThreshold)
            .InRange(DefaultYsonStringInternLengthThreshold, 1_GB);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum amount of a single batch of Execute requests is allowed to occupy the automaton thread.
    TDuration YieldTimeout;

    //! When user is banned or exceeds the queue size limit, Object Service replies
    //! with an error and caches this error for the given period of time.
    //! This helps to offload the Automaton Thread in case of DOS attack.
    TDuration StickyUserErrorExpireTime;

    //! Maximum time to wait before syncing with another cell.
    TDuration CrossCellSyncDelay;

    //! The amount of time remaining to a batch request timeout when the object
    //! service shall try and send partial (subbatch) response.
    //! NB: this will have no effect if the request's timeout is shorter than this.
    TDuration TimeoutBackoffLeadTime;

    //! Default timeout for ObjectService::Execute (if a client has not provided one).
    TDuration DefaultExecuteTimeout;

    //! Amount of time to reserve when computing the timeout for a forwarded request.
    TDuration ForwardedRequestTimeoutReserve;

    NObjectClient::TObjectServiceCacheConfigPtr MasterCache;

    TObjectServiceConfig()
    {
        RegisterParameter("yield_timeout", YieldTimeout)
            .Default(TDuration::MilliSeconds(10));

        RegisterParameter("sticky_user_error_expire_time", StickyUserErrorExpireTime)
            .Default(TDuration::Seconds(1));

        RegisterParameter("cross_cell_sync_delay", CrossCellSyncDelay)
            .Default(TDuration::MilliSeconds(10));

        RegisterParameter("timeout_backoff_lead_time", TimeoutBackoffLeadTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("default_execute_timeout", DefaultExecuteTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("forwarded_request_timeout_reserve", ForwardedRequestTimeoutReserve)
            .Default(TDuration::Seconds(3));

        RegisterParameter("master_cache", MasterCache)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicObjectServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTwoLevelCache;
    bool EnableMutationBoomerangs;
    TDuration ScheduleReplyRetryBackoff;

    TDynamicObjectServiceConfig()
    {
        RegisterParameter("enable_two_level_cache", EnableTwoLevelCache)
            .Default(false);
        RegisterParameter("enable_mutation_boomerangs", EnableMutationBoomerangs)
            .Default(true);
        RegisterParameter("schedule_reply_retry_backoff", ScheduleReplyRetryBackoff)
            .Default(TDuration::MilliSeconds(100));
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
