#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/cypress_election/config.h>

#include <yt/yt/core/bus/tcp/public.h>
#include <yt/yt/core/ypath/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct TPersistedStateManagerConfig
    : public virtual NYTree::TYsonStruct
{
    // Timeout for every YT request.
    TDuration Timeout;

    // Select limit from dynamic table.
    ssize_t MaxReadsPerTransaction{};

    // Maximal number of modified rows of dynamic table in one transaction.
    ssize_t MaxWritesPerTransaction{};

    REGISTER_YSON_STRUCT(TPersistedStateManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistedStateManagerConfig);

////////////////////////////////////////////////////////////////////////////////

struct TLeaseManagerConfig
    : public virtual NYTree::TYsonStruct
{
    TDuration LeaseTimeout;
    TDuration LeasePingPeriod;
    i64 MaxConcurrentRequests{};

    REGISTER_YSON_STRUCT(TLeaseManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLeaseManagerConfig);

////////////////////////////////////////////////////////////////////////////////

//! The scheduling cadence while the pipeline has no spec: such iterations are no-ops that only
//! refresh the leader lease row, and at the full cadence their commits starve the longer set-spec
//! commit of conflict-free windows on that row.
inline constexpr TDuration NoSpecIterationBackoff = TDuration::Seconds(1);

//! How many scheduling cadences a dyntable leader lease must outlast, checked at config load.
//! Nothing refreshes the leader row between two fenced commits, so a ttl that barely covers one
//! cadence demotes the leader as soon as a single iteration runs long.
inline constexpr int MinLeaderLeaseTtlToCadenceRatio = 3;

//! How many leader lease ttls the pipeline-wide lease deadline must outlast, checked at config
//! load. A replica cannot take over before the leader lease expires, and only then does it read
//! the lease table and refresh the deadline — all of it inside whatever the deceased leader left
//! of the deadline, which is at most a third short of the full timeout. Three ttls leave the
//! handover the same kind of margin the cadence check leaves an iteration.
inline constexpr int MinLeaseTimeoutToLeaderLeaseTtlRatio = 3;

////////////////////////////////////////////////////////////////////////////////

//! Settings shared by every mechanism that elects the leader and fences its transactions.
struct TElectionBackendConfigBase
    : public virtual NYTree::TYsonStruct
{
    //! How often a follower attempts to win the election.
    TDuration LockAcquisitionPeriod;
    //! How often the cached leader identity is refreshed.
    TDuration LeaderCacheUpdatePeriod;

    REGISTER_YSON_STRUCT(TElectionBackendConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TElectionBackendConfigBase);

////////////////////////////////////////////////////////////////////////////////

//! An exclusive Cypress lock; its transaction fences the leader's work as a prerequisite.
struct TCypressElectionBackendConfig
    : public TElectionBackendConfigBase
{
    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;

    REGISTER_YSON_STRUCT(TCypressElectionBackendConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionBackendConfig);

////////////////////////////////////////////////////////////////////////////////

//! A leader-lease row in the pipeline tables; every fenced tablet transaction validates and
//! refreshes it on commit (see dyntable_lease.h).
struct TDyntableElectionBackendConfig
    : public TElectionBackendConfigBase
{
    //! How long a written leader lease stays fresh.
    TDuration LeaderLeaseTtl;
    //! Self-demote when no renewal has succeeded for this long (the leases table is unreachable).
    TDuration DetachTimeout;

    REGISTER_YSON_STRUCT(TDyntableElectionBackendConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDyntableElectionBackendConfig);

////////////////////////////////////////////////////////////////////////////////

//! The discriminator stays "backend" rather than the default "type": the parameter predates the
//! polymorphic layout and is already written in deployed configs.
//!
//! Switching the backend of a live pipeline is not supported: the backends fence through
//! different mechanisms, so controllers running different ones do not see each other's
//! leadership. Stop the pipeline, switch the config, then start it — a stopped pipeline holds no
//! jobs, hence no leases of the old flavour to convert.
inline constexpr const char ElectionBackendDiscriminator[] = "backend";

DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM_WITH_CUSTOM_DISCRIMINATOR_AND_DEFAULT(
    ElectionManagerConfig,
    ElectionBackendDiscriminator,
    EElectionBackend,
    Cypress,
    TElectionBackendConfigBase,
    ((Cypress)(TCypressElectionBackendConfig))((Dyntable)(TDyntableElectionBackendConfig)));

////////////////////////////////////////////////////////////////////////////////

struct TControllerServiceConfig
    : public virtual NYTree::TYsonStruct
{
    int SetSpecRetryCount{};
    TDuration SetSpecRetryPeriod;

    TLoadThroughputThrottlerSpecPtr TablesThrottler;

    REGISTER_YSON_STRUCT(TControllerServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerServiceConfig);

////////////////////////////////////////////////////////////////////////////////

struct TControllerConfig
    : public virtual NYTree::TYsonStruct
{
    int ControllerThreads{};

    TDuration OrchidUpdatePeriod;

    TDuration WarmUpTime;
    TDuration SchedulerPeriod;
    TDuration CachePeriod;
    TDuration FeedbackPeriod;
    TDuration MetricsPeriod;
    TDuration WriteOwnRetryableErrorsPeriod;
    TDuration PublishRetryPeriod;
    TDuration PublishTimeout;

    TElectionManagerConfig ElectionManager;

    TPersistedStateManagerConfigPtr PersistedStateManager;
    TLeaseManagerConfigPtr LeaseManager;
    TControllerServiceConfigPtr ControllerService;

    // For channel factory.
    NBus::NTcp::TBusConfigPtr Bus;

    REGISTER_YSON_STRUCT(TControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
