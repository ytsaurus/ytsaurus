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

//! Job leases: the prerequisites workers attach to the commits of their epochs. Not to be confused
//! with the leader lease of #TElectionBackendConfigBase and its descendants.
struct TLeaseManagerConfig
    : public virtual NYTree::TYsonStruct
{
    //! How long a lease outlives its last prolongation. Under the dyntable backend the leases share
    //! a single deadline, so this is the lifetime of the whole fleet rather than of one lease.
    TDuration LeaseTimeout;
    //! How often the leader prolongs the leases. Every backend prolongs from the leader and none
    //! from the worker, but the machinery differs: the chaos leases are pinged by one periodic
    //! executor, the Cypress ones by a client-side pinger per lease transaction, and the shared
    //! dyntable deadline is simply rewritten.
    TDuration LeasePingPeriod;
    //! Caps the lease requests the manager keeps in flight at once when it attaches to leases or
    //! terminates them. Prolongation is not capped by it: the chaos pings are dispatched in one
    //! fire-and-forget round, and the Cypress ones belong to the lease transactions themselves.
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
//! the lease table and refresh the deadline. Three ttls leave the handover the same kind of margin
//! the cadence check leaves an iteration. What is left of the deadline by then is short of the
//! full timeout by up to one refresh period, so that period is added on top rather than counted in.
inline constexpr int MinLeaseTimeoutToLeaderLeaseTtlRatio = 3;

//! How many prolongation rounds a job lease must outlast, checked at config load. The rounds are
//! the only thing keeping the leases alive, so a timeout that covers just one of them kills every
//! job of the pipeline on a single failed round; three leave two whole retry windows.
inline constexpr int MinLeaseTimeoutToLeasePingPeriodRatio = 3;

//! How many handovers a job lease must outlast under the chaos backend, checked at config load.
//! Chaos job leases are pinged by the leader alone and the pinger stops with its leadership, so a
//! lease has to survive the whole change of leader: waiting out the dead leader's own lease,
//! winning the lock, recovering the state and warming up. Those make up the handover estimate;
//! doubling it leaves room for the recovery, whose duration the config cannot know. The lease is
//! also up to a ping period old by then, so that period is added on top rather than counted in.
inline constexpr int MinLeaseTimeoutToChaosHandoverRatio = 2;

////////////////////////////////////////////////////////////////////////////////

//! Settings shared by every mechanism that elects the leader and fences its transactions.
//!
//! Each backend also carries a |leader_lease_ttl|: how long the fence outlives the leader's last
//! confirmation, and therefore how long a contender waits before considering the leadership free.
//! It is registered per backend rather than here because the defaults differ.
struct TElectionBackendConfigBase
    : public virtual NYTree::TYsonStruct
{
    //! How often a follower attempts to win the election. Adds to the duration of a handover, so
    //! there is little reason to raise it; an attempt is cheap under every backend.
    TDuration LockAcquisitionPeriod;

    REGISTER_YSON_STRUCT(TElectionBackendConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TElectionBackendConfigBase);

////////////////////////////////////////////////////////////////////////////////

//! An exclusive Cypress lock; its transaction fences the leader's work as a prerequisite.
struct TCypressElectionBackendConfig
    : public TElectionBackendConfigBase
{
    //! The timeout of the lock transaction: the master releases the lock to the next contender
    //! once it goes unpinged for this long.
    TDuration LeaderLeaseTtl;
    //! How often the transaction is pinged.
    TDuration LeaderLeasePingPeriod;
    //! How often the identity of the leader is refetched into the election manager's cache.
    //! Cypress-only: the other backends publish the leader into the flow control table, which is
    //! what every reader of ours goes to anyway.
    TDuration LeaderCacheUpdatePeriod;

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
    //! How long a written leader lease stays fresh. There is no ping period to go with it: the
    //! lease rides the fenced commits, and the election loop only renews it during recovery.
    TDuration LeaderLeaseTtl;
    //! Self-demote when no renewal has succeeded for this long (the leases table is unreachable).
    TDuration DetachTimeout;

    REGISTER_YSON_STRUCT(TDyntableElectionBackendConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDyntableElectionBackendConfig);

////////////////////////////////////////////////////////////////////////////////

//! A chaos lease recorded in the pipeline's leader election lock table; the same bundle hosts the
//! per-job leases the workers commit under.
struct TChaosElectionBackendConfig
    : public TElectionBackendConfigBase
{
    std::string ChaosCellBundle;

    //! How long the chaos lease outlives its last ping, and the value the contenders read out of
    //! the lock table to decide that the leadership is free.
    TDuration LeaderLeaseTtl;
    //! How often the lease is pinged and the ping recorded in the lock table.
    TDuration LeaderLeasePingPeriod;

    REGISTER_YSON_STRUCT(TChaosElectionBackendConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosElectionBackendConfig);

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
    ((Cypress)(TCypressElectionBackendConfig))((Dyntable)(TDyntableElectionBackendConfig))(
        (Chaos)(TChaosElectionBackendConfig)));

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
    //! The lifetime of the transactions a leadership publication attempt opens, and the timeout of
    //! the attribute write it makes. It does not bound every request of the attempt: the commits
    //! and the confirming FlowExecute carry their own deadlines. A failed attempt is retried, so
    //! this decides how long the publication may hang on those two before the retry.
    TDuration PublishRequestTimeout;

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
