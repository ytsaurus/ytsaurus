#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

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
    TDuration LeaseCheckPeriod;
    double LeaseCheckPeriodJitter{};
    i64 MaxConcurrentRequests{};

    REGISTER_YSON_STRUCT(TLeaseManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLeaseManagerConfig);

////////////////////////////////////////////////////////////////////////////////

struct TElectionManagerConfig
    : public virtual NYTree::TYsonStruct
{
    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    TDuration LockAcquisitionPeriod;
    TDuration LeaderCacheUpdatePeriod;

    REGISTER_YSON_STRUCT(TElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TElectionManagerConfig);

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

    TElectionManagerConfigPtr ElectionManager;

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
