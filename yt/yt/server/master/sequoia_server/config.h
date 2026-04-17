#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableUpdateQueueConfig
    : public NYTree::TYsonStruct
{
    TDuration FlushPeriod;
    int FlushBatchSize;
    bool PauseFlush;

    REGISTER_YSON_STRUCT(TDynamicTableUpdateQueueConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableUpdateQueueConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicGroundUpdateQueueManagerConfig
    : public NYTree::TYsonStruct
{
    static constexpr auto DefaultProfilingPeriod = TDuration::Seconds(5);

    THashMap<NSequoiaClient::EGroundUpdateQueue, TDynamicTableUpdateQueueConfigPtr> Queues;

    TDuration ProfilingPeriod;

    const TDynamicTableUpdateQueueConfigPtr& GetQueueConfig(NSequoiaClient::EGroundUpdateQueue queue) const;

    REGISTER_YSON_STRUCT(TDynamicGroundUpdateQueueManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicGroundUpdateQueueManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCypressProxyTrackerConfig
    : public NYTree::TYsonStruct
{
    TDuration CypressProxyOrchidTimeout;

    REGISTER_YSON_STRUCT(TDynamicCypressProxyTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCypressProxyTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSequoiaManagerTestingConfig
    : public NYTree::TYsonStruct
{
    std::optional<double> SequoiaTransactionStartFailureProbability;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSequoiaManagerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    bool EnableCypressTransactionsInSequoia;
    bool EnableGroundUpdateQueues;

    // COMPAT(kvk1920)
    bool EnableAsyncSequoiaTransactionStart;

    TDynamicSequoiaManagerTestingConfigPtr Testing;

    // There is a complicated tradeoff here:
    //   - shared write locks are waitable. Therefore, when 2 Sequoia
    //     transactions are locking the same row the second transaction can wait
    //     the first one before conflict checking. Read locks may lead to
    //     confilct even if timestamp intervals don't overlap.
    //   - every lookup waits for any incoming potential change. Therefore,
    //     shared write locks increase lookup latency while shared read locks
    //     don't.
    bool UseSharedWriteLocksForCypressTransactions;

    bool CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
