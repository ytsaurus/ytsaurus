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
    THashMap<NSequoiaClient::EGroundUpdateQueue, TDynamicTableUpdateQueueConfigPtr> Queues;

    const TDynamicTableUpdateQueueConfigPtr& GetQueueConfig(NSequoiaClient::EGroundUpdateQueue queue) const;

    REGISTER_YSON_STRUCT(TDynamicGroundUpdateQueueManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicGroundUpdateQueueManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCypressProxyTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration CypressProxyOrchidTimeout;

    REGISTER_YSON_STRUCT(TDynamicCypressProxyTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCypressProxyTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSequoiaManagerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    bool EnableCypressTransactionsInSequoia;
    bool EnableGroundUpdateQueues;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
