#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableUpdateQueueConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration FlushPeriod;
    int FlushBatchSize;
    bool PauseFlush;

    REGISTER_YSON_STRUCT(TDynamicTableUpdateQueueConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableUpdateQueueConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicGroundUpdateQueueManagerConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<NSequoiaClient::EGroundUpdateQueue, TDynamicTableUpdateQueueConfigPtr> Queues;

    const TDynamicTableUpdateQueueConfigPtr& GetQueueConfig(NSequoiaClient::EGroundUpdateQueue queue) const;

    REGISTER_YSON_STRUCT(TDynamicGroundUpdateQueueManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicGroundUpdateQueueManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicSequoiaManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    bool EnableCypressTransactionsInSequoia;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
