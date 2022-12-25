#pragma once

#include "public.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentStageChannelConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    REGISTER_YSON_STRUCT(TQueueAgentStageChannelConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentStageChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cluster-parametrized path to directory containing queue agents' registration state.
    //! If no cluster is specified, the connection's local cluster is assumed.
    NYPath::TRichYPath Root;

    //! Period with which the registration table is polled by the registration cache.
    TDuration CacheRefreshPeriod;

    //! User under which requests are performed to read and write registrations.
    TString User;

    REGISTER_YSON_STRUCT(TQueueConsumerRegistrationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentConnectionConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TQueueAgentStageChannelConfigPtr> Stages;

    TQueueConsumerRegistrationManagerConfigPtr QueueConsumerRegistrationManager;

    REGISTER_YSON_STRUCT(TQueueAgentConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
