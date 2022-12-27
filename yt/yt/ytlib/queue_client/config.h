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

class TQueueAgentDynamicStateConfig
    : public NYTree::TYsonStruct
{
public:
    //! The path to the directory containing queue agent state.
    //! This path is local to the queue agent's home cluster.
    NYPath::TYPath Root;

    //! Path to the dynamic table containing queue consumer registrations.
    //! This table is shared by all queue agent installations and should be parametrized by the correct cluster.
    //! If no cluster is specified, the table will be assumed to be located on the queue agent's local cluster.
    NYPath::TRichYPath ConsumerRegistrationTablePath;

    REGISTER_YSON_STRUCT(TQueueAgentDynamicStateConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentDynamicStateConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cluster-parametrized path to the dynamic table containing queue agents' queue consumer registration state.
    //! If no cluster is specified, the connection's local cluster is assumed.
    NYPath::TRichYPath TablePath;

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
