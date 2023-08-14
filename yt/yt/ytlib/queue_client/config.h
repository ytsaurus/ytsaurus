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

    //! Path to the dynamic table containing a {[chaos] replicated table -> replicas} mapping.
    //! This table is shared by all queue agent installations and should be parametrized by the correct cluster.
    //! If no cluster is specified, the table will be assumed to be located on the queue agent's local cluster.
    NYPath::TRichYPath ReplicatedTableMappingTablePath;

    REGISTER_YSON_STRUCT(TQueueAgentDynamicStateConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentDynamicStateConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cluster-parametrized path to the dynamic table containing queue consumer registration state.
    //! If no cluster is specified, the connection's local cluster is assumed.
    //! This path is used for writing/removing registrations.
    NYPath::TRichYPath StateWritePath;
    //! Same as above, but parametrized with a non-empty list of clusters and used for reading registrations.
    //! If no clusters are specified, the connection's local cluster is used.
    //! If a list of clusters is specified, the registration manager will send read-requests to all of them
    //! and use any successful result.
    NYPath::TRichYPath StateReadPath;

    //! Path to the dynamic table containing mapping of replicated table objects to their corresponding replicas.
    //! Parametrized by a list of clusters. If no clusters are specified, the connection's local cluster is used.
    //! If a list of clusters is specified, the registration manager will send read-requests to all of them
    //! and use any successful result.
    NYPath::TRichYPath ReplicatedTableMappingReadPath;

    //! If true, the table will be polled for each registration check and orchid call.
    //! Off by default.
    bool BypassCaching;

    //! Period with which the registration table is polled by the registration cache.
    TDuration CacheRefreshPeriod;

    //! Period with which a dynamic version of this config is retrieved from the cluster directory of
    //! the connection with which the manager was created.
    TDuration ConfigurationRefreshPeriod;

    //! User under which requests are performed to read and write state tables.
    TString User;

    //! If true, then symbolic links in queues' and consumers' paths will be resolved in registrations and unregistrations.
    bool ResolveSymlinks;
    //! If true, replicas found in the replicated table mapping cache will be resolved to their corresponding replicated table objects.
    bool ResolveReplicas;

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
