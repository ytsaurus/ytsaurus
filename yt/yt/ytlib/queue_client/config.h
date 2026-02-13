#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EQueueConsumerRegistrationManagerCacheKind,
    (RegistrationLookup)
    (ListRegistrations)
    (ReplicaMappingLookup)
);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// XXX(apachee): In practice these structures are configs, but they are built from other
// configs, and are not converted to or from yson. Maybe that is not the most conventional
// way to do this, but I'm not sure what is the better alternative here.
// TODO(apachee): Move this to registration_manager_config.h (keep in NDetail namespace).

////////////////////////////////////////////////////////////////////////////////

struct TLookupSessionConfig
    : public virtual TRefCounted
{
    TString User;
    NYPath::TRichYPath Table;

    bool operator==(const TLookupSessionConfig&) const;
};

DEFINE_REFCOUNTED_TYPE(TLookupSessionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStateLookupCacheConfig
    : public virtual TRefCounted
{
    TAsyncExpiringCacheConfigPtr Cache;
    // TODO(apachee): Add batch lookup config.

    TStateLookupCacheConfig& operator=(const TStateLookupCacheConfigPtr&);

    bool operator==(const TStateLookupCacheConfig&) const;

    static TStateLookupCacheConfigPtr FromQueueConsumerRegistrationManagerCacheConfig(
        const TQueueConsumerRegistrationManagerCacheConfigPtr& config,
        EQueueConsumerRegistrationManagerCacheKind cacheKind);
};

DEFINE_REFCOUNTED_TYPE(TStateLookupCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCompoundStateLookupCacheConfig
    : public TLookupSessionConfig
    , public TStateLookupCacheConfig
{
    bool operator==(const TCompoundStateLookupCacheConfig&) const;

    //! \note Aborts if called for legacy implementation.
    static TCompoundStateLookupCacheConfigPtr FromQueueConsumerRegistrationManagerConfig(
        const TQueueConsumerRegistrationManagerConfigPtr& config,
        EQueueConsumerRegistrationManagerCacheKind cacheKind);
};

DEFINE_REFCOUNTED_TYPE(TCompoundStateLookupCacheConfig)

////////////////////////////////////////////////////////////////////////////////

}  // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentStageChannelConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Default timeout for every request.
    TDuration DefaultRequestTimeout;

    REGISTER_YSON_STRUCT(TQueueAgentStageChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentStageChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentDynamicStateConfig
    : public NYTree::TYsonStruct
{
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

DEFINE_ENUM(EQueueConsumerRegistrationManagerImplementation,
    ((Legacy)                     (0))
    ((AsyncExpiringCache)         (1))
);

////////////////////////////////////////////////////////////////////////////////

struct TQueueConsumerRegistrationManagerCacheConfig
    : public NYTree::TYsonStruct
{
    TAsyncExpiringCacheConfigPtr Base;
    TEnumIndexedArray<EQueueConsumerRegistrationManagerCacheKind, TAsyncExpiringCacheDynamicConfigPtr> Delta;

    // TODO(apachee): Add batch lookup config.

    REGISTER_YSON_STRUCT(TQueueConsumerRegistrationManagerCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManagerCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueConsumerRegistrationManagerConfig
    : public NYTree::TYsonStruct
{
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

    //! Path to the dynamic table containing mapping of replicas to their corresponding replicated tables.
    //! Parametrized by a list of clusters. If no clusters are specified, the connection's local cluster is used.
    //! If a list of clusters is specified, the registration manager will send read-requests to all of them
    //! and use any successful result.
    NYPath::TRichYPath ReplicaMappingReadPath;

    //! If true, the table will be polled for each registration check and orchid call.
    //! Off by default.
    bool BypassCaching;

    //! Period with which a dynamic version of this config is retrieved from the cluster directory of
    //! the connection with which the manager was created.
    TDuration ConfigurationRefreshPeriod;

    //! User under which requests are performed to read and write state tables.
    std::string User;

    //! If true, then symbolic links in queues' and consumers' paths will be resolved in registrations and unregistrations.
    bool ResolveSymlinks;
    //! If true, replicas found in the replicated table mapping cache will be resolved to their corresponding replicated table objects.
    bool ResolveReplicas;

    //! If true, then listing registrations without specifing both queue and consumer (i.e. listing all registrations) results in an error.
    //! By default, keeps the old behavior of allowing such requests.
    bool DisableListAllRegistrations;

    EQueueConsumerRegistrationManagerImplementation Implementation;

    //! Period with which the registration table is polled by the registration cache.
    //! \note If async expiring implementation is used, this field is ignored.
    TDuration CacheRefreshPeriod;

    //! Config for internally used async expiring caches.
    //! \note If legacy implementation is used, this field is ignored.
    TQueueConsumerRegistrationManagerCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TQueueConsumerRegistrationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentConnectionConfig
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TQueueAgentStageChannelConfigPtr> Stages;

    TQueueConsumerRegistrationManagerConfigPtr QueueConsumerRegistrationManager;

    REGISTER_YSON_STRUCT(TQueueAgentConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueProducerSystemMeta
    : public NYTree::TYsonStructLite
{
    std::optional<NRpc::TMutationId> MutationId;

    REGISTER_YSON_STRUCT_LITE(TQueueProducerSystemMeta);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
