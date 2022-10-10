#pragma once

#include "public.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/cell_master_client/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/library/query/engine/config.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/chaos_client/config.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheConnectionConfig
    : public TMasterConnectionConfig
{
public:
    bool EnableMasterCacheDiscovery;
    TDuration MasterCacheDiscoveryPeriod;
    TDuration MasterCacheDiscoveryPeriodSplay;

    REGISTER_YSON_STRUCT(TMasterCacheConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TClockServersConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to clock servers.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TClockServersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClockServersConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
    , public NChunkClient::TChunkTeleporterConfig
    , public NCellMasterClient::TCellDirectoryConfig
{
public:
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NChaosClient::TChaosCellDirectorySynchronizerConfigPtr ChaosCellDirectorySynchronizer;
    TClockServersConfigPtr ClockServers;

    NCellMasterClient::TCellDirectorySynchronizerConfigPtr MasterCellDirectorySynchronizer;

    NQueueClient::TQueueAgentConnectionConfigPtr QueueAgent;
    NYqlClient::TYqlAgentConnectionConfigPtr YqlAgent;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NTransactionClient::TClockManagerConfigPtr ClockManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    NChunkClient::TClientChunkMetaCacheConfigPtr ChunkMetaCache;
    NChunkClient::TChunkReplicaCacheConfigPtr ChunkReplicaCache;
    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;
    NChunkClient::TMediumDirectorySynchronizerConfigPtr MediumDirectorySynchronizer;
    NNodeTrackerClient::TNodeDirectorySynchronizerConfigPtr NodeDirectorySynchronizer;
    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcher;

    NQueryClient::TExecutorConfigPtr QueryEvaluator;
    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;
    TDuration DefaultSelectRowsTimeout;
    NCompression::ECodec SelectRowsResponseCodec;
    i64 DefaultInputRowLimit;
    i64 DefaultOutputRowLimit;

    TDuration WriteRowsTimeout;
    NCompression::ECodec WriteRowsRequestCodec;
    int MaxRowsPerWriteRequest;
    i64 MaxDataWeightPerWriteRequest;
    int MaxRowsPerTransaction;

    TDuration DefaultLookupRowsTimeout;
    NCompression::ECodec LookupRowsRequestCodec;
    NCompression::ECodec LookupRowsResponseCodec;
    int MaxRowsPerLookupRequest;

    int DefaultGetTabletErrorsLimit;

    NYPath::TYPath UdfRegistryPath;
    TAsyncExpiringCacheConfigPtr FunctionRegistryCache;
    TSlruCacheConfigPtr FunctionImplCache;

    int ThreadPoolSize;

    NBus::TTcpBusConfigPtr BusClient;
    TDuration IdleChannelTtl;

    TDuration DefaultGetInSyncReplicasTimeout;
    TDuration DefaultGetTabletInfosTimeout;
    TDuration DefaultTrimTableTimeout;
    TDuration DefaultGetOperationTimeout;
    TDuration DefaultGetOperationRetryInterval;
    TDuration DefaultListJobsTimeout;
    TDuration DefaultGetJobTimeout;
    TDuration DefaultListOperationsTimeout;
    TDuration DefaultPullRowsTimeout;
    TDuration DefaultSyncAlienCellsTimeout;
    TDuration DefaultChaosNodeServiceTimeout;

    int CypressWriteYsonNestingLevelLimit;

    TDuration JobProberRpcTimeout;

    int DefaultCacheStickyGroupSize;
    bool EnableDynamicCacheStickyGroupSize;

    ssize_t MaxRequestWindowSize;

    TDuration UploadTransactionTimeout;
    TDuration HiveSyncRpcTimeout;

    //! Visible in profiling as tag `connection_name`.
    TString ConnectionName;

    TAsyncExpiringCacheConfigPtr JobShellDescriptorCache;

    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;

    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    TDuration NestedInputTransactionTimeout;
    TDuration NestedInputTransactionPingPeriod;

    TDuration ClusterLivenessCheckTimeout;

    NObjectClient::TReqExecuteBatchWithRetriesConfigPtr ChunkFetchRetries;

    TAsyncExpiringCacheConfigPtr SyncReplicaCache;

    NChaosClient::TReplicationCardChannelConfigPtr ChaosCellChannel;

    NRpc::TRetryingChannelConfigPtr HydraAdminChannel;

    NYTree::TYPath SequoiaPath;
    TDuration SequoiaTransactionTimeout;

    bool UseFollowersForWriteTargetsAllocation;

    //! TVM application id corresponding to the cluster.
    //! If set, should (and hopefully will) be used for authentication in all native protocol RPC requests.
    std::optional<NAuth::TTvmId> TvmId;

    NChaosClient::TReplicationCardResidencyCacheConfigPtr ReplicationCardResidencyCache;

    //! Replaces all master addresses with given master cache addresses.
    //! Used to proxy all job requests through cluster nodes.
    void OverrideMasterAddresses(const std::vector<TString>& addresses);

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionDynamicConfig
    : public NApi::TConnectionDynamicConfig
{
public:
    TAsyncExpiringCacheDynamicConfigPtr SyncReplicaCache;

    NTransactionClient::TDynamicClockManagerConfigPtr ClockManager;

    REGISTER_YSON_STRUCT(TConnectionDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::TRemoteTimestampProviderConfigPtr CreateRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

