#pragma once

#include "public.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/cell_master_client/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/query_client/config.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    bool EnableMasterCacheDiscovery;
    TDuration MasterCacheDiscoveryPeriod;
    TDuration MasterCacheDiscoveryPeriodSplay;

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TClockServersConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to clock servers.
    TDuration RpcTimeout;

    TClockServersConfig();
};

DEFINE_REFCOUNTED_TYPE(TClockServersConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
    , public NChunkClient::TChunkTeleporterConfig
    , public NCellMasterClient::TCellDirectoryConfig
{
public:
    std::optional<TString> ClusterName;

    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    TClockServersConfigPtr ClockServers;

    NCellMasterClient::TCellDirectorySynchronizerConfigPtr MasterCellDirectorySynchronizer;

    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    NChunkClient::TClientChunkMetaCacheConfigPtr ChunkMetaCache;
    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;
    NChunkClient::TMediumDirectorySynchronizerConfigPtr MediumDirectorySynchronizer;
    NNodeTrackerClient::TNodeDirectorySynchronizerConfigPtr NodeDirectorySynchronizer;

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

    //! May be disabled for snapshot validation purposes.
    bool EnableNetworking;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::TRemoteTimestampProviderConfigPtr CreateRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

