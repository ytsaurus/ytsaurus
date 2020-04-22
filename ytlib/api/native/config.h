#pragma once

#include "public.h"

#include <yt/client/api/config.h>

#include <yt/ytlib/cell_master_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/query_client/config.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/security_client/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

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

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
    , public NChunkClient::TChunkTeleporterConfig
    , public NCellMasterClient::TCellDirectoryConfig
{
public:
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;

    NTransactionClient::TRemoteTimestampProviderWithDiscoveryConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;

    NCellMasterClient::TCellDirectorySynchronizerConfigPtr MasterCellDirectorySynchronizer;

    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
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
    bool EnableLookupMultiread;

    NYPath::TYPath UdfRegistryPath;
    TAsyncExpiringCacheConfigPtr FunctionRegistryCache;
    TSlruCacheConfigPtr FunctionImplCache;

    std::optional<int> ThreadPoolSize;

    int MaxConcurrentRequests;

    NBus::TTcpBusConfigPtr BusClient;

    TDuration DefaultGetInSyncReplicasTimeout;
    TDuration DefaultGetTabletInfosTimeout;
    TDuration DefaultTrimTableTimeout;
    TDuration DefaultGetOperationTimeout;
    TDuration DefaultListJobsTimeout;
    TDuration DefaultGetJobTimeout;
    TDuration DefaultListOperationsTimeout;

    int CacheStickyGroupSizeOverride;

    TDuration IdleChannelTtl;

    ssize_t MaxRequestWindowSize;

    TDuration UploadTransactionTimeout;
    TDuration HiveSyncRpcTimeout;

    //! Is visible in profiling as tag `connection_name`.
    TString Name;

    TAsyncExpiringCacheConfigPtr JobNodeDescriptorCache;

    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;

    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    TDuration NestedInputTransactionTimeout;
    TDuration NestedInputTransactionPingPeriod;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::TRemoteTimestampProviderWithDiscoveryConfigPtr CreateRemoteTimestampProviderWithDiscoveryConfig(TMasterConnectionConfigPtr config);
NTransactionClient::TBatchingRemoteTimestampProviderConfigPtr CreateBatchingRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

