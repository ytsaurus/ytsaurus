#pragma once

#include "public.h"

#include <yt/client/api/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/query_client/config.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

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

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
    , public NChunkClient::TChunkTeleporterConfig
{
public:
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;
    TMasterConnectionConfigPtr PrimaryMaster;
    std::vector<TMasterConnectionConfigPtr> SecondaryMasters;
    TMasterConnectionConfigPtr MasterCache;
    bool EnableReadFromFollowers;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;

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

    bool UseTabletService;

    TDuration IdleChannelTtl;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

