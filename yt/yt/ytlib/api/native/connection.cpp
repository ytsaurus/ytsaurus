#include "config.h"
#include "connection.h"
#include "client.h"
#include "tablet_sync_replica_cache.h"
#include "transaction_participant.h"
#include "transaction.h"
#include "private.h"

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>
#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cell_tracker.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/hive_service_proxy.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>
#include <yt/yt/ytlib/query_client/evaluator.h>
#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/scheduler_channel.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/ytlib/tablet_client/native_table_mount_cache.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/client/transaction_client/noop_timestamp_provider.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NRpc;
using namespace NHiveClient;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NJobProberClient;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NProfiling;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString MakeConnectionClusterId(const TConnectionConfigPtr& config)
{
    if (config->ClusterName) {
        return Format("Native(Name=%v)", *config->ClusterName);
    } else {
        return Format("Native(PrimaryCellTag=%v)",
            CellTagFromId(config->PrimaryMaster->CellId));
    }
}

} // namespace

class TConnection
    : public IConnection
{
public:
    TConnection(
        TConnectionConfigPtr config,
        const TConnectionOptions& options)
        : Config_(std::move(config))
        , Options_(options)
        , LoggingTag_(Format("PrimaryCellTag: %v, ConnectionId: %v, ConnectionName: %v",
            CellTagFromId(Config_->PrimaryMaster->CellId),
            TGuid::Create(),
            Config_->ConnectionName))
        , ClusterId_(MakeConnectionClusterId(Config_))
        , ChannelFactory_(CreateCachingChannelFactory(
            NRpc::NBus::CreateBusChannelFactory(Config_->BusClient),
            Config_->IdleChannelTtl))
        , StickyGroupSizeCache_(Config_->EnableDynamicCacheStickyGroupSize ? New<TStickyGroupSizeCache>() : nullptr)
        , TabletSyncReplicaCache_(New<TTabletSyncReplicaCache>())
        , Logger(ApiLogger.WithRawTag(LoggingTag_))
        , Profiler_(TProfiler("/connection").WithTag("connection_name", Config_->ConnectionName))
    { }

    void Initialize()
    {
        if (Options_.ConnectionInvoker) {
            ConnectionInvoker_ = Options_.ConnectionInvoker;
        } else {
            ConnectionThreadPool_ = New<TThreadPool>(Config_->ThreadPoolSize, "Connection");
            ConnectionInvoker_ = ConnectionThreadPool_->GetInvoker();
        }

        MasterCellDirectory_ = New<NCellMasterClient::TCellDirectory>(
            Config_,
            Options_,
            ChannelFactory_,
            Logger);
        MasterCellDirectorySynchronizer_ = New<NCellMasterClient::TCellDirectorySynchronizer>(
            Config_->MasterCellDirectorySynchronizer,
            MasterCellDirectory_);
        if (Config_->EnableNetworking) {
            MasterCellDirectorySynchronizer_->Start();
        }

        InitializeTimestampProvider();

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            ChannelFactory_,
            GetMasterChannelOrThrow(EMasterChannelKind::Leader),
            GetNetworks());

        PermissionCache_ = New<TPermissionCache>(
            Config_->PermissionCache,
            this);

        JobShellDescriptorCache_ = New<TJobShellDescriptorCache>(
            Config_->JobShellDescriptorCache,
            SchedulerChannel_);

        ClusterDirectory_ = New<TClusterDirectory>(NApi::TConnectionOptions{GetInvoker()});
        ClusterDirectorySynchronizer_ = New<TClusterDirectorySynchronizer>(
            Config_->ClusterDirectorySynchronizer,
            this,
            ClusterDirectory_);

        MediumDirectory_ = New<TMediumDirectory>();
        MediumDirectorySynchronizer_ = New<TMediumDirectorySynchronizer>(
            Config_->MediumDirectorySynchronizer,
            this,
            MediumDirectory_);

        CellDirectory_ = New<NHiveClient::TCellDirectory>(
            Config_->CellDirectory,
            ChannelFactory_,
            GetNetworks(),
            Logger);
        CellDirectory_->ReconfigureCell(Config_->PrimaryMaster);
        for (const auto& cellConfig : Config_->SecondaryMasters) {
            CellDirectory_->ReconfigureCell(cellConfig);
        }

        // For signlecell clusters we have to sync with primary cell.
        // For multicell clusters we sync with random secondary cell to reduce
        // load on primary cell.
        TCellIdList cellIdsToSyncCells;
        if (Config_->CellDirectorySynchronizer->SyncCellsWithSecondaryMasters) {
            cellIdsToSyncCells = MasterCellDirectory_->GetSecondaryMasterCellIds();
        }
        if (cellIdsToSyncCells.empty()) {
            cellIdsToSyncCells.push_back(GetPrimaryMasterCellId());
        }

        CellDirectorySynchronizer_ = CreateCellDirectorySynchronizer(
            Config_->CellDirectorySynchronizer,
            CellDirectory_,
            std::move(cellIdsToSyncCells),
            Logger);

        if (Options_.BlockCache) {
            BlockCache_ = Options_.BlockCache;
        } else {
            BlockCache_ = CreateClientBlockCache(
                Config_->BlockCache,
                EBlockType::CompressedData | EBlockType::UncompressedData,
                GetNullMemoryUsageTracker(),
                Profiler_.WithPrefix("/block_cache"));
        }

        if (Options_.ChunkMetaCache) {
            ChunkMetaCache_ = Options_.ChunkMetaCache;
        } else if (Config_->ChunkMetaCache) {
            ChunkMetaCache_ = CreateClientChunkMetaCache(
                Config_->ChunkMetaCache,
                Profiler_.WithPrefix("/chunk_meta_cache"));
        } else {
            ChunkMetaCache_ = nullptr;
        }

        TableMountCache_ = CreateNativeTableMountCache(
            Config_->TableMountCache,
            this,
            CellDirectory_,
            Logger);

        QueryEvaluator_ = CreateEvaluator(Config_->QueryEvaluator);
        ColumnEvaluatorCache_ = CreateColumnEvaluatorCache(Config_->ColumnEvaluatorCache);

        NodeDirectory_ = New<TNodeDirectory>();
        NodeDirectorySynchronizer_ = New<TNodeDirectorySynchronizer>(
            Config_->NodeDirectorySynchronizer,
            MakeStrong(this),
            NodeDirectory_);
    }

    // IConnection implementation.

    TCellTag GetCellTag() override
    {
        return GetPrimaryMasterCellTag();
    }

    const TString& GetLoggingTag() override
    {
        return LoggingTag_;
    }

    const TString& GetClusterId() override
    {
        return ClusterId_;
    }

    const ITableMountCachePtr& GetTableMountCache() override
    {
        return TableMountCache_;
    }

    const ITimestampProviderPtr& GetTimestampProvider() override
    {
        return TimestampProvider_;
    }

    const TJobShellDescriptorCachePtr& GetJobShellDescriptorCache() override
    {
        return JobShellDescriptorCache_;
    }

    const TPermissionCachePtr& GetPermissionCache() override
    {
        return PermissionCache_;
    }

    const TStickyGroupSizeCachePtr& GetStickyGroupSizeCache() override
    {
        return StickyGroupSizeCache_;
    }

    const TTabletSyncReplicaCachePtr& GetTabletSyncReplicaCache() override
    {
        return TabletSyncReplicaCache_;
    }

    IInvokerPtr GetInvoker() override
    {
        return ConnectionInvoker_;
    }

    NApi::IClientPtr CreateClient(const TClientOptions& options) override
    {
        return NNative::CreateClient(this, options);
    }

    void ClearMetadataCaches() override
    {
        TableMountCache_->Clear();
        PermissionCache_->Clear();
    }

    // NNative::IConnection implementation.

    const TConnectionConfigPtr& GetConfig() override
    {
        return Config_;
    }

    const TNetworkPreferenceList& GetNetworks() const override
    {
        return Config_->Networks ? *Config_->Networks : DefaultNetworkPreferences;
    }

    TCellId GetPrimaryMasterCellId() const override
    {
        return MasterCellDirectory_->GetPrimaryMasterCellId();
    }

    TCellTag GetPrimaryMasterCellTag() const override
    {
        return MasterCellDirectory_->GetPrimaryMasterCellTag();
    }

    const TCellTagList& GetSecondaryMasterCellTags() const override
    {
        return MasterCellDirectory_->GetSecondaryMasterCellTags();
    }

    TCellId GetMasterCellId(TCellTag cellTag) const override
    {
        return ReplaceCellTagInId(GetPrimaryMasterCellId(), cellTag);
    }

    IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        TCellTag cellTag = PrimaryMasterCellTag) override
    {
        return MasterCellDirectory_->GetMasterChannelOrThrow(kind, cellTag);
    }

    IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        TCellId cellId) override
    {
        return MasterCellDirectory_->GetMasterChannelOrThrow(kind, cellId);
    }

    const IChannelPtr& GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    const IChannelFactoryPtr& GetChannelFactory() override
    {
        return ChannelFactory_;
    }

    const IBlockCachePtr& GetBlockCache() override
    {
        return BlockCache_;
    }

    const IClientChunkMetaCachePtr& GetChunkMetaCache() override
    {
        return ChunkMetaCache_;
    }

    const IEvaluatorPtr& GetQueryEvaluator() override
    {
        return QueryEvaluator_;
    }

    const IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() override
    {
        return ColumnEvaluatorCache_;
    }

    const NCellMasterClient::TCellDirectoryPtr& GetMasterCellDirectory() override
    {
        return MasterCellDirectory_;
    }

    const NCellMasterClient::TCellDirectorySynchronizerPtr& GetMasterCellDirectorySynchronizer() override
    {
        return MasterCellDirectorySynchronizer_;
    }

    const NHiveClient::TCellDirectoryPtr& GetCellDirectory() override
    {
        return CellDirectory_;
    }

    const NHiveClient::ICellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() override
    {
        return CellDirectorySynchronizer_;
    }

    const TNodeDirectoryPtr& GetNodeDirectory() override
    {
        NodeDirectorySynchronizer_->Start();
        return NodeDirectory_;
    }

    const TNodeDirectorySynchronizerPtr& GetNodeDirectorySynchronizer() override
    {
        NodeDirectorySynchronizer_->Start();
        return NodeDirectorySynchronizer_;
    }

    const TCellTrackerPtr& GetDownedCellTracker() override
    {
        return DownedCellTracker_;
    }

    const NHiveClient::TClusterDirectoryPtr& GetClusterDirectory() override
    {
        return ClusterDirectory_;
    }

    const NHiveClient::TClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() override
    {
        return ClusterDirectorySynchronizer_;
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override
    {
        return MediumDirectory_;
    }

    const NChunkClient::TMediumDirectorySynchronizerPtr& GetMediumDirectorySynchronizer() override
    {
        return MediumDirectorySynchronizer_;
    }


    IClientPtr CreateNativeClient(const TClientOptions& options) override
    {
        return NNative::CreateClient(this, options);
    }

    NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        TCellId cellId,
        const TTransactionParticipantOptions& options) override
    {
        // For tablet writes, manual sync is not needed since Table Mount Cache
        // is responsible for populating cell directory. Transaction participants,
        // on the other hand, have no other way to keep cell directory up-to-date.
        CellDirectorySynchronizer_->Start();
        return NNative::CreateTransactionParticipant(
            CellDirectory_,
            CellDirectorySynchronizer_,
            TimestampProvider_,
            this,
            cellId,
            options);
    }

    IYPathServicePtr GetOrchidService() override
    {
        auto producer = BIND(&TConnection::BuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    void Terminate() override
    {
        Terminated_ = true;

        ClusterDirectory_->Clear();
        ClusterDirectorySynchronizer_->Stop();

        CellDirectory_->Clear();
        CellDirectorySynchronizer_->Stop();

        MediumDirectory_->Clear();
        MediumDirectorySynchronizer_->Stop();

        NodeDirectorySynchronizer_->Stop();
    }

    bool IsTerminated() override
    {
        return Terminated_;
    }

    TFuture<void> SyncHiveCellWithOthers(
        const std::vector<TCellId>& srcCellIds,
        TCellId dstCellId) override
    {
        YT_LOG_DEBUG("Started synchronizing Hive cell with others (SrcCellIds: %v, DstCellId: %v)",
            srcCellIds,
            dstCellId);

        auto channel = CellDirectory_->GetChannelOrThrow(dstCellId);
        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.SyncWithOthers();
        req->SetTimeout(Config_->HiveSyncRpcTimeout);
        ToProto(req->mutable_src_cell_ids(), srcCellIds);

        return req->Invoke()
            .Apply(BIND([=] (const THiveServiceProxy::TErrorOrRspSyncWithOthersPtr& rspOrError) {
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    rspOrError,
                    "Error synchronizing Hive cell %v with %v",
                    dstCellId,
                    srcCellIds);
                YT_LOG_DEBUG("Finished synchronizing Hive cell with others (SrcCellIds: %v, DstCellId: %v)",
                    srcCellIds,
                    dstCellId);
            }));
    }

private:
    const TConnectionConfigPtr Config_;
    const TConnectionOptions Options_;

    const TString LoggingTag_;
    const TString ClusterId_;

    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TStickyGroupSizeCachePtr StickyGroupSizeCache_;

    const TTabletSyncReplicaCachePtr TabletSyncReplicaCache_;

    const NLogging::TLogger Logger;
    const TProfiler Profiler_;

    // NB: There're also CellDirectory_ and CellDirectorySynchronizer_, which are completely different from these.
    NCellMasterClient::TCellDirectoryPtr MasterCellDirectory_;
    NCellMasterClient::TCellDirectorySynchronizerPtr MasterCellDirectorySynchronizer_;

    IChannelPtr SchedulerChannel_;
    IBlockCachePtr BlockCache_;
    IClientChunkMetaCachePtr ChunkMetaCache_;
    ITableMountCachePtr TableMountCache_;
    IChannelPtr TimestampProviderChannel_;
    ITimestampProviderPtr TimestampProvider_;
    TJobShellDescriptorCachePtr JobShellDescriptorCache_;
    TPermissionCachePtr PermissionCache_;
    IEvaluatorPtr QueryEvaluator_;
    IColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    TCellDirectoryPtr CellDirectory_;
    ICellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    const TCellTrackerPtr DownedCellTracker_ = New<TCellTracker>();

    TClusterDirectoryPtr ClusterDirectory_;
    TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;

    TMediumDirectoryPtr MediumDirectory_;
    TMediumDirectorySynchronizerPtr MediumDirectorySynchronizer_;

    TNodeDirectoryPtr NodeDirectory_;
    TNodeDirectorySynchronizerPtr NodeDirectorySynchronizer_;

    TThreadPoolPtr ConnectionThreadPool_;
    IInvokerPtr ConnectionInvoker_;

    std::atomic<bool> Terminated_ = false;


    void BuildOrchid(IYsonConsumer* consumer)
    {
        bool hasMasterCache = static_cast<bool>(Config_->MasterCache);
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("master_cache")
                    .BeginMap()
                        .Item("enabled").Value(hasMasterCache)
                        .DoIf(hasMasterCache, [this] (auto fluent) {
                            auto masterCacheChannel = MasterCellDirectory_->GetMasterChannelOrThrow(
                                EMasterChannelKind::Cache,
                                MasterCellDirectory_->GetPrimaryMasterCellId());
                            fluent
                                .Item("channel_attributes").Value(masterCacheChannel->GetEndpointAttributes());
                        })
                    .EndMap()
                .Item("timestamp_provider")
                    .BeginMap()
                        .Item("channel_attributes").Value(TimestampProviderChannel_->GetEndpointAttributes())
                    .EndMap()
            .EndMap();
    }

    void InitializeTimestampProvider()
    {
        if (!Config_->EnableNetworking) {
            TimestampProvider_ = CreateNoopTimestampProvider();
            return;
        }

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            timestampProviderConfig = CreateRemoteTimestampProviderConfig(Config_->PrimaryMaster);
        }

        TimestampProviderChannel_ = timestampProviderConfig->EnableTimestampProviderDiscovery ?
            CreateNodeAddressesChannel(
                timestampProviderConfig->TimestampProviderDiscoveryPeriod,
                timestampProviderConfig->TimestampProviderDiscoveryPeriodSplay,
                MakeWeak(MasterCellDirectory_),
                ENodeRole::TimestampProvider,
                BIND(&CreateTimestampProviderChannelFromAddresses, timestampProviderConfig, ChannelFactory_)) :
            CreateTimestampProviderChannel(timestampProviderConfig, ChannelFactory_);
        TimestampProvider_ = CreateBatchingRemoteTimestampProvider(timestampProviderConfig, TimestampProviderChannel_);
    }
};

IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TConnectionOptions options)
{
    NTracing::TNullTraceContextGuard nullTraceContext;

    if (!config->PrimaryMaster) {
        THROW_ERROR_EXCEPTION("Missing \"primary_master\" parameter in connection configuration");
    }
    auto connection = New<TConnection>(std::move(config), std::move(options));
    connection->Initialize();
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

TStickyGroupSizeCache::TKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Key);
    for (const auto& part : Message) {
        HashCombine(result, GetChecksum(part));
    }
    return result;
}

bool TStickyGroupSizeCache::TKey::operator == (const TKey& other) const
{
    if (Key != other.Key || Message.Size() != other.Message.Size()) {
        return false;
    }
    for (int i = 0; i < static_cast<int>(Message.Size()); ++i) {
        if (!TRef::AreBitwiseEqual(Message[i], other.Message[i])) {
            return false;
        }
    }
    return true;
}

TStickyGroupSizeCache::TStickyGroupSizeCache(TDuration expirationTimeout)
    : AdvisedStickyGroupSize_(New<TSyncExpiringCache<TKey, std::optional<int>>>(
        BIND([] (const TKey& /*key*/) {
            return std::optional<int>{};
        }),
        expirationTimeout,
        GetSyncInvoker()))
{ }

void TStickyGroupSizeCache::UpdateAdvisedStickyGroupSize(const TKey& key, int stickyGroupSize)
{
    AdvisedStickyGroupSize_->Set(key, stickyGroupSize);
}

std::optional<int> TStickyGroupSizeCache::GetAdvisedStickyGroupSize(const TKey& key)
{
    auto result = AdvisedStickyGroupSize_->Find(key);
    return result.value_or(std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr FindRemoteConnection(
    const IConnectionPtr& connection,
    const TString& clusterName)
{
    auto remoteConnection = connection->GetClusterDirectory()->FindConnection(clusterName);
    if (!remoteConnection) {
        return nullptr;
    }

    return dynamic_cast<NNative::IConnection*>(remoteConnection.Get());
}

IConnectionPtr FindRemoteConnection(
    const IConnectionPtr& connection,
    TCellTag cellTag)
{
    if (cellTag == connection->GetPrimaryMasterCellTag()) {
        return connection;
    }

    const auto& secondaryCellTags = connection->GetSecondaryMasterCellTags();
    if (std::find(secondaryCellTags.begin(), secondaryCellTags.end(), cellTag) != secondaryCellTags.end()) {
        return connection;
    }

    auto remoteConnection = connection->GetClusterDirectory()->FindConnection(cellTag);
    if (!remoteConnection) {
        return nullptr;
    }

    return dynamic_cast<NNative::IConnection*>(remoteConnection.Get());
}

IConnectionPtr GetRemoteConnectionOrThrow(
    const IConnectionPtr& connection,
    TCellTag cellTag)
{
    auto remoteConnection = FindRemoteConnection(connection, cellTag);
    if (!remoteConnection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell tag %v", cellTag);
    }
    return remoteConnection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
