#include "bootstrap.h"

#include "ally_replica_manager.h"
#include "blob_reader_cache.h"
#include "block_peer_table.h"
#include "chunk_block_manager.h"
#include "chunk_meta_manager.h"
#include "chunk_registry.h"
#include "chunk_store.h"
#include "data_node_service.h"
#include "job.h"
#include "job_heartbeat_processor.h"
#include "journal_dispatcher.h"
#include "master_connector.h"
#include "medium_updater.h"
#include "p2p_block_distributor.h"
#include "p2p.h"
#include "private.h"
#include "session_manager.h"
#include "skynet_http_handler.h"
#include "table_schema_cache.h"
#include "ytree_integration.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/lib/io/config.h>

#include <yt/yt/ytlib/tablet_client/row_comparer_generator.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const THashSet<EDataNodeThrottlerKind> DataNodeNetworkThrottlers = {
    EDataNodeThrottlerKind::TotalIn,
    EDataNodeThrottlerKind::TotalOut,
    EDataNodeThrottlerKind::ReplicationIn,
    EDataNodeThrottlerKind::ReplicationOut,
    EDataNodeThrottlerKind::RepairIn,
    EDataNodeThrottlerKind::RepairOut,
    EDataNodeThrottlerKind::MergeIn,
    EDataNodeThrottlerKind::MergeOut,
    EDataNodeThrottlerKind::AutotomyIn,
    EDataNodeThrottlerKind::AutotomyOut,
    EDataNodeThrottlerKind::ArtifactCacheIn,
    EDataNodeThrottlerKind::ArtifactCacheOut,
    EDataNodeThrottlerKind::ReadRpsOut,
    EDataNodeThrottlerKind::JobIn,
    EDataNodeThrottlerKind::JobOut,
    EDataNodeThrottlerKind::P2POut
};

// COMPAT(gritukan): Throttlers that were moved out of Data Node during node split.
static const THashSet<EDataNodeThrottlerKind> DataNodeCompatThrottlers = {
    // Cluster Node throttlers.
    EDataNodeThrottlerKind::TotalIn,
    EDataNodeThrottlerKind::TotalOut,
    EDataNodeThrottlerKind::ReadRpsOut,
    // Exec Node throttlers.
    EDataNodeThrottlerKind::ArtifactCacheIn,
    EDataNodeThrottlerKind::JobIn,
    EDataNodeThrottlerKind::JobOut,
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(NClusterNode::IBootstrap* bootstrap)
    : TBootstrapForward(bootstrap)
    , ClusterNodeBootstrap_(bootstrap)
{ }

void TBootstrap::Initialize()
{
    GetDynamicConfigManager()
        ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));
    const auto& dynamicConfig = GetDynamicConfigManager()->GetConfig();

    IOTracker_ = NIO::CreateIOTracker(dynamicConfig->DataNode->IOTracker);

    ChunkStore_ = New<TChunkStore>(GetConfig()->DataNode, this);

    ChunkBlockManager_ = CreateChunkBlockManager(this);

    SessionManager_ = New<TSessionManager>(GetConfig()->DataNode, this);

    MasterConnector_ = CreateMasterConnector(this);

    MediumUpdater_ = New<TMediumUpdater>(this);

    JournalDispatcher_ = CreateJournalDispatcher(this);

    ChunkStore_->Initialize();

    SessionManager_->Initialize();

    for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
        if (DataNodeCompatThrottlers.contains(kind)) {
            continue;
        }

        const auto& initialThrottlerConfig = GetConfig()->DataNode->Throttlers[kind];
        auto throttlerConfig = DataNodeNetworkThrottlers.contains(kind)
            ? ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(initialThrottlerConfig)
            : initialThrottlerConfig;
        RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
            std::move(throttlerConfig),
            ToString(kind),
            DataNodeLogger,
            DataNodeProfiler.WithPrefix("/throttlers"));
    }
    static const THashSet<EDataNodeThrottlerKind> InCombinedDataNodeThrottlerKinds = {
        EDataNodeThrottlerKind::ReplicationIn,
        EDataNodeThrottlerKind::RepairIn,
        EDataNodeThrottlerKind::MergeIn,
        EDataNodeThrottlerKind::AutotomyIn,
        EDataNodeThrottlerKind::ArtifactCacheIn,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn,
        EDataNodeThrottlerKind::TabletLoggingIn,
        EDataNodeThrottlerKind::TabletSnapshotIn,
        EDataNodeThrottlerKind::TabletStoreFlushIn,
        EDataNodeThrottlerKind::JobIn,
    };
    static const THashSet<EDataNodeThrottlerKind> OutCombinedDataNodeThrottlerKinds = {
        EDataNodeThrottlerKind::ReplicationOut,
        EDataNodeThrottlerKind::RepairOut,
        EDataNodeThrottlerKind::MergeOut,
        EDataNodeThrottlerKind::AutotomyOut,
        EDataNodeThrottlerKind::ArtifactCacheOut,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut,
        EDataNodeThrottlerKind::SkynetOut,
        EDataNodeThrottlerKind::TabletPreloadOut,
        EDataNodeThrottlerKind::TabletRecoveryOut,
        EDataNodeThrottlerKind::TabletReplicationOut,
        EDataNodeThrottlerKind::JobOut,
    };
    for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
        if (DataNodeCompatThrottlers.contains(kind)) {
            continue;
        }

        auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
        if (InCombinedDataNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({GetTotalInThrottler(), throttler});
        }
        if (OutCombinedDataNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({GetTotalOutThrottler(), throttler});
        }
        Throttlers_[kind] = throttler;
    }

    // Should be created after throttlers.
    AllyReplicaManager_ = CreateAllyReplicaManager(this);

    StorageLookupThreadPool_ = New<TThreadPool>(
        GetConfig()->DataNode->StorageLookupThreadCount,
        "StorageLookup");
    MasterJobThreadPool_ = New<TThreadPool>(
        dynamicConfig->DataNode->MasterJobThreadCount,
        "MasterJob");

    BlockPeerTable_ = New<TBlockPeerTable>(this);

    P2PActionQueue_ = New<TActionQueue>("P2P");
    P2PBlockDistributor_ = New<TP2PBlockDistributor>(this);
    P2PBlockCache_ = New<TP2PBlockCache>(
        GetConfig()->DataNode->P2P,
        P2PActionQueue_->GetInvoker(),
        GetMemoryUsageTracker()->WithCategory(EMemoryCategory::P2P));
    P2PSnooper_ = New<TP2PSnooper>(GetConfig()->DataNode->P2P);
    P2PDistributor_ = New<TP2PDistributor>(
        GetConfig()->DataNode->P2P,
        P2PActionQueue_->GetInvoker(),
        this);

    DiskTracker_ = New<NProfiling::TDiskTracker>();
    DataNodeProfiler.AddProducer("", DiskTracker_);

    TableSchemaCache_ = New<TTableSchemaCache>(GetConfig()->DataNode->TableSchemaCache);

    RowComparerProvider_ = NTabletClient::CreateRowComparerProvider(GetConfig()->TabletNode->ColumnEvaluatorCache->CGCache);

    GetRpcServer()->RegisterService(CreateDataNodeService(GetConfig()->DataNode, this));

    auto createMasterJob = BIND([this] (
        NJobAgent::TJobId jobId,
        NJobAgent::TOperationId /*operationId*/,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        NJobTrackerClient::NProto::TJobSpec&& jobSpec) -> NJobAgent::IJobPtr
    {
        return CreateMasterJob(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            GetConfig()->DataNode,
            this);
    });
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::RemoveChunk, createMasterJob);
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::ReplicateChunk, createMasterJob);
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::RepairChunk, createMasterJob);
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::SealChunk, createMasterJob);
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::MergeChunks, createMasterJob);
    GetJobController()->RegisterMasterJobFactory(NJobAgent::EJobType::AutotomizeChunk, createMasterJob);

    GetJobController()->AddHeartbeatProcessor<TMasterJobHeartbeatProcessor>(EObjectType::MasterJob, this);
}

void TBootstrap::Run()
{
    SkynetHttpServer_ = NHttp::CreateServer(GetConfig()->CreateSkynetHttpServerConfig());
    SkynetHttpServer_->AddHandler(
        "/read_skynet_part",
        MakeSkynetHttpHandler(this));

    SetNodeByYPath(
        GetOrchidRoot(),
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(ChunkStore_, GetAllyReplicaManager())
            ->Via(GetControlInvoker())));

    SetNodeByYPath(
        GetOrchidRoot(),
        "/ally_replica_manager",
        CreateVirtualNode(AllyReplicaManager_->GetOrchidService()));

    MasterConnector_->Initialize();

    MediumUpdater_->Start();

    P2PBlockDistributor_->Start();
    P2PDistributor_->Start();

    SkynetHttpServer_->Start();

    AllyReplicaManager_->Start();
}

const TChunkStorePtr& TBootstrap::GetChunkStore() const
{
    return ChunkStore_;
}

const IAllyReplicaManagerPtr& TBootstrap::GetAllyReplicaManager() const
{
    return AllyReplicaManager_;
}

const IChunkBlockManagerPtr& TBootstrap::GetChunkBlockManager() const
{
    return ChunkBlockManager_;
}

const TSessionManagerPtr& TBootstrap::GetSessionManager() const
{
    return SessionManager_;
}

const IMasterConnectorPtr& TBootstrap::GetMasterConnector() const
{
    return MasterConnector_;
}

const TMediumUpdaterPtr& TBootstrap::GetMediumUpdater() const
{
    return MediumUpdater_;
}

const IThroughputThrottlerPtr& TBootstrap::GetThrottler(EDataNodeThrottlerKind kind) const
{
    return Throttlers_[kind];
}

const IThroughputThrottlerPtr& TBootstrap::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
    static const THashMap<EWorkloadCategory, EDataNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemRepair,                EDataNodeThrottlerKind::RepairIn},
        {EWorkloadCategory::SystemReplication,           EDataNodeThrottlerKind::ReplicationIn},
        {EWorkloadCategory::SystemArtifactCacheDownload, EDataNodeThrottlerKind::ArtifactCacheIn},
        {EWorkloadCategory::SystemTabletCompaction,      EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletPartitioning,    EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletLogging,         EDataNodeThrottlerKind::TabletLoggingIn},
        {EWorkloadCategory::SystemTabletSnapshot,        EDataNodeThrottlerKind::TabletSnapshotIn},
        {EWorkloadCategory::SystemTabletStoreFlush,      EDataNodeThrottlerKind::TabletStoreFlushIn}
    };
    auto it = WorkloadCategoryToThrottlerKind.find(descriptor.Category);
    return it == WorkloadCategoryToThrottlerKind.end()
        ? GetTotalInThrottler()
        : Throttlers_[it->second];
}

const IThroughputThrottlerPtr& TBootstrap::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    static const THashMap<EWorkloadCategory, EDataNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemRepair,                EDataNodeThrottlerKind::RepairOut},
        {EWorkloadCategory::SystemReplication,           EDataNodeThrottlerKind::ReplicationOut},
        {EWorkloadCategory::SystemArtifactCacheDownload, EDataNodeThrottlerKind::ArtifactCacheOut},
        {EWorkloadCategory::SystemTabletCompaction,      EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletPartitioning,    EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletPreload,         EDataNodeThrottlerKind::TabletPreloadOut},
        {EWorkloadCategory::SystemTabletRecovery,        EDataNodeThrottlerKind::TabletRecoveryOut},
        {EWorkloadCategory::SystemTabletReplication,     EDataNodeThrottlerKind::TabletReplicationOut}
    };
    auto it = WorkloadCategoryToThrottlerKind.find(descriptor.Category);
    return it == WorkloadCategoryToThrottlerKind.end()
        ? GetTotalOutThrottler()
        : Throttlers_[it->second];
}

const IJournalDispatcherPtr& TBootstrap::GetJournalDispatcher() const
{
    return JournalDispatcher_;
}

const IInvokerPtr& TBootstrap::GetStorageLookupInvoker() const
{
    return StorageLookupThreadPool_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetMasterJobInvoker() const
{
    return MasterJobThreadPool_->GetInvoker();
}

const TBlockPeerTablePtr& TBootstrap::GetBlockPeerTable() const
{
    return BlockPeerTable_;
}

const TP2PBlockDistributorPtr& TBootstrap::GetP2PBlockDistributor() const
{
    return P2PBlockDistributor_;
}

const TP2PBlockCachePtr& TBootstrap::GetP2PBlockCache() const
{
    return P2PBlockCache_;
}

const TP2PSnooperPtr& TBootstrap::GetP2PSnooper() const
{
    return P2PSnooper_;
}

const TTableSchemaCachePtr& TBootstrap::GetTableSchemaCache() const
{
    return TableSchemaCache_;
}

const NTabletClient::IRowComparerProviderPtr& TBootstrap::GetRowComparerProvider() const
{
    return RowComparerProvider_;
}

const NIO::IIOTrackerPtr& TBootstrap::GetIOTracker() const
{
    return IOTracker_;
}

void TBootstrap::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
    const TClusterNodeDynamicConfigPtr& newConfig)
{
    for (auto kind : TEnumTraits<NDataNode::EDataNodeThrottlerKind>::GetDomainValues()) {
        if (DataNodeCompatThrottlers.contains(kind)) {
            continue;
        }

        const auto& initialThrottlerConfig = newConfig->DataNode->Throttlers[kind]
            ? newConfig->DataNode->Throttlers[kind]
            : GetConfig()->DataNode->Throttlers[kind];
        auto throttlerConfig = DataNodeNetworkThrottlers.contains(kind)
            ? ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(initialThrottlerConfig)
            : initialThrottlerConfig;
        RawThrottlers_[kind]->Reconfigure(std::move(throttlerConfig));
    }

    StorageLookupThreadPool_->Configure(
        newConfig->DataNode->StorageLookupThreadCount.value_or(GetConfig()->DataNode->StorageLookupThreadCount));
    MasterJobThreadPool_->Configure(newConfig->DataNode->MasterJobThreadCount);

    TableSchemaCache_->Configure(newConfig->DataNode->TableSchemaCache);

    IOTracker_->SetConfig(newConfig->DataNode->IOTracker);

    P2PBlockCache_->UpdateConfig(newConfig->DataNode->P2P);
    P2PSnooper_->UpdateConfig(newConfig->DataNode->P2P);
    P2PDistributor_->UpdateConfig(newConfig->DataNode->P2P);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
