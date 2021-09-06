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
#include "private.h"
#include "session_manager.h"
#include "skynet_http_handler.h"
#include "table_schema_cache.h"
#include "ytree_integration.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

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

class TBootstrap
    : public IBootstrap
    , public TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));
        const auto& dynamicConfig = GetDynamicConfigManager()->GetConfig();

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
        P2PBlockDistributor_ = New<TP2PBlockDistributor>(this);

        TableSchemaCache_ = New<TTableSchemaCache>(GetConfig()->DataNode->TableSchemaCache);

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

        GetJobController()->AddHeartbeatProcessor<TMasterJobHeartbeatProcessor>(EObjectType::MasterJob, this);
    }

    void Run() override
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

        SkynetHttpServer_->Start();

        AllyReplicaManager_->Start();
    }

    const TChunkStorePtr& GetChunkStore() const override
    {
        return ChunkStore_;
    }

    const IAllyReplicaManagerPtr& GetAllyReplicaManager() const override
    {
        return AllyReplicaManager_;
    }

    const IChunkBlockManagerPtr& GetChunkBlockManager() const override
    {
        return ChunkBlockManager_;
    }

    const TSessionManagerPtr& GetSessionManager() const override
    {
        return SessionManager_;
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const TMediumUpdaterPtr& GetMediumUpdater() const override
    {
        return MediumUpdater_;
    }

    const IThroughputThrottlerPtr& GetThrottler(EDataNodeThrottlerKind kind) const override
    {
        return Throttlers_[kind];
    }

    const IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const override
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

    const IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const override
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

    const IJournalDispatcherPtr& GetJournalDispatcher() const override
    {
        return JournalDispatcher_;
    }

    const IInvokerPtr& GetStorageLookupInvoker() const override
    {
        return StorageLookupThreadPool_->GetInvoker();
    }

    const IInvokerPtr& GetMasterJobInvoker() const override
    {
        return MasterJobThreadPool_->GetInvoker();
    }

    const TBlockPeerTablePtr& GetBlockPeerTable() const override
    {
        return BlockPeerTable_;
    }

    const TP2PBlockDistributorPtr& GetP2PBlockDistributor() const override
    {
        return P2PBlockDistributor_;
    }

    const TTableSchemaCachePtr& GetTableSchemaCache() const override
    {
        return TableSchemaCache_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TChunkStorePtr ChunkStore_;
    IAllyReplicaManagerPtr AllyReplicaManager_;

    IChunkBlockManagerPtr ChunkBlockManager_;

    TSessionManagerPtr SessionManager_;

    IMasterConnectorPtr MasterConnector_;

    TMediumUpdaterPtr MediumUpdater_;

    TEnumIndexedVector<EDataNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedVector<EDataNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    IJournalDispatcherPtr JournalDispatcher_;

    TThreadPoolPtr StorageLookupThreadPool_;
    TThreadPoolPtr MasterJobThreadPool_;

    TBlockPeerTablePtr BlockPeerTable_;
    TP2PBlockDistributorPtr P2PBlockDistributor_;

    TTableSchemaCachePtr TableSchemaCache_;

    NHttp::IServerPtr SkynetHttpServer_;

    void OnDynamicConfigChanged(
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

        TableSchemaCache_->Reconfigure(newConfig->DataNode->TableSchemaCache);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
