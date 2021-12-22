#include "bootstrap.h"

#include "ally_replica_manager.h"
#include "blob_reader_cache.h"
#include "chunk_block_manager.h"
#include "chunk_meta_manager.h"
#include "chunk_registry.h"
#include "chunk_store.h"
#include "data_node_service.h"
#include "medium_directory_manager.h"
#include "job.h"
#include "job_heartbeat_processor.h"
#include "journal_dispatcher.h"
#include "master_connector.h"
#include "medium_updater.h"
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

        const auto& dynamicConfig = GetDynamicConfigManager()->GetConfig()->DataNode;

        IOTracker_ = NIO::CreateIOTracker(dynamicConfig->IOTracker);

        ChunkStore_ = New<TChunkStore>(GetConfig()->DataNode, this);

        ChunkBlockManager_ = CreateChunkBlockManager(this);

        SessionManager_ = New<TSessionManager>(GetConfig()->DataNode, this);

        MasterConnector_ = CreateMasterConnector(this);

        JournalDispatcher_ = CreateJournalDispatcher(this);

        MediumDirectoryManager_ = New<TMediumDirectoryManager>(
            this,
            DataNodeLogger);

        MediumUpdater_ = New<TMediumUpdater>(
            this,
            MediumDirectoryManager_,
            dynamicConfig->MediumUpdater->Period);

        ChunkStore_->Initialize();

        SessionManager_->Initialize();

        for (auto kind : {
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
        }) {
            Throttlers_[kind] = ClusterNodeBootstrap_->GetInThrottler(FormatEnum(kind));
        }

        for (auto kind : {
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
        }) {
            Throttlers_[kind] = ClusterNodeBootstrap_->GetOutThrottler(FormatEnum(kind));
        }

        // Should be created after throttlers.
        AllyReplicaManager_ = CreateAllyReplicaManager(this);

        StorageLookupThreadPool_ = New<TThreadPool>(
            GetConfig()->DataNode->StorageLookupThreadCount,
            "StorageLookup");
        MasterJobThreadPool_ = New<TThreadPool>(
            dynamicConfig->MasterJobThreadCount,
            "MasterJob");

        P2PActionQueue_ = New<TActionQueue>("P2P");
        P2PBlockCache_ = New<TP2PBlockCache>(
            GetConfig()->DataNode->P2P,
            P2PActionQueue_->GetInvoker(),
            GetMemoryUsageTracker()->WithCategory(EMemoryCategory::P2P));
        P2PSnooper_ = New<TP2PSnooper>(GetConfig()->DataNode->P2P);
        P2PDistributor_ = New<TP2PDistributor>(
            GetConfig()->DataNode->P2P,
            P2PActionQueue_->GetInvoker(),
            this);

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

        P2PDistributor_->Start();

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

    const TMediumDirectoryManagerPtr& GetMediumDirectoryManager() const override
    {
        return MediumDirectoryManager_;
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
            ? GetDefaultInThrottler()
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
            ? GetDefaultOutThrottler()
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

    const TP2PBlockCachePtr& GetP2PBlockCache() const override
    {
        return P2PBlockCache_;
    }

    const TP2PSnooperPtr& GetP2PSnooper() const override
    {
        return P2PSnooper_;
    }

    const TTableSchemaCachePtr& GetTableSchemaCache() const override
    {
        return TableSchemaCache_;
    }

    const NTabletClient::IRowComparerProviderPtr& GetRowComparerProvider() const override
    {
        return RowComparerProvider_;
    }

    const NIO::IIOTrackerPtr& GetIOTracker() const override
    {
        return IOTracker_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TChunkStorePtr ChunkStore_;
    IAllyReplicaManagerPtr AllyReplicaManager_;

    IChunkBlockManagerPtr ChunkBlockManager_;

    TSessionManagerPtr SessionManager_;

    IMasterConnectorPtr MasterConnector_;
    TMediumDirectoryManagerPtr MediumDirectoryManager_;
    TMediumUpdaterPtr MediumUpdater_;

    TEnumIndexedVector<EDataNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    IJournalDispatcherPtr JournalDispatcher_;

    TThreadPoolPtr StorageLookupThreadPool_;
    TThreadPoolPtr MasterJobThreadPool_;

    TActionQueuePtr P2PActionQueue_;
    TP2PBlockCachePtr P2PBlockCache_;
    TP2PSnooperPtr P2PSnooper_;
    TP2PDistributorPtr P2PDistributor_;

    TTableSchemaCachePtr TableSchemaCache_;

    NTabletClient::IRowComparerProviderPtr RowComparerProvider_;

    NHttp::IServerPtr SkynetHttpServer_;

    NIO::IIOTrackerPtr IOTracker_;

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        StorageLookupThreadPool_->Configure(
            newConfig->DataNode->StorageLookupThreadCount.value_or(GetConfig()->DataNode->StorageLookupThreadCount));
        MasterJobThreadPool_->Configure(newConfig->DataNode->MasterJobThreadCount);

        TableSchemaCache_->Configure(newConfig->DataNode->TableSchemaCache);

        IOTracker_->SetConfig(newConfig->DataNode->IOTracker);

        P2PBlockCache_->UpdateConfig(newConfig->DataNode->P2P);
        P2PSnooper_->UpdateConfig(newConfig->DataNode->P2P);
        P2PDistributor_->UpdateConfig(newConfig->DataNode->P2P);

        const auto& mediumUpdaterConfig = newConfig->DataNode->MediumUpdater;
        MediumUpdater_->SetPeriod(mediumUpdaterConfig->Period);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
