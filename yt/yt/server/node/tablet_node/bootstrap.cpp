#include "bootstrap.h"

#include "backing_store_cleaner.h"
#include "hint_manager.h"
#include "hunk_chunk_sweeper.h"
#include "in_memory_manager.h"
#include "in_memory_service.h"
#include "lsm_interop.h"
#include "master_connector.h"
#include "partition_balancer.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "sorted_dynamic_comparer.h"
#include "store_compactor.h"
#include "store_flusher.h"
#include "store_rotator.h"
#include "store_trimmer.h"
#include "structured_logger.h"
#include "tablet_cell_service.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/node/data_node/bootstrap.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/query_agent/query_service.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NCellarNode;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NQueryClient;
using namespace NSecurityServer;
using namespace NTabletNode;
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

        MasterConnector_ = CreateMasterConnector(this);

        TabletSnapshotStore_ = CreateTabletSnapshotStore(GetConfig()->TabletNode, this);

        SlotManager_ = CreateSlotManager(this);

        InMemoryManager_ = CreateInMemoryManager(GetConfig()->TabletNode->InMemoryManager, this);
        GetRpcServer()->RegisterService(CreateInMemoryService(GetConfig()->TabletNode->InMemoryManager, this));

        StructuredLogger_ = CreateStructuredLogger(this);

        HintManager_ = CreateHintManager(this);

        QueryThreadPool_ = CreateTwoLevelFairShareThreadPool(
            GetConfig()->QueryAgent->QueryThreadPoolSize,
            "Query");
        TableReplicatorThreadPool_ = New<TThreadPool>(
            GetConfig()->TabletNode->TabletManager->ReplicatorThreadPoolSize,
            "Replicator");
        TabletLookupThreadPool_ = New<TThreadPool>(
            GetConfig()->QueryAgent->LookupThreadPoolSize,
            "TabletLookup");
        TabletFetchThreadPool_ = New<TThreadPool>(
            GetConfig()->QueryAgent->FetchThreadPoolSize,
            "TabletFetch");

        for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
            auto throttlerConfig = GetConfig()->TabletNode->Throttlers[kind];
            throttlerConfig = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(throttlerConfig);
            RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                std::move(throttlerConfig),
                ToString(kind),
                TabletNodeLogger,
                TabletNodeProfiler.WithPrefix("/throttlers"));
        }
        static const THashSet<ETabletNodeThrottlerKind> InCombinedTabletNodeThrottlerKinds = {
            ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn,
            ETabletNodeThrottlerKind::ReplicationIn,
            ETabletNodeThrottlerKind::StaticStorePreloadIn
        };
        static const THashSet<ETabletNodeThrottlerKind> OutCombinedTabletNodeThrottlerKinds = {
            ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut,
            ETabletNodeThrottlerKind::StoreFlushOut,
            ETabletNodeThrottlerKind::ReplicationOut,
            ETabletNodeThrottlerKind::DynamicStoreReadOut
        };
        static const THashSet<ETabletNodeThrottlerKind> InStealingTabletNodeThrottlerKinds = {
            ETabletNodeThrottlerKind::UserBackendIn,
        };
        for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
            auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
            if (InCombinedTabletNodeThrottlerKinds.contains(kind)) {
                throttler = CreateCombinedThrottler({GetTotalInThrottler(), throttler});
            }
            if (OutCombinedTabletNodeThrottlerKinds.contains(kind)) {
                throttler = CreateCombinedThrottler({GetTotalOutThrottler(), throttler});
            }
            if (InStealingTabletNodeThrottlerKinds.contains(kind)) {
                throttler = CreateStealingThrottler(throttler, GetTotalInThrottler());
            }
            Throttlers_[kind] = throttler;
        }

        ColumnEvaluatorCache_ = NQueryClient::CreateColumnEvaluatorCache(GetConfig()->TabletNode->ColumnEvaluatorCache);

        RowComparerProvider_ = CreateRowComparerProvider(GetConfig()->TabletNode->ColumnEvaluatorCache->CGCache);

        StoreCompactor_ = CreateStoreCompactor(this);
        StoreFlusher_ = CreateStoreFlusher(this);
        StoreRotator_ = CreateStoreRotator(this);
        StoreTrimmer_ = CreateStoreTrimmer(this);
        HunkChunkSweeper_ = CreateHunkChunkSweeper(this);
        PartitionBalancer_ = CreatePartitionBalancer(this);
        BackingStoreCleaner_ = CreateBackingStoreCleaner(this);
        LsmInterop_ = CreateLsmInterop(this, StoreCompactor_, PartitionBalancer_, StoreRotator_);

        GetRpcServer()->RegisterService(CreateTabletCellService(this));
        GetRpcServer()->RegisterService(CreateQueryService(GetConfig()->QueryAgent, this));

        SlotManager_->Initialize();
    }

    void Run() override
    {
        SetNodeByYPath(
            GetOrchidRoot(),
            "/tablet_cells",
            CreateVirtualNode(GetCellarManager()->GetCellar(ECellarType::Tablet)->GetOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/store_compactor",
            CreateVirtualNode(StoreCompactor_->GetOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/tablet_slot_manager",
            CreateVirtualNode(SlotManager_->GetOrchidService()));

        MasterConnector_->Initialize();
        StoreCompactor_->Start();
        StoreFlusher_->Start();
        StoreTrimmer_->Start();
        HunkChunkSweeper_->Start();
        BackingStoreCleaner_->Start();
        LsmInterop_->Start();
        HintManager_->Start();
    }

    const ITabletSnapshotStorePtr& GetTabletSnapshotStore() const override
    {
        return TabletSnapshotStore_;
    }

    const IInMemoryManagerPtr& GetInMemoryManager() const override
    {
        return InMemoryManager_;
    }

    const IResourceLimitsManagerPtr& GetResourceLimitsManager() const override
    {
        return GetCellarNodeBootstrap()->GetResourceLimitsManager();
    }

    const IStructuredLoggerPtr& GetStructuredLogger() const override
    {
        return StructuredLogger_;
    }

    const IHintManagerPtr& GetHintManager() const override
    {
        return HintManager_;
    }

    const ISlotManagerPtr& GetSlotManager() const override
    {
        return SlotManager_;
    }

    const ICellarManagerPtr& GetCellarManager() const override
    {
        return GetCellarNodeBootstrap()->GetCellarManager();
    }

    const IInvokerPtr& GetTransactionTrackerInvoker() const override
    {
        return GetCellarNodeBootstrap()->GetTransactionTrackerInvoker();
    }

    const IInvokerPtr& GetTableReplicatorPoolInvoker() const override
    {
        return TableReplicatorThreadPool_->GetInvoker();
    }

    const IInvokerPtr& GetTabletLookupPoolInvoker() const override
    {
        return TabletLookupThreadPool_->GetInvoker();
    }

    const IInvokerPtr& GetTabletFetchPoolInvoker() const override
    {
        return TabletFetchThreadPool_->GetInvoker();
    }

    IInvokerPtr GetQueryPoolInvoker(
        const TString& poolName,
        double weight,
        const TFairShareThreadPoolTag& tag) const override
    {
        return QueryThreadPool_->GetInvoker(poolName, weight, tag);
    }

    const IThroughputThrottlerPtr& GetThrottler(NTabletNode::ETabletNodeThrottlerKind kind) const override
    {
        return Throttlers_[kind];
    }

    const IThroughputThrottlerPtr& GetInThrottler(EWorkloadCategory category) const override
    {
        static const THashMap<EWorkloadCategory, ETabletNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
            {EWorkloadCategory::SystemTabletCompaction,      ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn},
            {EWorkloadCategory::SystemTabletPartitioning,    ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn},
            {EWorkloadCategory::SystemTabletPreload,         ETabletNodeThrottlerKind::StaticStorePreloadIn},
            // NB: |UserBatch| is intentionally not accounted in |UserBackendIn|.
            {EWorkloadCategory::UserInteractive,             ETabletNodeThrottlerKind::UserBackendIn},
            {EWorkloadCategory::UserRealtime,                ETabletNodeThrottlerKind::UserBackendIn},
        };
        auto it = WorkloadCategoryToThrottlerKind.find(category);
        return it == WorkloadCategoryToThrottlerKind.end()
            ? GetTotalInThrottler()
            : Throttlers_[it->second];
    }

    const IThroughputThrottlerPtr& GetOutThrottler(EWorkloadCategory category) const override
    {
        static const THashMap<EWorkloadCategory, ETabletNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
            {EWorkloadCategory::SystemTabletCompaction,      ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut},
            {EWorkloadCategory::SystemTabletPartitioning,    ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut},
            {EWorkloadCategory::SystemTabletStoreFlush,      ETabletNodeThrottlerKind::StoreFlushOut},
            {EWorkloadCategory::SystemTabletReplication,     ETabletNodeThrottlerKind::ReplicationOut},
            {EWorkloadCategory::UserDynamicStoreRead,        ETabletNodeThrottlerKind::DynamicStoreReadOut}
        };
        auto it = WorkloadCategoryToThrottlerKind.find(category);
        return it == WorkloadCategoryToThrottlerKind.end()
            ? GetTotalOutThrottler()
            : Throttlers_[it->second];
    }

    const IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() const override
    {
        return ColumnEvaluatorCache_;
    }

    const IRowComparerProviderPtr& GetRowComparerProvider() const override
    {
        return RowComparerProvider_;
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const NCellarNode::IMasterConnectorPtr& GetCellarNodeMasterConnector() const override
    {
        return GetCellarNodeBootstrap()->GetMasterConnector();
    }

    const IChunkRegistryPtr& GetChunkRegistry() const override
    {
        if (ClusterNodeBootstrap_->IsDataNode()) {
            return ClusterNodeBootstrap_
                ->GetDataNodeBootstrap()
                ->GetChunkRegistry();
        } else {
            const static IChunkRegistryPtr NullChunkRegistry;
            return NullChunkRegistry;
        }
    }

    const IChunkBlockManagerPtr& GetChunkBlockManager() const override
    {
        if (ClusterNodeBootstrap_->IsDataNode()) {
            return ClusterNodeBootstrap_
                ->GetDataNodeBootstrap()
                ->GetChunkBlockManager();
        } else {
            const static IChunkBlockManagerPtr NullChunkBlockManager;
            return NullChunkBlockManager;
        }
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    IMasterConnectorPtr MasterConnector_;

    ITabletSnapshotStorePtr TabletSnapshotStore_;
    IInMemoryManagerPtr InMemoryManager_;
    IStructuredLoggerPtr StructuredLogger_;
    IHintManagerPtr HintManager_;
    ISlotManagerPtr SlotManager_;

    TThreadPoolPtr TableReplicatorThreadPool_;
    TThreadPoolPtr TabletLookupThreadPool_;
    TThreadPoolPtr TabletFetchThreadPool_;
    ITwoLevelFairShareThreadPoolPtr QueryThreadPool_;

    TEnumIndexedVector<ETabletNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedVector<ETabletNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    IRowComparerProviderPtr RowComparerProvider_;

    IStoreCompactorPtr StoreCompactor_;
    IStoreFlusherPtr StoreFlusher_;
    IStoreRotatorPtr StoreRotator_;
    IStoreTrimmerPtr StoreTrimmer_;
    IHunkChunkSweeperPtr HunkChunkSweeper_;
    IPartitionBalancerPtr PartitionBalancer_;
    IBackingStoreCleanerPtr BackingStoreCleaner_;
    ILsmInteropPtr LsmInterop_;

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        for (auto kind : TEnumTraits<NTabletNode::ETabletNodeThrottlerKind>::GetDomainValues()) {
            const auto& initialThrottlerConfig = newConfig->TabletNode->Throttlers[kind]
                ? newConfig->TabletNode->Throttlers[kind]
                : GetConfig()->TabletNode->Throttlers[kind];
            auto throttlerConfig = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(initialThrottlerConfig);
            RawThrottlers_[kind]->Reconfigure(std::move(throttlerConfig));
        }

        TableReplicatorThreadPool_->Configure(
            newConfig->TabletNode->TabletManager->ReplicatorThreadPoolSize.value_or(
                GetConfig()->TabletNode->TabletManager->ReplicatorThreadPoolSize));
        QueryThreadPool_->Configure(
            newConfig->QueryAgent->QueryThreadPoolSize.value_or(GetConfig()->QueryAgent->QueryThreadPoolSize));
        TabletLookupThreadPool_->Configure(
            newConfig->QueryAgent->LookupThreadPoolSize.value_or(GetConfig()->QueryAgent->LookupThreadPoolSize));
        TabletFetchThreadPool_->Configure(
            newConfig->QueryAgent->FetchThreadPoolSize.value_or(GetConfig()->QueryAgent->FetchThreadPoolSize));

        ColumnEvaluatorCache_->Reconfigure(newConfig->TabletNode->ColumnEvaluatorCache);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
