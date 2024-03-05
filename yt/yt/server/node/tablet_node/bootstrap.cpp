#include "bootstrap.h"

#include "backing_store_cleaner.h"
#include "compression_dictionary_builder.h"
#include "compression_dictionary_manager.h"
#include "error_manager.h"
#include "hedging_manager_registry.h"
#include "hint_manager.h"
#include "hunk_chunk_sweeper.h"
#include "in_memory_manager.h"
#include "in_memory_service.h"
#include "lsm_interop.h"
#include "master_connector.h"
#include "overload_controller.h"
#include "partition_balancer.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "sorted_dynamic_comparer.h"
#include "statistics_reporter.h"
#include "store_compactor.h"
#include "store_flusher.h"
#include "store_rotator.h"
#include "store_trimmer.h"
#include "structured_logger.h"
#include "table_config_manager.h"
#include "tablet_cell_service.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>
#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>
#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/server/node/data_node/bootstrap.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/query_agent/config.h>
#include <yt/yt/server/node/query_agent/query_service.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/containers/disk_manager/config.h>
#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>
#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/rpc/dispatcher.h>

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
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

static const TString BusXferThreadPoolName = "BusXfer";
static const TString CompressionThreadPoolName = "Compression";
static const TString LookupThreadPoolName = "TabletLookup";
static const TString QueryThreadPoolName = "Query";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPoolWeightCache)

class TPoolWeightCache
    : public TAsyncExpiringCache<TString, double>
    , public IPoolWeightProvider
{
public:
    TPoolWeightCache(
        TAsyncExpiringCacheConfigPtr config,
        TWeakPtr<NApi::NNative::IClient> client,
        IInvokerPtr invoker)
        : TAsyncExpiringCache(
            std::move(config),
            TabletNodeLogger.WithTag("Cache: PoolWeight"))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
    { }

    double GetWeight(const TString& poolName) override
    {
        auto poolWeight = DefaultQLExecutionPoolWeight;
        auto weightFuture = this->Get(poolName);
        if (auto optionalWeightOrError = weightFuture.TryGet()) {
            poolWeight = optionalWeightOrError->ValueOrThrow();
        }
        return poolWeight;
    }

private:
    static constexpr double DefaultQLExecutionPoolWeight = 1.0;

    const TWeakPtr<NApi::NNative::IClient> Client_;
    const IInvokerPtr Invoker_;

    TFuture<double> DoGet(
        const TString& poolName,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto client = Client_.Lock();
        if (!client) {
            return MakeFuture<double>(TError(NYT::EErrorCode::Canceled, "Client destroyed"));
        }
        return BIND(GetPoolWeight, std::move(client), poolName)
            .AsyncVia(Invoker_)
            .Run();
    }

    static double GetPoolWeight(const NApi::NNative::IClientPtr& client, const TString& poolName)
    {
        auto path = QueryPoolsPath + "/" + NYPath::ToYPathLiteral(poolName);

        NApi::TGetNodeOptions options;
        options.ReadFrom = NApi::EMasterChannelKind::Cache;
        auto rspOrError = WaitFor(client->GetNode(path + "/@weight", options));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return DefaultQLExecutionPoolWeight;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to get pool info from Cypress, assuming defaults (Pool: %v)",
                poolName);
            return DefaultQLExecutionPoolWeight;
        }

        try {
            return ConvertTo<double>(rspOrError.Value());
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error parsing pool weight retrieved from Cypress, assuming default (Pool: %v)",
                poolName);
            return DefaultQLExecutionPoolWeight;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPoolWeightCache)

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
        YT_LOG_INFO("Initializing tablet node");

        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

        GetBundleDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnBundleDynamicConfigChanged, this));

        MasterConnector_ = CreateMasterConnector(this);

        TabletSnapshotStore_ = CreateTabletSnapshotStore(GetConfig()->TabletNode, this);

        SlotManager_ = CreateSlotManager(this);

        InMemoryManager_ = CreateInMemoryManager(this);
        GetRpcServer()->RegisterService(CreateInMemoryService(this));

        StructuredLogger_ = CreateStructuredLogger(this);

        HintManager_ = CreateHintManager(this);

        HedgingManagerRegistry_ = CreateHedgingManagerRegistry(
            NChunkClient::TDispatcher::Get()->GetReaderInvoker());

        TableDynamicConfigManager_ = New<TTableDynamicConfigManager>(this);

        QueryThreadPool_ = CreateNewTwoLevelFairShareThreadPool(
            GetConfig()->QueryAgent->QueryThreadPoolSize,
            QueryThreadPoolName,
            {
                New<TPoolWeightCache>(
                    GetConfig()->QueryAgent->PoolWeightCache,
                    GetClient(),
                    GetControlInvoker())
            });

        TableReplicatorThreadPool_ = CreateThreadPool(
            GetConfig()->TabletNode->TabletManager->ReplicatorThreadPoolSize,
            "Replicator");
        TabletLookupThreadPool_ = CreateThreadPool(
            GetConfig()->QueryAgent->LookupThreadPoolSize,
            LookupThreadPoolName);
        TabletFetchThreadPool_ = CreateThreadPool(
            GetConfig()->QueryAgent->FetchThreadPoolSize,
            "TabletFetch");
        TableRowFetchThreadPool_ = CreateThreadPool(
            GetConfig()->QueryAgent->TableRowFetchThreadPoolSize,
            "TableRowFetch");

        if (GetConfig()->EnableFairThrottler) {
            for (auto kind : {
                ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn,
                ETabletNodeThrottlerKind::ReplicationIn,
                ETabletNodeThrottlerKind::StaticStorePreloadIn,
                ETabletNodeThrottlerKind::UserBackendIn,
            }) {
                Throttlers_[kind] = ClusterNodeBootstrap_->GetInThrottler(FormatEnum(kind));
            }

            for (auto kind : {
                ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut,
                ETabletNodeThrottlerKind::StoreFlushOut,
                ETabletNodeThrottlerKind::ReplicationOut,
                ETabletNodeThrottlerKind::DynamicStoreReadOut,
            }) {
                Throttlers_[kind] = ClusterNodeBootstrap_->GetOutThrottler(FormatEnum(kind));
            }
        } else {
            for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
                auto throttlerConfig = GetConfig()->TabletNode->Throttlers[kind];
                throttlerConfig = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(throttlerConfig);
                LegacyRawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                    std::move(throttlerConfig),
                    ToString(kind),
                    TabletNodeLogger,
                    TabletNodeProfiler.WithPrefix("/throttlers"));
            }

            static const THashSet<ETabletNodeThrottlerKind> InCombinedTabletNodeThrottlerKinds = {
                ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn,
                ETabletNodeThrottlerKind::ReplicationIn,
                ETabletNodeThrottlerKind::StaticStorePreloadIn,
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
                auto throttler = IThroughputThrottlerPtr(LegacyRawThrottlers_[kind]);
                if (InCombinedTabletNodeThrottlerKinds.contains(kind)) {
                    throttler = CreateCombinedThrottler({GetDefaultInThrottler(), throttler});
                }
                if (OutCombinedTabletNodeThrottlerKinds.contains(kind)) {
                    throttler = CreateCombinedThrottler({GetDefaultOutThrottler(), throttler});
                }
                if (InStealingTabletNodeThrottlerKinds.contains(kind)) {
                    throttler = CreateStealingThrottler(throttler, GetDefaultInThrottler());
                }
                Throttlers_[kind] = throttler;
            }
        }

        ColumnEvaluatorCache_ = NQueryClient::CreateColumnEvaluatorCache(GetConfig()->TabletNode->ColumnEvaluatorCache);

        RowComparerProvider_ = NQueryClient::CreateRowComparerProvider(GetConfig()->TabletNode->ColumnEvaluatorCache->CGCache);

        StatisticsReporter_ = New<TStatisticsReporter>(this);
        StoreCompactor_ = CreateStoreCompactor(this);
        StoreFlusher_ = CreateStoreFlusher(this);
        StoreRotator_ = CreateStoreRotator(this);
        StoreTrimmer_ = CreateStoreTrimmer(this);
        HunkChunkSweeper_ = CreateHunkChunkSweeper(this);
        PartitionBalancer_ = CreatePartitionBalancer(this);
        BackingStoreCleaner_ = CreateBackingStoreCleaner(this);
        LsmInterop_ = CreateLsmInterop(this, StoreCompactor_, PartitionBalancer_, StoreRotator_);
        CompressionDictionaryBuilder_ = CreateCompressionDictionaryBuilder(this);
        ErrorManager_ = New<TErrorManager>(this);
        CompressionDictionaryManager_ = CreateCompressionDictionaryManager(
            GetConfig()->TabletNode->CompressionDictionaryCache,
            this);

        InitializeOverloadController();

        GetRpcServer()->RegisterService(CreateQueryService(GetConfig()->QueryAgent, this));
        GetRpcServer()->RegisterService(CreateTabletCellService(this));

        DiskManagerProxy_ = CreateDiskManagerProxy(GetConfig()->DiskManagerProxy);
        DiskInfoProvider_ = New<NContainers::TDiskInfoProvider>(
            DiskManagerProxy_,
            GetConfig()->DiskInfoProvider);
        DiskChangeChecker_ = New<TDiskChangeChecker>(
            DiskInfoProvider_,
            GetControlInvoker(),
            TabletNodeLogger);

        SlotManager_->Initialize();
        MasterConnector_->Initialize();

        SubscribePopulateAlerts(BIND(&TDiskChangeChecker::PopulateAlerts, DiskChangeChecker_));
    }

    void InitializeOverloadController()
    {
        OverloadController_ = New<TOverloadController>(New<TOverloadControllerConfig>());
        OverloadController_->TrackInvoker(BusXferThreadPoolName, NBus::TTcpDispatcher::Get()->GetXferPoller()->GetInvoker());
        OverloadController_->TrackInvoker(CompressionThreadPoolName, NRpc::TDispatcher::Get()->GetCompressionPoolInvoker());
        OverloadController_->TrackInvoker(LookupThreadPoolName, TabletLookupThreadPool_->GetInvoker());
        OverloadController_->TrackFSHThreadPool(QueryThreadPoolName, QueryThreadPool_);
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
            "/store_flusher",
            CreateVirtualNode(StoreFlusher_->GetOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/tablet_slot_manager",
            CreateVirtualNode(SlotManager_->GetOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/tablet_snapshot_store",
            CreateVirtualNode(TabletSnapshotStore_->GetOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/tablet_node_thread_pools",
            CreateVirtualNode(CreateThreadPoolsOrchidService()));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/disk_monitoring",
            CreateVirtualNode(DiskChangeChecker_->GetOrchidService()));

        StoreCompactor_->Start();
        StoreFlusher_->Start();
        StoreTrimmer_->Start();
        HunkChunkSweeper_->Start();
        StatisticsReporter_->Start();
        BackingStoreCleaner_->Start();
        LsmInterop_->Start();
        HintManager_->Start();
        TableDynamicConfigManager_->Start();
        SlotManager_->Start();
        CompressionDictionaryBuilder_->Start();
        OverloadController_->Start();
        DiskChangeChecker_->Start();
        ErrorManager_->Start();
    }

    NYTree::IYPathServicePtr CreateThreadPoolsOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TBootstrap::BuildThreadPoolsOrchid, this))
            ->Via(GetControlInvoker());
    }

    void BuildThreadPoolsOrchid(IYsonConsumer* consumer)
    {
        VERIFY_INVOKER_AFFINITY(GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("table_replicator_thread_pool_size").Value(TableReplicatorThreadPool_->GetThreadCount())
                .Item("tablet_lookup_thread_pool_size").Value(TabletLookupThreadPool_->GetThreadCount())
                .Item("tablet_fetch_thread_pool_size").Value(TabletFetchThreadPool_->GetThreadCount())
                .Item("table_row_fetch_thread_pool_size").Value(TableRowFetchThreadPool_->GetThreadCount())
                .Item("query_thread_pool_size").Value(QueryThreadPool_->GetThreadCount())
            .EndMap();
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

    const IHedgingManagerRegistryPtr& GetHedgingManagerRegistry() const override
    {
        return HedgingManagerRegistry_;
    }

    const TTableDynamicConfigManagerPtr& GetTableDynamicConfigManager() const override
    {
        return TableDynamicConfigManager_;
    }

    const TErrorManagerPtr& GetErrorManager() const override
    {
        return ErrorManager_;
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

    const IInvokerPtr& GetTableRowFetchPoolInvoker() const override
    {
        return TableRowFetchThreadPool_->GetInvoker();
    }

    IInvokerPtr GetQueryPoolInvoker(
        const TString& poolName,
        const TFairShareThreadPoolTag& tag) const override
    {
        return QueryThreadPool_->GetInvoker(poolName, tag);
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
            ? GetDefaultInThrottler()
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
            ? GetDefaultOutThrottler()
            : Throttlers_[it->second];
    }

    const IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() const override
    {
        return ColumnEvaluatorCache_;
    }

    const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const override
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

    const ICompressionDictionaryManagerPtr& GetCompressionDictionaryManager() const override
    {
        return CompressionDictionaryManager_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    IMasterConnectorPtr MasterConnector_;

    ITabletSnapshotStorePtr TabletSnapshotStore_;
    IInMemoryManagerPtr InMemoryManager_;
    IStructuredLoggerPtr StructuredLogger_;
    IHintManagerPtr HintManager_;
    IHedgingManagerRegistryPtr HedgingManagerRegistry_;
    TTableDynamicConfigManagerPtr TableDynamicConfigManager_;
    ISlotManagerPtr SlotManager_;

    IThreadPoolPtr TableReplicatorThreadPool_;
    IThreadPoolPtr TabletLookupThreadPool_;
    IThreadPoolPtr TabletFetchThreadPool_;
    IThreadPoolPtr TableRowFetchThreadPool_;

    ITwoLevelFairShareThreadPoolPtr QueryThreadPool_;

    TEnumIndexedArray<ETabletNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> LegacyRawThrottlers_;
    TEnumIndexedArray<ETabletNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    NQueryClient::IRowComparerProviderPtr RowComparerProvider_;

    IStoreCompactorPtr StoreCompactor_;
    IStoreFlusherPtr StoreFlusher_;
    IStoreRotatorPtr StoreRotator_;
    IStoreTrimmerPtr StoreTrimmer_;
    IHunkChunkSweeperPtr HunkChunkSweeper_;
    IPartitionBalancerPtr PartitionBalancer_;
    TStatisticsReporterPtr StatisticsReporter_;
    IBackingStoreCleanerPtr BackingStoreCleaner_;
    ILsmInteropPtr LsmInterop_;
    ICompressionDictionaryBuilderPtr CompressionDictionaryBuilder_;
    TErrorManagerPtr ErrorManager_;
    ICompressionDictionaryManagerPtr CompressionDictionaryManager_;
    TOverloadControllerPtr OverloadController_;

    NContainers::IDiskManagerProxyPtr DiskManagerProxy_;
    NContainers::TDiskInfoProviderPtr DiskInfoProvider_;
    TDiskChangeCheckerPtr DiskChangeChecker_;

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        if (!GetConfig()->EnableFairThrottler) {
            for (auto kind : TEnumTraits<NTabletNode::ETabletNodeThrottlerKind>::GetDomainValues()) {
                const auto& initialThrottlerConfig = newConfig->TabletNode->Throttlers[kind]
                    ? newConfig->TabletNode->Throttlers[kind]
                    : GetConfig()->TabletNode->Throttlers[kind];
                auto throttlerConfig = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(initialThrottlerConfig);
                LegacyRawThrottlers_[kind]->Reconfigure(std::move(throttlerConfig));
            }
        }

        TableReplicatorThreadPool_->Configure(
            newConfig->TabletNode->TabletManager->ReplicatorThreadPoolSize.value_or(
                GetConfig()->TabletNode->TabletManager->ReplicatorThreadPoolSize));
        ColumnEvaluatorCache_->Configure(newConfig->TabletNode->ColumnEvaluatorCache);

        auto bundleConfig = GetBundleDynamicConfigManager()->GetConfig();
        ReconfigureQueryAgent(bundleConfig, newConfig);

        OverloadController_->Reconfigure(newConfig->TabletNode->OverloadController);

        StatisticsReporter_->Reconfigure(newConfig);

        CompressionDictionaryManager_->OnDynamicConfigChanged(newConfig->TabletNode->CompressionDictionaryCache);

        DiskManagerProxy_->OnDynamicConfigChanged(newConfig->DiskManagerProxy);
        ErrorManager_->Reconfigure(newConfig);
    }

    void OnBundleDynamicConfigChanged(
        const TBundleDynamicConfigPtr& /*oldConfig*/,
        const TBundleDynamicConfigPtr& newConfig)
    {
        auto nodeConfig = GetDynamicConfigManager()->GetConfig();
        ReconfigureQueryAgent(newConfig, nodeConfig);
    }

    void ReconfigureQueryAgent(
        const TBundleDynamicConfigPtr& bundleConfig,
        const TClusterNodeDynamicConfigPtr& nodeConfig)
    {
        TabletFetchThreadPool_->Configure(
            nodeConfig->QueryAgent->FetchThreadPoolSize.value_or(GetConfig()->QueryAgent->FetchThreadPoolSize));
        TableRowFetchThreadPool_->Configure(
            nodeConfig->QueryAgent->TableRowFetchThreadPoolSize.value_or(GetConfig()->QueryAgent->TableRowFetchThreadPoolSize));

        {
            auto fallbackQueryThreadCount = nodeConfig->QueryAgent->QueryThreadPoolSize.value_or(
                GetConfig()->QueryAgent->QueryThreadPoolSize);
            QueryThreadPool_->Configure(
                bundleConfig->CpuLimits->QueryThreadPoolSize.value_or(fallbackQueryThreadCount));
        }

        {
            auto fallbackLookupThreadCount = nodeConfig->QueryAgent->LookupThreadPoolSize.value_or(
                GetConfig()->QueryAgent->LookupThreadPoolSize);
            TabletLookupThreadPool_->Configure(
                bundleConfig->CpuLimits->LookupThreadPoolSize.value_or(fallbackLookupThreadCount));
        }
    }

    const TOverloadControllerPtr& GetOverloadController() const override
    {
        return OverloadController_;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
