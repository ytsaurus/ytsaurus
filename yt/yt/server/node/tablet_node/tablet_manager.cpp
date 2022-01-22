#include "tablet_manager.h"
#include "private.h"
#include "automaton.h"
#include "bootstrap.h"
#include "distributed_throttler_manager.h"
#include "sorted_chunk_store.h"
#include "ordered_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "ordered_dynamic_store.h"
#include "hunk_chunk.h"
#include "replicated_store_manager.h"
#include "in_memory_manager.h"
#include "partition.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "sorted_store_manager.h"
#include "ordered_store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_cell_write_manager.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "table_replicator.h"
#include "tablet_profiling.h"
#include "tablet_snapshot_store.h"
#include "table_puller.h"
#include "backup_manager.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/chunk_block_manager.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/node/tablet_node/transaction_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>
#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/compact_vector.h>
#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/generic/cast.h>
#include <util/generic/algorithm.h>

namespace NYT::NTabletNode {

using namespace NCompression;
using namespace NConcurrency;
using namespace NChaosClient;
using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NClusterNode;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NHiveServer;
using namespace NHiveServer::NProto;
using namespace NQueryClient;
using namespace NApi;
using namespace NProfiling;
using namespace NDistributedThrottler;

using NLsm::EStoreRotationReason;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
    , public ITabletCellWriteManagerHost
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot->GetCellId(),
            slot->GetSimpleHydraManager(),
            slot->GetAutomaton(),
            slot->GetAutomatonInvoker())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
        , Config_(config)
        , TabletContext_(this)
        , TabletMap_(TTabletMapTraits(this))
        , DistributedThrottlerManager_(
            CreateDistributedThrottlerManager(Bootstrap_, Slot_->GetCellId()))
        , DecommissionCheckExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(),
            BIND(&TImpl::OnCheckTabletCellDecommission, MakeWeak(this)),
            Config_->TabletCellDecommissionCheckPeriod))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
        , BackupManager_(CreateBackupManager(
            Slot_,
            Bootstrap_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));
        RegisterLoader(
            "TabletManager.Async",
            BIND(&TImpl::LoadAsync, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
        RegisterSaver(
            EAsyncSerializationPriority::Default,
            "TabletManager.Async",
            BIND(&TImpl::SaveAsync, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraMountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnmountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFreezeTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnfreezeTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraTrimRows, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraLockTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraReportTabletLocked, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnlockTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRotateStore, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSplitPartition, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraMergePartitions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdatePartitionSampleKeys, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAddTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTableReplicaEnabled, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDecommissionTabletCell, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletCellDecommissioned, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnDynamicStoreAllocated, Unretained(this)));
    }

    void Initialize()
    {
        const auto& transactionManager = Slot_->GetTransactionManager();

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReplicateRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReplicateRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortReplicateRows, MakeStrong(this))));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareWritePulledRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitWritePulledRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortWritePulledRows, MakeStrong(this))));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUpdateTabletStores, MakeStrong(this))));

        BackupManager_->Initialize();
    }

    void Finalize()
    {
        DistributedThrottlerManager_->Finalize();
    }


    TTablet* GetTabletOrThrow(TTabletId id) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                id)
                << TErrorAttribute("tablet_id", id);
        }
        return tablet;
    }

    std::vector<TTabletMemoryStats> GetMemoryStats()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TTabletMemoryStats> results;
        results.reserve(Tablets().size());

        for (const auto& [tabletId, tablet] : Tablets()) {
            auto& tabletMemory = results.emplace_back();
            tabletMemory.TabletId = tabletId;
            tabletMemory.TablePath = tablet->GetTablePath();

            auto& stats = tabletMemory.Stats;

            if (tablet->IsPhysicallySorted()) {
                for (const auto& store : tablet->GetEden()->Stores()) {
                    CountStoreMemoryStats(&stats, *store);
                }

                for (const auto& partition : tablet->PartitionList()) {
                    for (const auto& store : partition->Stores()) {
                        CountStoreMemoryStats(&stats, *store);
                    }
                }
            } else if (tablet->IsPhysicallyOrdered()) {
                for (const auto& [_, store] : tablet->StoreIdMap()) {
                    CountStoreMemoryStats(&stats, *store);
                }
            }

            auto error = GetPreloadError(tablet);
            if (!error.IsOK()) {
                stats.PreloadErrors.push_back(error);
            }

            if (auto rowCache = tablet->GetRowCache()) {
                stats.RowCache.Usage = rowCache->GetUsedBytesCount();
            }
        }

        return results;
    }

    TError GetPreloadError(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tablet->GetId(), tablet->GetMountRevision());
        if (tabletSnapshot) {
            return tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Preload].Load();
        }
        return {};
    }

    void CountStoreMemoryStats(TMemoryStats* stats, IStore& store)
    {
        if (store.IsDynamic()) {
            auto dynamic = store.AsDynamic();
            stats->Dynamic.Usage += dynamic->GetPoolCapacity();
        } else if (store.IsChunk()) {
            auto chunk = store.AsChunk();

            if (auto backing = chunk->GetBackingStore()) {
                stats->Dynamic.Usage += backing->GetPoolCapacity();
                stats->DynamicBacking.Usage += backing->GetPoolCapacity();
            }

            auto countChunkStoreMemory = [&] (i64 bytes) {
                stats->PreloadStoreCount += 1;
                switch (chunk->GetPreloadState()) {
                    case EStorePreloadState::Scheduled:
                    case EStorePreloadState::Running:
                        stats->PendingStoreCount += 1;
                        stats->PendingStoreBytes += bytes;
                        break;

                    case EStorePreloadState::Complete:
                        stats->Static.Usage += bytes;
                        break;

                    case EStorePreloadState::Failed:
                        stats->PreloadStoreFailedCount += 1;
                        break;

                    case EStorePreloadState::None:
                        break;

                    default:
                        YT_ABORT();
                }
            };

            switch (chunk->GetInMemoryMode()) {
                case EInMemoryMode::Compressed:
                    countChunkStoreMemory(chunk->GetCompressedDataSize());
                    break;

                case EInMemoryMode::Uncompressed:
                    countChunkStoreMemory(chunk->GetUncompressedDataSize());
                    break;

                case EInMemoryMode::None:
                    break;

                default:
                    YT_ABORT();
            }
        }
    }

    TFuture<void> Trim(
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 trimmedRowCount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        try {
            auto* tablet = GetTabletOrThrow(tabletSnapshot->TabletId);

            if (tablet->IsPhysicallyLog()) {
                THROW_ERROR_EXCEPTION("Trim is not supported for this table type");
            }

            tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
            ValidateTabletMounted(tablet);

            i64 totalRowCount = tablet->GetTotalRowCount();
            if (trimmedRowCount > totalRowCount) {
                THROW_ERROR_EXCEPTION("Cannot trim tablet %v at row %v since it only has %v row(s)",
                    tablet->GetId(),
                    trimmedRowCount,
                    totalRowCount);
            }

            NProto::TReqTrimRows hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
            hydraRequest.set_mount_revision(tablet->GetMountRevision());
            hydraRequest.set_trimmed_row_count(trimmedRowCount);

            auto mutation = CreateMutation(Slot_->GetHydraManager(), hydraRequest);
            mutation->SetCurrentTraceContext();
            return mutation->Commit().As<void>();
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
    }

    void ScheduleStoreRotation(TTablet* tablet, EStoreRotationReason reason)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsRotationPossible()) {
            return;
        }

        storeManager->ScheduleRotation(reason);

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_reason(static_cast<int>(reason));
        Slot_->CommitTabletMutation(request);
    }

    void ReleaseBackingStore(const IChunkStorePtr& store)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (store->HasBackingStore()) {
            store->SetBackingStore(nullptr);
            YT_LOG_DEBUG("Backing store released (StoreId: %v)", store->GetId());
            // XXX(ifsmirnov): uncomment when tablet id is stored in TStoreBase.
            // store->GetTablet()->GetStructuredLogger()->OnBackingStoreReleased(store);
        }
    }

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const ITransactionPtr& transaction)
    {
        YT_LOG_DEBUG("Acquiring tablet stores commit semaphore (%v, TransactionId: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId());

        auto promise = NewPromise<void>();
        auto future = promise.ToFuture();
        tablet
            ->GetStoresUpdateCommitSemaphore()
            ->AsyncAcquire(
                BIND(
                    &TImpl::OnStoresUpdateCommitSemaphoreAcquired,
                    MakeWeak(this),
                    tablet,
                    transaction,
                    Passed(std::move(promise))),
                tablet->GetEpochAutomatonInvoker());
        return future;
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }

    ETabletCellLifeStage GetTabletCellLifeStage() const
    {
        return CellLifeStage_;
    }

    TTransactionManagerPtr GetTransactionManager() const override
    {
        return Slot_->GetTransactionManager();
    }

    TDynamicTabletCellOptionsPtr GetDynamicOptions() const override
    {
        return Slot_->GetDynamicOptions();
    }

    TTabletManagerConfigPtr GetConfig() const override
    {
        return Config_;
    }

    TTimestamp GetLatestTimestamp() const override
    {
        return Bootstrap_
            ->GetMasterConnection()
            ->GetTimestampProvider()
            ->GetLatestTimestamp();
    }

    // FindTablet and Tablets must override corresponding interface methods, so we inline
    // DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet) here.

    TTablet* FindTablet(const TTabletId& id) const override;
    TTablet* GetTablet(const TTabletId& id) const;
    const TReadOnlyEntityMap<TTablet>& Tablets() const override;

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TImpl> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& tablet : owner->Tablets()) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(tablet.first));
                }
            }
            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->Tablets().size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto tablet = owner->FindTablet(TTabletId::FromString(key))) {
                    auto producer = BIND(&TImpl::BuildTabletOrchidYson, owner, tablet);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TImpl> Owner_;

        explicit TOrchidService(TWeakPtr<TImpl> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND();
    };

    const TTabletManagerConfigPtr Config_;

    class TTabletContext
        : public ITabletContext
    {
    public:
        explicit TTabletContext(TImpl* owner)
            : Owner_(owner)
        { }

        TCellId GetCellId() override
        {
            return Owner_->Slot_->GetCellId();
        }

        const TString& GetTabletCellBundleName() override
        {
            return Owner_->Slot_->GetTabletCellBundleName();
        }

        EPeerState GetAutomatonState() override
        {
            return Owner_->Slot_->GetAutomatonState();
        }

        IColumnEvaluatorCachePtr GetColumnEvaluatorCache() override
        {
            return Owner_->Bootstrap_->GetColumnEvaluatorCache();
        }

        NTabletClient::IRowComparerProviderPtr GetRowComparerProvider() override
        {
            return Owner_->Bootstrap_->GetRowComparerProvider();
        }

        TObjectId GenerateId(EObjectType type) override
        {
            return Owner_->Slot_->GenerateId(type);
        }

        IStorePtr CreateStore(
            TTablet* tablet,
            EStoreType type,
            TStoreId storeId,
            const TAddStoreDescriptor* descriptor) override
        {
            return Owner_->CreateStore(tablet, type, storeId, descriptor);
        }

        THunkChunkPtr CreateHunkChunk(
            TTablet* tablet,
            TChunkId chunkId,
            const TAddHunkChunkDescriptor* descriptor) override
        {
            return Owner_->CreateHunkChunk(tablet, chunkId, descriptor);
        }

        TTransactionManagerPtr GetTransactionManager() override
        {
            return Owner_->Slot_->GetTransactionManager();
        }

        NRpc::IServerPtr GetLocalRpcServer() override
        {
            return Owner_->Bootstrap_->GetRpcServer();
        }

        NClusterNode::TNodeMemoryTrackerPtr GetMemoryUsageTracker() override
        {
            return Owner_->Bootstrap_->GetMemoryUsageTracker();
        }

        TString GetLocalHostName() override
        {
            return Owner_->Bootstrap_->GetLocalHostName();
        }

        NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() override
        {
            return Owner_->Bootstrap_->GetLocalDescriptor();
        }

    private:
        TImpl* const Owner_;
    };

    class TTabletMapTraits
    {
    public:
        explicit TTabletMapTraits(TImpl* owner)
            : Owner_(owner)
        { }

        std::unique_ptr<TTablet> Create(TTabletId id) const
        {
            return std::make_unique<TTablet>(id, &Owner_->TabletContext_);
        }

    private:
        TImpl* const Owner_;
    };

    TTabletContext TabletContext_;
    TEntityMap<TTablet, TTabletMapTraits> TabletMap_;
    ETabletCellLifeStage CellLifeStage_ = ETabletCellLifeStage::Running;

    TRingQueue<TTablet*> PrelockedTablets_;

    THashSet<IDynamicStorePtr> OrphanedStores_;
    THashMap<TTabletId, std::unique_ptr<TTablet>> OrphanedTablets_;

    const IDistributedThrottlerManagerPtr DistributedThrottlerManager_;

    const TPeriodicExecutorPtr DecommissionCheckExecutor_;

    const IYPathServicePtr OrchidService_;

    IBackupManagerPtr BackupManager_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        TabletMap_.SaveValues(context);
        Save(context, CellLifeStage_);
    }

    TCallback<void(TSaveContext&)> SaveAsync()
    {
        std::vector<std::pair<TTabletId, TCallback<void(TSaveContext&)>>> capturedTablets;
        for (auto [tabletId, tablet] : TabletMap_) {
            capturedTablets.emplace_back(tabletId, tablet->AsyncSave());
        }

        return BIND(
            [
                capturedTablets = std::move(capturedTablets)
            ] (TSaveContext& context) {
                using NYT::Save;
                for (const auto& [tabletId, callback] : capturedTablets) {
                    Save(context, tabletId);
                    callback(context);
                }
            });
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        TabletMap_.LoadValues(context);

        Load(context, CellLifeStage_);

        Automaton_->RememberReign(static_cast<TReign>(context.GetVersion()));
    }

    void LoadAsync(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        SERIALIZATION_DUMP_WRITE(context, "tablets[%v]", TabletMap_.size());
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != TabletMap_.size(); ++index) {
                auto tabletId = LoadSuspended<TTabletId>(context);
                auto* tablet = GetTablet(tabletId);
                SERIALIZATION_DUMP_WRITE(context, "%v =>", tabletId);
                SERIALIZATION_DUMP_INDENT(context) {
                    tablet->AsyncLoad(context);
                }
            }
        }
    }

    void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnAfterSnapshotLoaded();

        for (auto [tabletId, tablet] : TabletMap_) {
            tablet->SetStructuredLogger(
                Bootstrap_->GetStructuredLogger()->CreateLogger(tablet));
            auto storeManager = CreateStoreManager(tablet);
            tablet->SetStoreManager(storeManager);
            tablet->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
            tablet->FillProfilerTags();
            tablet->UpdateReplicaCounters();
            Bootstrap_->GetStructuredLogger()->OnHeartbeatRequest(
                Slot_->GetTabletManager(),
                true /*initial*/);
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        TabletMap_.Clear();
        OrphanedStores_.clear();
        OrphanedTablets_.clear();
    }


    void OnLeaderRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderRecoveryComplete();

        StartEpoch();
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderActive();

        for (auto [tabletId, tablet] : TabletMap_) {
            CheckIfTabletFullyUnlocked(tablet);
            CheckIfTabletFullyFlushed(tablet);
        }

        DecommissionCheckExecutor_->Start();
    }


    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        StopEpoch();

        DecommissionCheckExecutor_->Stop();
    }


    void OnFollowerRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnFollowerRecoveryComplete();

        StartEpoch();
    }

    void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopFollowing();

        StopEpoch();
    }


    void StartEpoch()
    {
        for (auto [tabletId, tablet] : TabletMap_) {
            StartTabletEpoch(tablet);
        }
    }

    void StopEpoch()
    {
        for (auto [tabletId, tablet] : TabletMap_) {
            StopTabletEpoch(tablet);
        }
    }


    void HydraMountTablet(TReqMountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto tableId = FromProto<TObjectId>(request->table_id());
        const auto& path = request->path();
        auto schemaId = FromProto<TObjectId>(request->schema_id());
        auto schema = FromProto<TTableSchemaPtr>(request->schema());
        auto pivotKey = request->has_pivot_key() ? FromProto<TLegacyOwningKey>(request->pivot_key()) : TLegacyOwningKey();
        auto nextPivotKey = request->has_next_pivot_key() ? FromProto<TLegacyOwningKey>(request->next_pivot_key()) : TLegacyOwningKey();
        auto settings = DeserializeTableSettings(request, tabletId);
        auto atomicity = FromProto<EAtomicity>(request->atomicity());
        auto commitOrdering = FromProto<ECommitOrdering>(request->commit_ordering());
        bool freeze = request->freeze();
        auto upstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        auto replicaDescriptors = FromProto<std::vector<TTableReplicaDescriptor>>(request->replicas());
        auto retainedTimestamp = request->has_retained_timestamp()
            ? FromProto<TTimestamp>(request->retained_timestamp())
            : MinTimestamp;
        const auto& mountHint = request->mount_hint();

        auto tabletHolder = std::make_unique<TTablet>(
            tabletId,
            settings,
            mountRevision,
            tableId,
            path,
            &TabletContext_,
            schemaId,
            schema,
            pivotKey,
            nextPivotKey,
            atomicity,
            commitOrdering,
            upstreamReplicaId,
            retainedTimestamp);

        tabletHolder->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
        tabletHolder->FillProfilerTags();
        tabletHolder->SetStructuredLogger(
            Bootstrap_->GetStructuredLogger()->CreateLogger(tabletHolder.get()));
        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        if (tablet->IsPhysicallyOrdered()) {
            tablet->SetTrimmedRowCount(request->trimmed_row_count());
        }

        auto storeManager = CreateStoreManager(tablet);
        tablet->SetStoreManager(storeManager);
        PopulateDynamicStoreIdPool(tablet, request);

        storeManager->Mount(
            MakeRange(request->stores()),
            MakeRange(request->hunk_chunks()),
            /*createDynamicStore*/ !freeze,
            mountHint);

        tablet->SetState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet mounted (%v, MountRevision: %llx, Keys: %v .. %v, "
            "StoreCount: %v, HunkChunkCount: %v, PartitionCount: %v, TotalRowCount: %v, TrimmedRowCount: %v, Atomicity: %v, "
            "CommitOrdering: %v, Frozen: %v, UpstreamReplicaId: %v, RetainedTimestamp: %llx, SchemaId: %v)",
            tablet->GetLoggingTag(),
            mountRevision,
            pivotKey,
            nextPivotKey,
            request->stores_size(),
            request->hunk_chunks_size(),
            tablet->IsPhysicallySorted() ? std::make_optional(tablet->PartitionList().size()) : std::nullopt,
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTotalRowCount()),
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTrimmedRowCount()),
            tablet->GetAtomicity(),
            tablet->GetCommitOrdering(),
            freeze,
            upstreamReplicaId,
            retainedTimestamp,
            schemaId);

        for (const auto& descriptor : request->replicas()) {
            AddTableReplica(tablet, descriptor);
        }

        if (request->has_replication_card_id()) {
            auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
            auto progress = FromProto<TReplicationProgress>(request->replication_progress());
            YT_LOG_DEBUG("Tablet bound for chaos replication (%v, ReplicationCardId: %v, ReplicationProgress: %v)",
                tablet->GetLoggingTag(),
                replicationCardId,
                progress);

            tablet->RuntimeData()->ReplicationProgress.Store(New<TRefCountedReplicationProgress>(std::move(progress)));
            AddChaosAgent(tablet, replicationCardId);
        }

        const auto& lockManager = tablet->GetLockManager();

        for (const auto& lock : request->locks()) {
            auto transactionId = FromProto<TTabletId>(lock.transaction_id());
            auto lockTimestamp = static_cast<TTimestamp>(lock.timestamp());
            lockManager->Lock(lockTimestamp, transactionId, true);
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            response.set_frozen(freeze);
            PostMasterMessage(tabletId, response);
        }

        tablet->GetStructuredLogger()->OnFullHeartbeat();

        if (!IsRecovery()) {
            StartTabletEpoch(tablet);
        }
    }

    void HydraUnmountTablet(TReqUnmountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (request->force()) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet is forcefully unmounted (%v)",
                tablet->GetLoggingTag());

            auto tabletHolder = TabletMap_.Release(tabletId);

            if (tablet->GetTabletLockCount() > 0) {
                SetTabletOrphaned(std::move(tabletHolder));
            } else {
                // Just a formality.
                tablet->SetState(ETabletState::Unmounted);
            }

            for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                SetStoreOrphaned(tablet, store);
            }

            const auto& storeManager = tablet->GetStoreManager();
            for (const auto& store : storeManager->GetLockedStores()) {
                SetStoreOrphaned(tablet, store);
            }

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
            }
        } else {
            auto state = tablet->GetState();
            if (IsInUnmountWorkflow(state)) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Requested to unmount a tablet in a wrong state, ignored (State: %v, %v)",
                    state,
                    tablet->GetLoggingTag());
                return;
            }

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Unmounting tablet (%v)",
                tablet->GetLoggingTag());

            tablet->SetState(ETabletState::UnmountWaitingForLocks);

            YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet locks to be released (%v)",
                tablet->GetLoggingTag());

            CheckIfTabletFullyUnlocked(tablet);
        }
    }

    void HydraRemountTablet(TReqRemountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto settings = DeserializeTableSettings(request, tabletId);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Remount(settings);

        tablet->ReconfigureThrottlers();
        tablet->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
        tablet->FillProfilerTags();
        tablet->UpdateReplicaCounters();
        tablet->GetStructuredLogger()->SetEnabled(settings.MountConfig->EnableStructuredLogger);
        UpdateTabletSnapshot(tablet);

        if (!IsRecovery()) {
            for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                StopTableReplicaEpoch(&replicaInfo);
                StartTableReplicaEpoch(tablet, &replicaInfo);
            }
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet remounted (%v)",
            tablet->GetLoggingTag());
    }

    void HydraFreezeTablet(TReqFreezeTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (IsInUnmountWorkflow(state) || IsInFreezeWorkflow(state)) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Requested to freeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Freezing tablet (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::FreezeWaitingForLocks);

        YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet locks to be released (%v)",
            tablet->GetLoggingTag());

        CheckIfTabletFullyUnlocked(tablet);
    }

    void HydraUnfreezeTablet(TReqUnfreezeTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Frozen)  {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Requested to unfreeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unfrozen (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::Mounted);

        PopulateDynamicStoreIdPool(tablet, request);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Rotate(true, EStoreRotationReason::None);
        storeManager->InitializeRotation();

        UpdateTabletSnapshot(tablet);

        TRspUnfreezeTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        PostMasterMessage(tabletId, response);
    }

    void HydraLockTablet(TReqLockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        auto transactionId = FromProto<TTabletId>(request->lock().transaction_id());
        auto lockTimestamp = static_cast<TTimestamp>(request->lock().timestamp());

        const auto& lockManager = tablet->GetLockManager();
        lockManager->Lock(lockTimestamp, transactionId, /*confirmed*/ false);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet locked (TabletId: %v, TransactionId: %v)",
            tabletId,
            transactionId);

        CheckIfTabletFullyUnlocked(tablet);
    }

    void HydraReportTabletLocked(TReqReportTabletLocked* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& lockManager = tablet->GetLockManager();
        auto transactionIds = lockManager->ExtractUnconfirmedTransactionIds();
        if (transactionIds.empty()) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet lock confirmed (TabletId: %v, TransactionIds: %v)",
            tabletId,
            transactionIds);

        TRspLockTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        ToProto(response.mutable_transaction_ids(), transactionIds);
        PostMasterMessage(tabletId, response);
    }

    void HydraUnlockTablet(TReqUnlockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        // COMPAT(ifsmirnov)
        if (request->has_mount_revision() && request->mount_revision() != 0) {
            auto mountRevision = request->mount_revision();
            if (mountRevision != tablet->GetMountRevision()) {
                return;
            }
        }

        auto transactionId = FromProto<TTabletId>(request->transaction_id());
        auto updateMode = FromProto<EUpdateMode>(request->update_mode());

        std::vector<TStoreId> addedStoreIds;
        std::vector<IStorePtr> storesToAdd;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());
            addedStoreIds.push_back(storeId);

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storesToAdd.push_back(std::move(store));
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (updateMode == EUpdateMode::Overwrite) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(),
                "All stores of tablet are going to be discarded (%v)",
                tablet->GetLoggingTag());

            tablet->ClearDynamicStoreIdPool();
            PopulateDynamicStoreIdPool(tablet, request);

            storeManager->DiscardAllStores();
        }

        const auto& structuredLogger = tablet->GetStructuredLogger();
        structuredLogger->OnTabletUnlocked(
            MakeRange(storesToAdd),
            updateMode == EUpdateMode::Overwrite,
            transactionId);

        storeManager->BulkAddStores(MakeRange(storesToAdd), /*onMount*/ false);

        const auto& lockManager = tablet->GetLockManager();

        if (tablet->GetAtomicity() == EAtomicity::Full) {
            auto nextEpoch = lockManager->GetEpoch() + 1;
            UpdateTabletSnapshot(tablet, nextEpoch);

            // COMPAT(ifsmirnov)
            auto commitTimestamp = request->has_commit_timestamp()
                ? request->commit_timestamp()
                : MinTimestamp;
            lockManager->Unlock(commitTimestamp, transactionId);
        } else {
            UpdateTabletSnapshot(tablet);
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(),
            "Tablet unlocked (%v, TransactionId: %v, AddedStoreIds: %v, LockManagerEpoch: %v)",
            tablet->GetLoggingTag(),
            transactionId,
            addedStoreIds,
            lockManager->GetEpoch());
    }

    void HydraSetTabletState(TReqSetTabletState* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto requestedState = ETabletState(request->state());

        switch (requestedState) {
            case ETabletState::FreezeFlushing: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Improper tablet state transition requested, ignored (CurrentState: %v, RequestedState: %v, %v)",
                        state,
                        requestedState,
                        tablet->GetLoggingTag());
                    return;
                }
                // No break intentionally.
            }

            case ETabletState::UnmountFlushing: {
                tablet->SetState(requestedState);

                const auto& storeManager = tablet->GetStoreManager();
                storeManager->Rotate(false, EStoreRotationReason::None);

                YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet stores to be flushed (%v, NewState: %v)",
                    tablet->GetLoggingTag(),
                    requestedState);

                CheckIfTabletFullyFlushed(tablet);
                break;
            }

            case ETabletState::Unmounted: {
                tablet->SetState(ETabletState::Unmounted);

                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unmounted (%v)",
                    tablet->GetLoggingTag());

                if (!IsRecovery()) {
                    StopTabletEpoch(tablet);
                }

                for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                    PostTableReplicaStatistics(tablet, replicaInfo);
                }

                TRspUnmountTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();
                if (auto replicationProgress = tablet->RuntimeData()->ReplicationProgress.Load()) {
                    ToProto(response.mutable_replication_progress(), *replicationProgress);
                }

                TabletMap_.Remove(tabletId);

                PostMasterMessage(tabletId, response);
                break;
            }

            case ETabletState::Frozen: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Improper tablet state transition requested, ignored (CurrentState %v, RequestedState: %v, %v)",
                        state,
                        requestedState,
                        tablet->GetLoggingTag());
                    return;
                }

                tablet->SetState(ETabletState::Frozen);
                tablet->ClearDynamicStoreIdPool();

                for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                    if (store->IsChunk()) {
                        ReleaseBackingStore(store->AsChunk());
                    }
                }

                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet frozen (%v)",
                    tablet->GetLoggingTag());

                TRspFreezeTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();
                PostMasterMessage(tabletId, response);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void HydraTrimRows(TReqTrimRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto trimmedRowCount = request->trimmed_row_count();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        UpdateTrimmedRowCount(tablet, trimmedRowCount);
    }

    void HydraRotateStore(TReqRotateStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead && tablet->DynamicStoreIdPool().empty()) {
            if (!tablet->GetDynamicStoreIdRequested()) {
                AllocateDynamicStore(tablet);
            }
            // TODO(ifsmirnov): Store flusher will try making unsuccessful mutations if response
            // from master comes late. Maybe should optimize.
            storeManager->UnscheduleRotation();
            return;
        }

        auto reason = static_cast<EStoreRotationReason>(request->reason());
        storeManager->Rotate(true, reason);
        UpdateTabletSnapshot(tablet);
    }

    void HydraPrepareUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);
        const auto& structuredLogger = tablet->GetStructuredLogger();

        // Validate.
        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            YT_VERIFY(hunkChunkIdsToAdd.insert(chunkId).second);
        }

        std::vector<TStoreId> storeIdsToAdd;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunkOrThrow(chunkId);
                        if (hunkChunk->GetState() != EHunkChunkState::Active) {
                            THROW_ERROR_EXCEPTION("Referenced hunk chunk %v is in %Qlv state",
                                chunkId,
                                hunkChunk->GetState());
                        }
                    }
                }
            }
            storeIdsToAdd.push_back(storeId);
        }

        std::vector<TStoreId> storeIdsToRemove;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            storeIdsToRemove.push_back(storeId);
            auto store = tablet->GetStoreOrThrow(storeId);
            auto state = store->GetStoreState();
            if (state != EStoreState::PassiveDynamic && state != EStoreState::Persistent) {
                THROW_ERROR_EXCEPTION("Store %v has invalid state %Qlv",
                    storeId,
                    state);
            }
        }

        std::vector<TChunkId> hunkChunkIdsToRemove;
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            hunkChunkIdsToRemove.push_back(chunkId);
            auto hunkChunk = tablet->GetHunkChunkOrThrow(chunkId);
            auto state = hunkChunk->GetState();
            if (state != EHunkChunkState::Active) {
                THROW_ERROR_EXCEPTION("Hunk chunk %v is in %Qlv state",
                    chunkId,
                    state);
            }
            if (!hunkChunk->IsDangling()) {
                THROW_ERROR_EXCEPTION("Hunk chunk %v is not dangling",
                    chunkId)
                    << TErrorAttribute("store_ref_count", hunkChunk->GetStoreRefCount())
                    << TErrorAttribute("prepared_store_ref_count", hunkChunk->GetPreparedStoreRefCount());
            }
        }

        // Prepare.
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto store = tablet->GetStore(storeId);
            store->SetStoreState(EStoreState::RemovePrepared);
            structuredLogger->OnStoreStateChanged(store);
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto hunkChunk = tablet->GetHunkChunk(chunkId);
            hunkChunk->SetState(EHunkChunkState::RemovePrepared);
            structuredLogger->OnHunkChunkStateChanged(hunkChunk);
        }

        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunk(chunkId);
                        tablet->UpdatePreparedStoreRefCount(hunkChunk, +1);
                    }
                }
            }
        }

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        // TODO(ifsmirnov): log preparation errors as well.
        structuredLogger->OnTabletStoresUpdatePrepared(
            storeIdsToAdd,
            storeIdsToRemove,
            updateReason,
            transaction->GetId());

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update prepared "
            "(%v, TransactionId: %v, StoreIdsToAdd: %v, HunkChunkIdsToAdd: %v, StoreIdsToRemove: %v, HunkChunkIdsToRemove: %v, "
            "UpdateReason: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId(),
            storeIdsToAdd,
            hunkChunkIdsToAdd,
            storeIdsToRemove,
            hunkChunkIdsToRemove,
            updateReason);
    }

    void BackoffStoreRemoval(TTablet* tablet, const IStorePtr& store)
    {
        switch (store->GetType()) {
            case EStoreType::SortedDynamic:
            case EStoreType::OrderedDynamic:
                store->SetStoreState(EStoreState::PassiveDynamic);
                break;
            case EStoreType::SortedChunk:
            case EStoreType::OrderedChunk:
                store->SetStoreState(EStoreState::Persistent);
                break;
            default:
                YT_ABORT();
        }

        tablet->GetStructuredLogger()->OnStoreStateChanged(store);

        if (IsLeader()) {
            tablet->GetStoreManager()->BackoffStoreRemoval(store);
        }
    }

    void HydraAbortUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            YT_VERIFY(hunkChunkIdsToAdd.insert(chunkId).second);
        }

        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->FindHunkChunk(chunkId);
                        if (!hunkChunk) {
                            continue;
                        }

                        tablet->UpdatePreparedStoreRefCount(hunkChunk, -1);
                    }
                }
            }
        }

        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (auto store = tablet->FindStore(storeId)) {
                BackoffStoreRemoval(tablet, store);
            }
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto hunkChunk = tablet->FindHunkChunk(chunkId);
            if (!hunkChunk) {
                continue;
            }

            hunkChunk->SetState(EHunkChunkState::Active);
        }

        CheckIfTabletFullyFlushed(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update aborted "
            "(%v, TransactionId: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId());
    }

    bool IsBackingStoreRequired(TTablet* tablet)
    {
        return
            tablet->GetAtomicity() == EAtomicity::Full &&
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime != TDuration::Zero();
    }

    void HydraCommitUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        if (auto discardStoresRevision = tablet->GetLastDiscardStoresRevision()) {
            auto prepareRevision = transaction->GetPrepareRevision();
            if (prepareRevision < discardStoresRevision) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Tablet stores update commit interrupted by stores discard, ignored "
                    "(%v, TransactionId: %v, DiscardStoresRevision: %llx, "
                    "PrepareUpdateTabletStoresRevision: %llx)",
                    tablet->GetLoggingTag(),
                    transaction->GetId(),
                    discardStoresRevision,
                    prepareRevision);

                // Validate that all prepared-for-removal stores were indeed discarded.
                for (const auto& descriptor : request->stores_to_remove()) {
                    auto storeId = FromProto<TStoreId>(descriptor.store_id());
                    if (const auto& store = tablet->FindStore(storeId)) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                            "Store prepared for removal was not discarded while tablet "
                            "stores update commit was interrupted by the discard "
                            "(%v, StoreId: %v, TransactionId: %v, DiscardStoresRevision: %llx, "
                            "PrepareUpdateTabletStoresRevision: %llx)",
                            tablet->GetLoggingTag(),
                            storeId,
                            transaction->GetId(),
                            discardStoresRevision,
                            prepareRevision);

                        BackoffStoreRemoval(tablet, store);
                    }
                }

                return;
            }
        }

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        const auto& storeManager = tablet->GetStoreManager();

        // NB: Must handle store removals before store additions since
        // row index map forbids having multiple stores with the same starting row index.
        // But before proceeding to removals, we must take care of backing stores.
        THashMap<TStoreId, IDynamicStorePtr> idToBackingStore;
        auto registerBackingStore = [&] (const IStorePtr& store) {
            YT_VERIFY(idToBackingStore.emplace(store->GetId(), store->AsDynamic()).second);
        };

        if (!IsRecovery()) {
            for (const auto& descriptor : request->stores_to_add()) {
                if (descriptor.has_backing_store_id()) {
                    auto backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                    auto backingStore = tablet->GetStore(backingStoreId);
                    registerBackingStore(backingStore);
                }
            }
        }

        THashSet<THunkChunkPtr> addedHunkChunks;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());

            auto hunkChunk = CreateHunkChunk(tablet, chunkId, &descriptor);
            hunkChunk->Initialize();
            tablet->AddHunkChunk(hunkChunk);
            YT_VERIFY(addedHunkChunks.insert(hunkChunk).second);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk added (%v, ChunkId: %v)",
                tablet->GetLoggingTag(),
                chunkId);
        }

        std::vector<TStoreId> removedStoreIds;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            removedStoreIds.push_back(storeId);

            auto store = tablet->GetStore(storeId);
            storeManager->RemoveStore(store);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Store removed (%v, StoreId: %v, DynamicMemoryUsage: %v)",
                tablet->GetLoggingTag(),
                storeId,
                store->GetDynamicMemoryUsage());

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, -1);

                    const auto& hunkChunk = ref.HunkChunk;

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk unreferenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
                        tablet->GetLoggingTag(),
                        storeId,
                        ref,
                        hunkChunk->GetStoreRefCount());
                }
            }
        }

        std::vector<TChunkId> removedHunkChunkIds;
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            removedHunkChunkIds.push_back(chunkId);

            auto hunkChunk = tablet->GetHunkChunk(chunkId);
            tablet->RemoveHunkChunk(hunkChunk);
            hunkChunk->SetState(EHunkChunkState::Removed);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk removed (%v, ChunkId: %v)",
                tablet->GetLoggingTag(),
                chunkId);
        }

        std::vector<IStorePtr> addedStores;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storeManager->AddStore(store, false);
            addedStores.push_back(store);

            TStoreId backingStoreId;
            if (!IsRecovery() && descriptor.has_backing_store_id() && IsBackingStoreRequired(tablet)) {
                backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                const auto& backingStore = GetOrCrash(idToBackingStore, backingStoreId);
                SetBackingStore(tablet, store, backingStore);
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk store added (%v, StoreId: %v, MaxTimestamp: %llx, BackingStoreId: %v)",
                tablet->GetLoggingTag(),
                storeId,
                store->GetMaxTimestamp(),
                backingStoreId);

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, +1);

                    const auto& hunkChunk = ref.HunkChunk;
                    if (!addedHunkChunks.contains(hunkChunk)) {
                        tablet->UpdatePreparedStoreRefCount(hunkChunk, -1);
                    }

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk referenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
                        tablet->GetLoggingTag(),
                        storeId,
                        ref,
                        hunkChunk->GetStoreRefCount());
                }
            }
        }

        auto retainedTimestamp = std::max(
            tablet->GetRetainedTimestamp(),
            static_cast<TTimestamp>(request->retained_timestamp()));
        tablet->SetRetainedTimestamp(retainedTimestamp);
        TDynamicStoreId allocatedDynamicStoreId;

        if (updateReason == ETabletStoresUpdateReason::Flush && request->request_dynamic_store_id()) {
            auto storeId = ReplaceTypeInId(
                transaction->GetId(),
                tablet->IsPhysicallySorted()
                    ? EObjectType::SortedDynamicTabletStore
                    : EObjectType::OrderedDynamicTabletStore);
            tablet->PushDynamicStoreIdToPool(storeId);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store id added to the pool (%v, StoreId: %v)",
                tablet->GetLoggingTag(),
                storeId);

            allocatedDynamicStoreId = storeId;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update committed "
            "(%v, TransactionId: %v, AddedStoreIds: %v, RemovedStoreIds: %v, AddedHunkChunkIds: %v, RemovedHunkChunkIds: %v, "
            "RetainedTimestamp: %llx, UpdateReason: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId(),
            MakeFormattableView(addedStores, TStoreIdFormatter()),
            removedStoreIds,
            MakeFormattableView(addedHunkChunks, THunkChunkIdFormatter()),
            removedHunkChunkIds,
            retainedTimestamp,
            updateReason);

        tablet->GetStructuredLogger()->OnTabletStoresUpdateCommitted(
            addedStores,
            removedStoreIds,
            std::vector<THunkChunkPtr>(addedHunkChunks.begin(), addedHunkChunks.end()),
            removedHunkChunkIds,
            updateReason,
            allocatedDynamicStoreId,
            transaction->GetId());

        UpdateTabletSnapshot(tablet);

        CheckIfTabletFullyFlushed(tablet);
    }

    void HydraSplitPartition(TReqSplitPartition* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->GetPartition(partitionId);

        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());

        int partitionIndex = partition->GetIndex();
        i64 partitionDataSize = partition->GetCompressedDataSize();

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        bool result = storeManager->SplitPartition(partition->GetIndex(), pivotKeys);
        if (!result) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition split failed (%v, PartitionId: %v, Keys: %v)",
                tablet->GetLoggingTag(),
                partitionId,
                JoinToString(pivotKeys, TStringBuf(" .. ")));
            return;
        }

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition split (%v, OriginalPartitionId: %v, "
            "ResultingPartitionIds: %v, DataSize: %v, Keys: %v)",
            tablet->GetLoggingTag(),
            partitionId,
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + partitionIndex,
                    tablet->PartitionList().data() + partitionIndex + pivotKeys.size()),
                TPartitionIdFormatter()),
            partitionDataSize,
            JoinToString(pivotKeys, TStringBuf(" .. ")));
    }

    void HydraMergePartitions(TReqMergePartitions* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto firstPartitionId = FromProto<TPartitionId>(request->partition_id());
        auto* firstPartition = tablet->GetPartition(firstPartitionId);

        int firstPartitionIndex = firstPartition->GetIndex();
        int lastPartitionIndex = firstPartitionIndex + request->partition_count() - 1;

        auto originalPartitionIds = Format("%v",
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        i64 partitionsDataSize = 0;
        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            const auto& partition = tablet->PartitionList()[index];
            partitionsDataSize += partition->GetCompressedDataSize();
        }

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->MergePartitions(
            firstPartition->GetIndex(),
            firstPartition->GetIndex() + request->partition_count() - 1);

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partitions merged (%v, OriginalPartitionIds: %v, "
            "ResultingPartitionId: %v, DataSize: %v)",
            tablet->GetLoggingTag(),
            originalPartitionIds,
            tablet->PartitionList()[firstPartitionIndex]->GetId(),
            partitionsDataSize);
    }

    void HydraUpdatePartitionSampleKeys(TReqUpdatePartitionSampleKeys* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->FindPartition(partitionId);
        if (!partition) {
            return;
        }

        TWireProtocolReader reader(TSharedRef::FromString(request->sample_keys()));
        auto sampleKeys = reader.ReadUnversionedRowset(true);

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->UpdatePartitionSampleKeys(partition, sampleKeys);

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition sample keys updated (%v, PartitionId: %v, SampleKeyCount: %v)",
            tablet->GetLoggingTag(),
            partition->GetId(),
            sampleKeys.Size());
    }

    void HydraAddTableReplica(TReqAddTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto* replicaInfo = AddTableReplica(tablet, request->replica());
        if (!replicaInfo) {
            return;
        }

        if (!IsRecovery()) {
            StartTableReplicaEpoch(tablet, replicaInfo);
        }
    }

    void HydraRemoveTableReplica(TReqRemoveTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        RemoveTableReplica(tablet, replicaId);
    }

    // COMPAT(aozeritsky)
    void HydraSetTableReplicaEnabled(TReqSetTableReplicaEnabled* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        bool enabled = request->enabled();
        if (enabled) {
            EnableTableReplica(tablet, replicaInfo);
        } else {
            DisableTableReplica(tablet, replicaInfo);
        }
        replicaInfo->RecomputeReplicaStatus();
    }

    void HydraAlterTableReplica(TReqAlterTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        auto enabled = request->has_enabled()
            ? std::make_optional(request->enabled())
            : std::nullopt;

        auto mode = request->has_mode()
            ? std::make_optional(ETableReplicaMode(request->mode()))
            : std::nullopt;
        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
        }

        auto atomicity = request->has_atomicity()
            ? std::make_optional(NTransactionClient::EAtomicity(request->atomicity()))
            : std::nullopt;
        auto preserveTimestamps = request->has_preserve_timestamps()
            ? std::make_optional(request->preserve_timestamps())
            : std::nullopt;

        if (enabled) {
            if (*enabled) {
                EnableTableReplica(tablet, replicaInfo);
            } else {
                DisableTableReplica(tablet, replicaInfo);
            }
            replicaInfo->RecomputeReplicaStatus();
        }

        if (mode) {
            replicaInfo->SetMode(*mode);
            replicaInfo->RecomputeReplicaStatus();
        }

        if (atomicity) {
            replicaInfo->SetAtomicity(*atomicity);
        }

        if (preserveTimestamps) {
            replicaInfo->SetPreserveTimestamps(*preserveTimestamps);
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica updated (%v, ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId(),
            enabled,
            mode,
            atomicity,
            preserveTimestamps);
    }

    void HydraPrepareWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto round = request->replication_round();
        auto* tablet = GetTabletOrThrow(tabletId);

        if (tablet->ChaosData()->ReplicationRound != round) {
            THROW_ERROR_EXCEPTION("Replication round mismatch: expected %v, got %v",
                tablet->ChaosData()->ReplicationRound,
                round);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Write pulled rows prepared (TabletId: %v, TransactionId: %v, ReplictionRound: %v",
            tabletId,
            transaction->GetId(),
            round);
    }

    void HydraCommitWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto round = request->replication_round();
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->ChaosData()->ReplicationRound == round);

        auto progress = New<TRefCountedReplicationProgress>(FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress()));
        tablet->RuntimeData()->ReplicationProgress.Store(progress);

        THashMap<TTabletId, i64> currentReplicationRowIndexes;
        for (auto protoEndReplicationRowIndex : request->new_replication_row_indexes()) {
            auto tabletId = FromProto<TTabletId>(protoEndReplicationRowIndex.tablet_id());
            auto endReplicationRowIndex = protoEndReplicationRowIndex.replication_row_index();
            YT_VERIFY(currentReplicationRowIndexes.insert(std::make_pair(tabletId, endReplicationRowIndex)).second);
        }

        tablet->ChaosData()->CurrentReplicationRowIndexes = std::move(currentReplicationRowIndexes);
        tablet->ChaosData()->ReplicationRound = round + 1;

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Write pulled rows committed (TabletId: %v, TransactionId: %v, ReplicationProgress: %v, ReplicationRowIndexes: %v, NewReplicationRound: %v)",
            tabletId,
            transaction->GetId(),
            static_cast<NChaosClient::TReplicationProgress>(*progress),
            tablet->ChaosData()->CurrentReplicationRowIndexes,
            tablet->ChaosData()->ReplicationRound);
    }

    void HydraAbortWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Write pulled rows aborted (TabletId: %v, TransactionId: %v)",
            tabletId,
            transaction->GetId());
    }

    void HydraPrepareReplicateRows(TTransaction* transaction, TReqReplicateRows* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->GetReplicaInfoOrThrow(replicaId);

        if (replicaInfo->GetState() != ETableReplicaState::Enabled) {
            THROW_ERROR_EXCEPTION("Replica %v is in %Qlv state",
                replicaId,
                replicaInfo->GetState());
        }

        if (replicaInfo->GetPreparedReplicationTransactionId()) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v since these are already "
                "prepared by transaction %v",
                transaction->GetId(),
                replicaId,
                tabletId,
                replicaInfo->GetPreparedReplicationTransactionId());
        }

        auto newReplicationRowIndex = request->new_replication_row_index();
        auto newReplicationTimestamp = request->new_replication_timestamp();

        if (request->has_prev_replication_row_index()) {
            auto prevReplicationRowIndex = request->prev_replication_row_index();
            if (replicaInfo->GetCurrentReplicationRowIndex() != prevReplicationRowIndex) {
                THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v due to current replication row index "
                    "mismatch: %v != %v",
                    transaction->GetId(),
                    replicaId,
                    tabletId,
                    replicaInfo->GetCurrentReplicationRowIndex(),
                    prevReplicationRowIndex);
            }
            YT_VERIFY(newReplicationRowIndex >= prevReplicationRowIndex);
        }

        if (newReplicationRowIndex < replicaInfo->GetCurrentReplicationRowIndex()) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v since current replication row index "
                "is already too high: %v > %v",
                transaction->GetId(),
                replicaId,
                tabletId,
                replicaInfo->GetCurrentReplicationRowIndex(),
                newReplicationRowIndex);
        }

        YT_VERIFY(newReplicationRowIndex <= tablet->GetTotalRowCount());
        YT_VERIFY(replicaInfo->GetPreparedReplicationRowIndex() == -1);

        replicaInfo->SetPreparedReplicationRowIndex(newReplicationRowIndex);
        replicaInfo->SetPreparedReplicationTransactionId(transaction->GetId());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows prepared (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %llx -> %llx)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            newReplicationRowIndex,
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            newReplicationTimestamp);

    }

    void HydraCommitReplicateRows(TTransaction* transaction, TReqReplicateRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        // COMPAT(babenko)
        if (request->has_prev_replication_row_index()) {
            YT_VERIFY(replicaInfo->GetCurrentReplicationRowIndex() == request->prev_replication_row_index());
        }
        YT_VERIFY(replicaInfo->GetPreparedReplicationRowIndex() == request->new_replication_row_index());
        YT_VERIFY(replicaInfo->GetPreparedReplicationTransactionId() == transaction->GetId());
        replicaInfo->SetPreparedReplicationRowIndex(-1);
        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        auto prevCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
        auto prevCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();

        auto newCurrentReplicationRowIndex = request->new_replication_row_index();
        auto newCurrentReplicationTimestamp = request->new_replication_timestamp();

        if (newCurrentReplicationRowIndex < prevCurrentReplicationRowIndex) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "CurrentReplicationIndex went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationRowIndex: %v -> %v)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationRowIndex,
                newCurrentReplicationRowIndex);
            newCurrentReplicationRowIndex = prevCurrentReplicationRowIndex;
        }
        if (newCurrentReplicationTimestamp < prevCurrentReplicationTimestamp) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "CurrentReplicationTimestamp went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationTimestamp: %llx -> %llx)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationTimestamp,
                newCurrentReplicationTimestamp);
            newCurrentReplicationTimestamp = prevCurrentReplicationTimestamp;
        }

        replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);
        replicaInfo->RecomputeReplicaStatus();

        AdvanceReplicatedTrimmedRowCount(tablet, transaction);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows committed (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, CurrentReplicationTimestamp: %llx -> %llx, TrimmedRowCount: %v -> %v, TotalRowCount: %v)",
            tabletId,
            replicaId,
            transaction->GetId(),
            prevCurrentReplicationRowIndex,
            replicaInfo->GetCurrentReplicationRowIndex(),
            prevCurrentReplicationTimestamp,
            replicaInfo->GetCurrentReplicationTimestamp(),
            prevTrimmedRowCount,
            tablet->GetTrimmedRowCount(),
            tablet->GetTotalRowCount());
    }

    void HydraAbortReplicateRows(TTransaction* transaction, TReqReplicateRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        if (transaction->GetId() != replicaInfo->GetPreparedReplicationTransactionId()) {
            return;
        }

        replicaInfo->SetPreparedReplicationRowIndex(-1);
        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows aborted (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %llx -> %llx)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            request->new_replication_row_index(),
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            request->new_replication_timestamp());
    }

    void HydraDecommissionTabletCell(TReqDecommissionTabletCellOnNode* /*request*/)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet cell is decommissioning");

        CellLifeStage_ = ETabletCellLifeStage::DecommissioningOnNode;
        Slot_->GetTransactionManager()->Decommission();
        Slot_->GetTransactionSupervisor()->Decommission();
    }

    void OnCheckTabletCellDecommission()
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        if (Slot_->GetDynamicOptions()->SuppressTabletCellDecommission.value_or(false)) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Checking if tablet cell is decommissioned "
            "(LifeStage: %v, TabletMapEmpty: %v, TransactionManagerDecommissined: %v, TransactionSupervisorDecommissioned: %v)",
            CellLifeStage_,
            TabletMap_.empty(),
            Slot_->GetTransactionManager()->IsDecommissioned(),
            Slot_->GetTransactionSupervisor()->IsDecommissioned());

        if (!TabletMap_.empty()) {
            return;
        }

        if (!Slot_->GetTransactionManager()->IsDecommissioned()) {
            return;
        }

        if (!Slot_->GetTransactionSupervisor()->IsDecommissioned()) {
            return;
        }

        CreateMutation(Slot_->GetHydraManager(), TReqOnTabletCellDecommissioned())
            ->CommitAndLog(Logger);
    }

    void HydraOnTabletCellDecommissioned(TReqOnTabletCellDecommissioned* /*request*/)
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet cell decommissioned");

        CellLifeStage_ = ETabletCellLifeStage::Decommissioned;

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = Slot_->GetMasterMailbox();
        TRspDecommissionTabletCellOnNode response;
        ToProto(response.mutable_cell_id(), Slot_->GetCellId());
        hiveManager->PostMessage(mailbox, response);
    }


    template <class TRequest>
    void PopulateDynamicStoreIdPool(TTablet* tablet, const TRequest* request)
    {
        for (const auto& protoStoreId : request->dynamic_store_ids()) {
            auto storeId = FromProto<TDynamicStoreId>(protoStoreId);
            tablet->PushDynamicStoreIdToPool(storeId);
        }
    }

    void AllocateDynamicStore(TTablet* tablet)
    {
        TReqAllocateDynamicStore req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());
        tablet->SetDynamicStoreIdRequested(true);
        PostMasterMessage(tablet->GetId(), req);
    }

    void HydraOnDynamicStoreAllocated(TRspAllocateDynamicStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounted &&
            state != ETabletState::UnmountFlushPending &&
            state != ETabletState::UnmountFlushing &&
            state != ETabletState::FreezeFlushPending &&
            state != ETabletState::FreezeFlushing)
        {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Dynamic store id sent to a tablet in a wrong state, ignored (%v, State: %v)",
                tablet->GetLoggingTag(),
                state);
            return;
        }

        auto dynamicStoreId = FromProto<TDynamicStoreId>(request->dynamic_store_id());
        tablet->PushDynamicStoreIdToPool(dynamicStoreId);
        tablet->SetDynamicStoreIdRequested(false);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store allocated for a tablet (%v, DynamicStoreId: %v)",
            tablet->GetLoggingTag(),
            dynamicStoreId);
    }

    void SetStoreOrphaned(TTablet* tablet, IStorePtr store)
    {
        if (store->GetStoreState() == EStoreState::Orphaned) {
            return;
        }

        store->SetStoreState(EStoreState::Orphaned);

        if (!store->IsDynamic()) {
            return;
        }

        auto dynamicStore = store->AsDynamic();
        auto lockCount = dynamicStore->GetLockCount();
        if (lockCount > 0) {
            YT_VERIFY(OrphanedStores_.insert(dynamicStore).second);
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Dynamic memory store is orphaned and will be kept "
                "(StoreId: %v, TabletId: %v, LockCount: %v)",
                store->GetId(),
                tablet->GetId(),
                lockCount);
        }
    }

    bool ValidateRowRef(const TSortedDynamicRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        return store->GetStoreState() != EStoreState::Orphaned;
    }

    bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& rowRef) override
    {
        auto* store = rowRef.Store;
        if (store->GetStoreState() != EStoreState::Orphaned) {
            return true;
        }

        auto lockCount = store->Unlock();
        if (lockCount == 0) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Store unlocked and will be dropped (StoreId: %v)",
                store->GetId());
            YT_VERIFY(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }


    void SetTabletOrphaned(std::unique_ptr<TTablet> tabletHolder)
    {
        auto id = tabletHolder->GetId();
        tabletHolder->SetState(ETabletState::Orphaned);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet is orphaned and will be kept (TabletId: %v, LockCount: %v)",
            id,
            tabletHolder->GetTabletLockCount());
        YT_VERIFY(OrphanedTablets_.emplace(id, std::move(tabletHolder)).second);
    }

    void OnTabletUnlocked(TTablet* tablet) override
    {
        CheckIfTabletFullyUnlocked(tablet);
        if (tablet->GetState() == ETabletState::Orphaned && tablet->GetTabletLockCount() == 0) {
            auto id = tablet->GetId();
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unlocked and will be dropped (TabletId: %v)",
                id);
            YT_VERIFY(OrphanedTablets_.erase(id) == 1);
        }
    }

    void OnTabletRowUnlocked(TTablet* tablet) override
    {
        CheckIfTabletFullyUnlocked(tablet);
    }

    void CheckIfTabletFullyUnlocked(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        if (tablet->GetTabletLockCount() > 0) {
            return;
        }

        if (tablet->GetStoreManager()->HasActiveLocks()) {
            return;
        }

        NTracing::TNullTraceContextGuard guard;

        const auto& lockManager = tablet->GetLockManager();
        if (lockManager->HasUnconfirmedTransactions()) {
            TReqReportTabletLocked request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            Slot_->CommitTabletMutation(request);
        }

        auto state = tablet->GetState();
        if (state != ETabletState::UnmountWaitingForLocks && state != ETabletState::FreezeWaitingForLocks) {
            return;
        }

        ETabletState newTransientState;
        ETabletState newPersistentState;
        switch (state) {
            case ETabletState::UnmountWaitingForLocks:
                newTransientState = ETabletState::UnmountFlushPending;
                newPersistentState = ETabletState::UnmountFlushing;
                break;
            case ETabletState::FreezeWaitingForLocks:
                newTransientState = ETabletState::FreezeFlushPending;
                newPersistentState = ETabletState::FreezeFlushing;
                break;
            default:
                YT_ABORT();
        }
        tablet->SetState(newTransientState);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "All tablet locks released (%v, NewState: %v)",
            tablet->GetLoggingTag(),
            newTransientState);

        {
            TReqSetTabletState request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            request.set_state(static_cast<int>(newPersistentState));
            Slot_->CommitTabletMutation(request);
        }
    }

    void CheckIfTabletFullyFlushed(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::UnmountFlushing && state != ETabletState::FreezeFlushing) {
            return;
        }

        if (tablet->GetStoreManager()->HasUnflushedStores()) {
            return;
        }

        ETabletState newTransientState;
        ETabletState newPersistentState;
        switch (state) {
            case ETabletState::UnmountFlushing:
                newTransientState = ETabletState::UnmountPending;
                newPersistentState = ETabletState::Unmounted;
                break;
            case ETabletState::FreezeFlushing:
                newTransientState = ETabletState::FreezePending;
                newPersistentState = ETabletState::Frozen;
                break;
            default:
                YT_ABORT();
        }
        tablet->SetState(newTransientState);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "All tablet stores flushed (%v, NewState: %v)",
            tablet->GetLoggingTag(),
            newTransientState);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_state(static_cast<int>(newPersistentState));
        Slot_->CommitTabletMutation(request);
    }

    void PostMasterMessage(TTabletId tabletId, const ::google::protobuf::MessageLite& message)
    {
        // Used in tests only. NB: synchronous sleep is required since we don't expect
        // context switches here.
        if (auto sleepDuration = Config_->SleepBeforePostToMaster) {
            Sleep(*sleepDuration);
        }

        Slot_->PostMasterMessage(tabletId, message);
    }

    void StartTabletEpoch(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        storeManager->StartEpoch(Slot_);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->RegisterTabletSnapshot(Slot_, tablet);

        for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            StartTableReplicaEpoch(tablet, &replicaInfo);
        }

        if (tablet->GetChaosAgent()) {
            tablet->GetChaosAgent()->Enable();
            tablet->GetTablePuller()->Enable();
        }
    }

    void StopTabletEpoch(TTablet* tablet)
    {
        if (const auto& storeManager = tablet->GetStoreManager()) {
            // Store Manager could be null if snapshot loading is aborted.
            storeManager->StopEpoch();
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->UnregisterTabletSnapshot(Slot_, tablet);

        for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            StopTableReplicaEpoch(&replicaInfo);
        }

        tablet->SetInFlightUserMutationCount(0);
        tablet->SetInFlightReplicatorMutationCount(0);

        if (tablet->GetChaosAgent()) {
            tablet->GetChaosAgent()->Disable();
            tablet->GetTablePuller()->Disable();
        }
    }


    void StartTableReplicaEpoch(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_VERIFY(!replicaInfo->GetReplicator());

        if (IsLeader()) {
            auto replicator = New<TTableReplicator>(
                Config_,
                tablet,
                replicaInfo,
                Bootstrap_->GetMasterClient()->GetNativeConnection(),
                Slot_,
                Bootstrap_->GetTabletSnapshotStore(),
                Bootstrap_->GetHintManager(),
                CreateSerializedInvoker(Bootstrap_->GetTableReplicatorPoolInvoker()),
                EWorkloadCategory::SystemTabletReplication,
                Bootstrap_->GetOutThrottler(EWorkloadCategory::SystemTabletReplication));
            replicaInfo->SetReplicator(replicator);

            if (replicaInfo->GetState() == ETableReplicaState::Enabled) {
                replicator->Enable();
            }
        }
    }

    void StopTableReplicaEpoch(TTableReplicaInfo* replicaInfo)
    {
        if (!replicaInfo->GetReplicator()) {
            return;
        }
        replicaInfo->GetReplicator()->Disable();
        replicaInfo->SetReplicator(nullptr);
    }


    void SetBackingStore(TTablet* tablet, const IChunkStorePtr& store, const IDynamicStorePtr& backingStore)
    {
        store->SetBackingStore(backingStore);
        YT_LOG_DEBUG("Backing store set (%v, StoreId: %v, BackingStoreId: %v, BackingDynamicMemoryUsage: %v)",
            tablet->GetLoggingTag(),
            store->GetId(),
            backingStore->GetId(),
            backingStore->GetDynamicMemoryUsage());
        tablet->GetStructuredLogger()->OnBackingStoreSet(store, backingStore);

        TDelayedExecutor::Submit(
            // NB: Submit the callback via the regular automaton invoker, not the epoch one since
            // we need the store to be released even if the epoch ends.
            BIND(&TTabletManager::TImpl::ReleaseBackingStoreWeak, MakeWeak(this), MakeWeak(store))
                .Via(Slot_->GetAutomatonInvoker()),
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime);
    }

    void ReleaseBackingStoreWeak(const TWeakPtr<IChunkStore>& storeWeak)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto store = storeWeak.Lock()) {
            ReleaseBackingStore(store);
        }
    }

    void BuildTabletOrchidYson(TTablet* tablet, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("table_id").Value(tablet->GetTableId())
                .Item("state").Value(tablet->GetState())
                .Item("hash_table_size").Value(tablet->GetHashTableSize())
                .Item("overlapping_store_count").Value(tablet->GetOverlappingStoreCount())
                .Item("retained_timestamp").Value(tablet->GetRetainedTimestamp())
                .Item("in_flight_user_mutation_count").Value(tablet->GetInFlightUserMutationCount())
                .Item("in_flight_replicator_mutation_count").Value(tablet->GetInFlightReplicatorMutationCount())
                .Item("pending_user_write_record_count").Value(tablet->GetPendingUserWriteRecordCount())
                .Item("pending_replicator_write_record_count").Value(tablet->GetPendingReplicatorWriteRecordCount())
                .Item("replication_card").Value(tablet->ChaosData()->ReplicationCard)
                .Item("replication_progress").Value(tablet->RuntimeData()->ReplicationProgress.Load())
                .Item("write_mode").Value(tablet->RuntimeData()->WriteMode.load())
                .Do([tablet] (auto fluent) {
                    BuildTableSettingsOrchidYson(tablet->GetSettings(), fluent);
                })
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("pivot_key").Value(tablet->GetPivotKey())
                        .Item("next_pivot_key").Value(tablet->GetNextPivotKey())
                        .Item("eden").DoMap(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), tablet->GetEden()))
                        .Item("partitions").DoListFor(
                            tablet->PartitionList(), [&] (auto fluent, const std::unique_ptr<TPartition>& partition) {
                                fluent
                                    .Item()
                                    .DoMap(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), partition.get()));
                            });
                })
                .DoIf(tablet->IsPhysicallyOrdered(), [&] (auto fluent) {
                    fluent
                        .Item("stores").DoMapFor(
                            tablet->StoreIdMap(),
                            [&] (auto fluent, const auto& pair) {
                                const auto& [storeId, store] = pair;
                                fluent
                                    .Item(ToString(storeId))
                                    .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
                            })
                        .Item("total_row_count").Value(tablet->GetTotalRowCount())
                        .Item("trimmed_row_count").Value(tablet->GetTrimmedRowCount());
                })
                .Item("hunk_chunks").DoMapFor(tablet->HunkChunkMap(), [&] (auto fluent, const auto& pair) {
                    const auto& [chunkId, hunkChunk] = pair;
                    fluent
                        .Item(ToString(chunkId))
                        .Do(BIND(&TImpl::BuildHunkChunkOrchidYson, Unretained(this), hunkChunk));
                })
                .DoIf(tablet->IsReplicated(), [&] (auto fluent) {
                    fluent
                        .Item("replicas").DoMapFor(
                            tablet->Replicas(),
                            [&] (auto fluent, const auto& pair) {
                                const auto& [replicaId, replica] = pair;
                                fluent
                                    .Item(ToString(replicaId))
                                    .Do(BIND(&TImpl::BuildReplicaOrchidYson, Unretained(this), replica));
                            });
                })
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("dynamic_table_locks").DoMap(
                            BIND(&TLockManager::BuildOrchidYson, tablet->GetLockManager()));
                })
                .Item("errors").DoListFor(
                    TEnumTraits<ETabletBackgroundActivity>::GetDomainValues(),
                    [&] (auto fluent, auto activity) {
                        auto error = tablet->RuntimeData()->Errors[activity].Load();
                        if (!error.IsOK()) {
                            fluent
                                .Item().Value(error);
                        }
                    })
                .Item("replication_errors").DoMapFor(
                    tablet->Replicas(),
                    [&] (auto fluent, const auto& replica) {
                        auto replicaId = replica.first;
                        auto error = replica.second.GetError();
                        if (!error.IsOK()) {
                            fluent
                                .Item(ToString(replicaId)).Value(error);
                        }
                    })
                .DoIf(tablet->GetSettings().MountConfig->EnableDynamicStoreRead, [&] (auto fluent) {
                    fluent
                        .Item("dynamic_store_id_pool")
                            .BeginAttributes()
                                .Item("opaque").Value(true)
                            .EndAttributes()
                            .DoListFor(
                                tablet->DynamicStoreIdPool(),
                                [&] (auto fluent, auto dynamicStoreId) {
                                    fluent
                                        .Item().Value(dynamicStoreId);
                                });
                })
                .Item("backup_stage").Value(tablet->GetBackupStage())
                .Item("backup_checkpoint_timestamp").Value(tablet->GetBackupCheckpointTimestamp())
            .EndMap();
    }

    void BuildPartitionOrchidYson(TPartition* partition, TFluentMap fluent)
    {
        fluent
            .Item("id").Value(partition->GetId())
            .Item("state").Value(partition->GetState())
            .Item("pivot_key").Value(partition->GetPivotKey())
            .Item("next_pivot_key").Value(partition->GetNextPivotKey())
            .Item("sample_key_count").Value(partition->GetSampleKeys()->Keys.Size())
            .Item("sampling_time").Value(partition->GetSamplingTime())
            .Item("sampling_request_time").Value(partition->GetSamplingRequestTime())
            .Item("compaction_time").Value(partition->GetCompactionTime())
            .Item("allowed_split_time").Value(partition->GetAllowedSplitTime())
            .Item("uncompressed_data_size").Value(partition->GetUncompressedDataSize())
            .Item("compressed_data_size").Value(partition->GetCompressedDataSize())
            .Item("unmerged_row_count").Value(partition->GetUnmergedRowCount())
            .Item("stores").DoMapFor(partition->Stores(), [&] (auto fluent, const IStorePtr& store) {
                fluent
                    .Item(ToString(store->GetId()))
                    .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
            })
            .DoIf(partition->IsImmediateSplitRequested(), [&] (auto fluent) {
                fluent
                    .Item("immediate_split_keys").DoListFor(
                        partition->PivotKeysForImmediateSplit(),
                        [&] (auto fluent, const TLegacyOwningKey& key)
                    {
                        fluent
                            .Item().Value(key);
                    });

            });
    }

    void BuildStoreOrchidYson(const IStorePtr& store, TFluentAny fluent)
    {
        fluent
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Do(BIND(&IStore::BuildOrchidYson, store))
            .EndMap();
    }

    void BuildHunkChunkOrchidYson(const THunkChunkPtr& hunkChunk, TFluentAny fluent)
    {
        fluent
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Item("hunk_count").Value(hunkChunk->GetHunkCount())
                .Item("total_hunk_length").Value(hunkChunk->GetTotalHunkLength())
                .Item("referenced_hunk_count").Value(hunkChunk->GetReferencedHunkCount())
                .Item("referenced_total_hunk_length").Value(hunkChunk->GetReferencedTotalHunkLength())
                .Item("store_ref_count").Value(hunkChunk->GetStoreRefCount())
                .Item("prepared_store_ref_count").Value(hunkChunk->GetPreparedStoreRefCount())
                .Item("dangling").Value(hunkChunk->IsDangling())
            .EndMap();
    }

    void BuildReplicaOrchidYson(const TTableReplicaInfo& replica, TFluentAny fluent)
    {
        fluent
            .BeginMap()
                .Item("cluster_name").Value(replica.GetClusterName())
                .Item("replica_path").Value(replica.GetReplicaPath())
                .Item("state").Value(replica.GetState())
                .Item("mode").Value(replica.GetMode())
                .Item("atomicity").Value(replica.GetAtomicity())
                .Item("preserve_timestamps").Value(replica.GetPreserveTimestamps())
                .Item("start_replication_timestamp").Value(replica.GetStartReplicationTimestamp())
                .Item("current_replication_row_index").Value(replica.GetCurrentReplicationRowIndex())
                .Item("current_replication_timestamp").Value(replica.GetCurrentReplicationTimestamp())
                .Item("prepared_replication_transaction").Value(replica.GetPreparedReplicationTransactionId())
                .Item("prepared_replication_row_index").Value(replica.GetPreparedReplicationRowIndex())
            .EndMap();
    }


    void ValidateMemoryLimit(const std::optional<TString>& poolTag) override
    {
        if (Bootstrap_->GetSlotManager()->IsOutOfMemory(poolTag)) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Node is out of tablet memory, all writes disabled");
        }
    }

    void UpdateTabletSnapshot(TTablet* tablet, std::optional<TLockManagerEpoch> epoch = std::nullopt)
    {
        if (!IsRecovery()) {
            const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
            snapshotStore->RegisterTabletSnapshot(Slot_, tablet, epoch);
        }
    }


    template <class TRequest>
    NTabletNode::TTableSettings DeserializeTableSettings(TRequest* request, TTabletId tabletId)
    {
        // COMPAT(babenko)
        if (request->has_table_settings()) {
            const auto& tableSettings = request->table_settings();
            return {
                .MountConfig = DeserializeTableMountConfig(TYsonString(tableSettings.mount_config()), tabletId),
                .StoreReaderConfig = DeserializeTabletStoreReaderConfig(TYsonString(tableSettings.store_reader_config()), tabletId),
                .HunkReaderConfig = DeserializeTabletHunkReaderConfig(TYsonString(tableSettings.hunk_reader_config()), tabletId),
                .StoreWriterConfig = DeserializeTabletStoreWriterConfig(TYsonString(tableSettings.store_writer_config()), tabletId),
                .StoreWriterOptions = DeserializeTabletStoreWriterOptions(TYsonString(tableSettings.store_writer_options()), tabletId),
                .HunkWriterConfig = DeserializeTabletHunkWriterConfig(TYsonString(tableSettings.hunk_writer_config()), tabletId),
                .HunkWriterOptions = DeserializeTabletHunkWriterOptions(TYsonString(tableSettings.hunk_writer_options()), tabletId)
            };
        } else {
            auto mountConfig = DeserializeTableMountConfig(TYsonString(request->mount_config()), tabletId);
            auto storeReaderConfig = DeserializeTabletStoreReaderConfig(TYsonString(request->store_reader_config()), tabletId);
            auto storeWriterConfig = DeserializeTabletStoreWriterConfig(TYsonString(request->store_writer_config()), tabletId);
            auto storeWriterOptions = DeserializeTabletStoreWriterOptions(TYsonString(request->store_writer_options()), tabletId);
            return {
                .MountConfig = mountConfig,
                .StoreReaderConfig = storeReaderConfig,
                .HunkReaderConfig = New<TTabletHunkReaderConfig>(),
                .StoreWriterConfig = storeWriterConfig,
                .StoreWriterOptions = storeWriterOptions,
                .HunkWriterConfig = New<TTabletHunkWriterConfig>(),
                .HunkWriterOptions = TTablet::CreateFallbackHunkWriterOptions(storeWriterOptions)
            };
        }
    }

    TTableMountConfigPtr DeserializeTableMountConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTableMountConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing tablet mount config (TabletId: %v)",
                 tabletId);
            return New<TTableMountConfig>();
        }
    }

    TTabletStoreReaderConfigPtr DeserializeTabletStoreReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store reader config (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreReaderConfig>();
        }
    }

    TTabletHunkReaderConfigPtr DeserializeTabletHunkReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk reader config (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkReaderConfig>();
        }
    }

    TTabletStoreWriterConfigPtr DeserializeTabletStoreWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store writer config (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreWriterConfig>();
        }
    }

    TTabletStoreWriterOptionsPtr DeserializeTabletStoreWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store writer options (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreWriterOptions>();
        }
    }

    TTabletHunkWriterConfigPtr DeserializeTabletHunkWriterConfig(const TYsonString& str, const TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk writer config (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkWriterConfig>();
        }
    }

    TTabletHunkWriterOptionsPtr DeserializeTabletHunkWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk writer options (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkWriterOptions>();
        }
    }



    IStoreManagerPtr CreateStoreManager(TTablet* tablet)
    {
        if (tablet->IsPhysicallyLog()) {
            return DoCreateStoreManager<TReplicatedStoreManager>(tablet);
        } else {
            if (tablet->IsPhysicallySorted()) {
                return DoCreateStoreManager<TSortedStoreManager>(tablet);
            } else {
                return DoCreateStoreManager<TOrderedStoreManager>(tablet);
            }
        }
    }

    template <class TImpl>
    IStoreManagerPtr DoCreateStoreManager(TTablet* tablet)
    {
        return New<TImpl>(
            Config_,
            tablet,
            &TabletContext_,
            Slot_->GetHydraManager(),
            Bootstrap_->GetInMemoryManager(),
            Bootstrap_->GetMasterClient());
    }


    IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const TAddStoreDescriptor* descriptor)
    {
        auto store = DoCreateStore(tablet, type, storeId, descriptor);
        store->SetMemoryTracker(Bootstrap_->GetMemoryUsageTracker());
        return store;
    }

    TIntrusivePtr<TStoreBase> DoCreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const TAddStoreDescriptor* descriptor)
    {
        switch (type) {
            case EStoreType::SortedChunk: {
                NChunkClient::TLegacyReadRange readRange;
                TChunkId chunkId;
                auto overrideTimestamp = NullTimestamp;
                auto maxClipTimestamp = NullTimestamp;

                if (descriptor) {
                    if (descriptor->has_chunk_view_descriptor()) {
                        const auto& chunkViewDescriptor = descriptor->chunk_view_descriptor();
                        if (chunkViewDescriptor.has_read_range()) {
                            readRange = FromProto<NChunkClient::TLegacyReadRange>(chunkViewDescriptor.read_range());
                        }
                        if (chunkViewDescriptor.has_override_timestamp()) {
                            overrideTimestamp = static_cast<TTimestamp>(chunkViewDescriptor.override_timestamp());
                        }
                        if (chunkViewDescriptor.has_max_clip_timestamp()) {
                            maxClipTimestamp = static_cast<TTimestamp>(chunkViewDescriptor.max_clip_timestamp());
                        }
                        chunkId = FromProto<TChunkId>(chunkViewDescriptor.underlying_chunk_id());
                    } else {
                        chunkId = storeId;
                    }
                } else {
                    YT_VERIFY(IsRecovery());
                }

                return New<TSortedChunkStore>(
                    Bootstrap_,
                    Config_,
                    storeId,
                    chunkId,
                    readRange,
                    overrideTimestamp,
                    maxClipTimestamp,
                    tablet,
                    descriptor,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetVersionedChunkMetaManager(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetLocalDescriptor());
            }

            case EStoreType::SortedDynamic:
                return New<TSortedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            case EStoreType::OrderedChunk: {
                if (!IsRecovery()) {
                    YT_VERIFY(descriptor);
                    YT_VERIFY(!descriptor->has_chunk_view_descriptor());
                }

                return New<TOrderedChunkStore>(
                    Bootstrap_,
                    Config_,
                    storeId,
                    tablet,
                    descriptor,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetVersionedChunkMetaManager(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetLocalDescriptor());
            }

            case EStoreType::OrderedDynamic:
                return New<TOrderedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            default:
                YT_ABORT();
        }
    }


    THunkChunkPtr CreateHunkChunk(
        TTablet* /*tablet*/,
        TChunkId chunkId,
        const TAddHunkChunkDescriptor* descriptor)
    {
        return New<THunkChunk>(
            chunkId,
            descriptor);
    }


    TTableReplicaInfo* AddTableReplica(TTablet* tablet, const TTableReplicaDescriptor& descriptor)
    {
        auto replicaId = FromProto<TTableReplicaId>(descriptor.replica_id());
        auto& replicas = tablet->Replicas();
        if (replicas.find(replicaId) != replicas.end()) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Requested to add an already existing table replica (TabletId: %v, ReplicaId: %v)",
                tablet->GetId(),
                replicaId);
            return nullptr;
        }

        auto [replicaIt, replicaInserted] = replicas.emplace(replicaId, TTableReplicaInfo(tablet, replicaId));
        YT_VERIFY(replicaInserted);
        auto& replicaInfo = replicaIt->second;

        replicaInfo.SetClusterName(descriptor.cluster_name());
        replicaInfo.SetReplicaPath(descriptor.replica_path());
        replicaInfo.SetStartReplicationTimestamp(descriptor.start_replication_timestamp());
        replicaInfo.SetState(ETableReplicaState::Disabled);
        replicaInfo.SetMode(ETableReplicaMode(descriptor.mode()));
        if (descriptor.has_atomicity()) {
            replicaInfo.SetAtomicity(NTransactionClient::EAtomicity(descriptor.atomicity()));
        }
        if (descriptor.has_preserve_timestamps()) {
            replicaInfo.SetPreserveTimestamps(descriptor.preserve_timestamps());
        }
        replicaInfo.MergeFromStatistics(descriptor.statistics());
        replicaInfo.RecomputeReplicaStatus();

        tablet->UpdateReplicaCounters();
        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica added (%v, ReplicaId: %v, ClusterName: %v, ReplicaPath: %v, "
            "Mode: %v, StartReplicationTimestamp: %llx, CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tablet->GetLoggingTag(),
            replicaId,
            replicaInfo.GetClusterName(),
            replicaInfo.GetReplicaPath(),
            replicaInfo.GetMode(),
            replicaInfo.GetStartReplicationTimestamp(),
            replicaInfo.GetCurrentReplicationRowIndex(),
            replicaInfo.GetCurrentReplicationTimestamp());

        return &replicaInfo;
    }

    void RemoveTableReplica(TTablet* tablet, TTableReplicaId replicaId)
    {
        auto& replicas = tablet->Replicas();
        auto it = replicas.find(replicaId);
        if (it == replicas.end()) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Requested to remove a non-existing table replica (TabletId: %v, ReplicaId: %v)",
                tablet->GetId(),
                replicaId);
            return;
        }

        auto& replicaInfo = it->second;

        if (!IsRecovery()) {
            StopTableReplicaEpoch(&replicaInfo);
        }

        replicas.erase(it);

        AdvanceReplicatedTrimmedRowCount(tablet, nullptr);
        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica removed (%v, ReplicaId: %v)",
            tablet->GetLoggingTag(),
            replicaId);
    }


    void EnableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica enabled (%v, ReplicaId: %v)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId());

        replicaInfo->SetState(ETableReplicaState::Enabled);

        if (IsLeader()) {
            replicaInfo->GetReplicator()->Enable();
        }

        {
            TRspEnableTableReplica response;
            ToProto(response.mutable_tablet_id(), tablet->GetId());
            ToProto(response.mutable_replica_id(), replicaInfo->GetId());
            response.set_mount_revision(tablet->GetMountRevision());
            PostMasterMessage(tablet->GetId(), response);
        }
    }

    void DisableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica disabled (%v, ReplicaId: %v, "
            "CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            replicaInfo->GetCurrentReplicationTimestamp());

        replicaInfo->SetState(ETableReplicaState::Disabled);
        replicaInfo->SetError(TError());

        if (IsLeader()) {
            replicaInfo->GetReplicator()->Disable();
        }

        PostTableReplicaStatistics(tablet, *replicaInfo);

        {
            TRspDisableTableReplica response;
            ToProto(response.mutable_tablet_id(), tablet->GetId());
            ToProto(response.mutable_replica_id(), replicaInfo->GetId());
            response.set_mount_revision(tablet->GetMountRevision());
            PostMasterMessage(tablet->GetId(), response);
        }
    }

    void PostTableReplicaStatistics(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        TReqUpdateTableReplicaStatistics request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_replica_id(), replicaInfo.GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        replicaInfo.PopulateStatistics(request.mutable_statistics());
        PostMasterMessage(tablet->GetId(), request);
    }


    void AddChaosAgent(TTablet* tablet, TReplicationCardId replicationCardId)
    {
        tablet->SetChaosAgent(CreateChaosAgent(
            tablet,
            Slot_,
            replicationCardId,
            Bootstrap_->GetMasterClient()->GetNativeConnection()));
        tablet->SetTablePuller(CreateTablePuller(
            Config_,
            tablet,
            Bootstrap_->GetMasterClient()->GetNativeConnection(),
            Slot_,
            Bootstrap_->GetTabletSnapshotStore(),
            CreateSerializedInvoker(Bootstrap_->GetTableReplicatorPoolInvoker()),
            Bootstrap_->GetInThrottler(EWorkloadCategory::SystemTabletReplication)));
    }


    void UpdateTrimmedRowCount(TTablet* tablet, i64 trimmedRowCount)
    {
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();
        if (trimmedRowCount <= prevTrimmedRowCount) {
            return;
        }
        tablet->SetTrimmedRowCount(trimmedRowCount);

        {
            TReqUpdateTabletTrimmedRowCount masterRequest;
            ToProto(masterRequest.mutable_tablet_id(), tablet->GetId());
            masterRequest.set_mount_revision(tablet->GetMountRevision());
            masterRequest.set_trimmed_row_count(trimmedRowCount);
            PostMasterMessage(tablet->GetId(), masterRequest);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows trimmed (TabletId: %v, TrimmedRowCount: %v -> %v)",
            tablet->GetId(),
            prevTrimmedRowCount,
            trimmedRowCount);
    }

    void AdvanceReplicatedTrimmedRowCount(TTablet* tablet, TTransaction* transaction) override
    {
        YT_VERIFY(tablet->IsReplicated());

        if (tablet->Replicas().empty()) {
            return;
        }

        auto minReplicationRowIndex = std::numeric_limits<i64>::max();
        for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            minReplicationRowIndex = std::min(minReplicationRowIndex, replicaInfo.GetCurrentReplicationRowIndex());
        }

        const auto& storeRowIndexMap = tablet->StoreRowIndexMap();
        if (storeRowIndexMap.empty()) {
            return;
        }

        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto retentionDeadline = transaction
            ? TimestampToInstant(transaction->GetCommitTimestamp()).first - mountConfig->MinReplicationLogTtl
            : TInstant::Max();
        auto it = storeRowIndexMap.find(tablet->GetTrimmedRowCount());
        while (it != storeRowIndexMap.end()) {
            const auto& store = it->second;
            if (store->IsDynamic()) {
                break;
            }
            if (minReplicationRowIndex < store->GetStartingRowIndex() + store->GetRowCount()) {
                break;
            }
            if (TimestampToInstant(store->GetMaxTimestamp()).first > retentionDeadline) {
                break;
            }
            ++it;
        }

        i64 trimmedRowCount;
        if (it == storeRowIndexMap.end()) {
            // Looks like a full trim.
            // Typically we have a sentinel dynamic store at the end but during unmount this one may be missing.
            YT_VERIFY(!storeRowIndexMap.empty());
            const auto& lastStore = storeRowIndexMap.rbegin()->second;
            YT_VERIFY(minReplicationRowIndex == lastStore->GetStartingRowIndex() + lastStore->GetRowCount());
            trimmedRowCount = minReplicationRowIndex;
        } else {
            trimmedRowCount = it->second->GetStartingRowIndex();
        }

        YT_VERIFY(tablet->GetTrimmedRowCount() <= trimmedRowCount);
        UpdateTrimmedRowCount(tablet, trimmedRowCount);
    }


    void OnStoresUpdateCommitSemaphoreAcquired(
        TTablet* tablet,
        const ITransactionPtr& transaction,
        TPromise<void> promise,
        TAsyncSemaphoreGuard /*guard*/)
    {
        try {
            YT_LOG_DEBUG("Started committing tablet stores update transaction (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_DEBUG("Tablet stores update transaction committed (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            promise.Set();
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
    }


    TCellId GetCellId() const override
    {
        return Slot_->GetCellId();
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TabletMap_)

////////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    ITabletSlotPtr slot,
    IBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager() = default;

void TTabletManager::Initialize()
{
    Impl_->Initialize();
}

void TTabletManager::Finalize()
{
    Impl_->Finalize();
}

TTablet* TTabletManager::GetTabletOrThrow(TTabletId id)
{
    return Impl_->GetTabletOrThrow(id);
}

TFuture<void> TTabletManager::Trim(
    TTabletSnapshotPtr tabletSnapshot,
    i64 trimmedRowCount)
{
    return Impl_->Trim(
        std::move(tabletSnapshot),
        trimmedRowCount);
}

void TTabletManager::ScheduleStoreRotation(TTablet* tablet, EStoreRotationReason reason)
{
    Impl_->ScheduleStoreRotation(tablet, reason);
}

TFuture<void> TTabletManager::CommitTabletStoresUpdateTransaction(
    TTablet* tablet,
    const ITransactionPtr& transaction)
{
    return Impl_->CommitTabletStoresUpdateTransaction(tablet, transaction);
}

void TTabletManager::ReleaseBackingStore(const IChunkStorePtr& store)
{
    Impl_->ReleaseBackingStore(store);
}

IYPathServicePtr TTabletManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

ETabletCellLifeStage TTabletManager::GetTabletCellLifeStage() const
{
    return Impl_->GetTabletCellLifeStage();
}

ITabletCellWriteManagerHostPtr TTabletManager::GetTabletCellWriteManagerHost()
{
    return Impl_;
}

std::vector<TTabletMemoryStats> TTabletManager::GetMemoryStats()
{
    return Impl_->GetMemoryStats();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
