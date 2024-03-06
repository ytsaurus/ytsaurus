#include "tablet_manager.h"

#include "private.h"
#include "automaton.h"
#include "bootstrap.h"
#include "sorted_chunk_store.h"
#include "ordered_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "ordered_dynamic_store.h"
#include "hunk_chunk.h"
#include "replicated_store_manager.h"
#include "partition.h"
#include "slot_manager.h"
#include "sorted_store_manager.h"
#include "ordered_store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_cell_write_manager.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "table_config_manager.h"
#include "table_replicator.h"
#include "tablet_profiling.h"
#include "tablet_snapshot_store.h"
#include "table_puller.h"
#include "backup_manager.h"
#include "hunk_lock_manager.h"
#include "row_digest_fetcher.h"
#include "chunk_view_size_fetcher.h"
#include "smooth_movement_tracker.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>

#include <yt/yt/server/node/tablet_node/transaction_manager.pb.h>

#include <yt/yt/server/lib/hive/avenue_directory.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/tablet_client/proto/tablet_service.pb.h>
#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/iterator/zip.h>

#include <util/generic/cast.h>
#include <util/generic/algorithm.h>

#include <optional>

namespace NYT::NTabletNode {

using namespace NCompression;
using namespace NConcurrency;
using namespace NChaosClient;
using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NLeaseServer;
using namespace NClusterNode;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
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

class TTabletManager
    : public TTabletAutomatonPart
    , public virtual ITabletCellWriteManagerHost
    , public virtual ITabletWriteManagerHost
    , public virtual ISmoothMovementTrackerHost
    , public ITabletManager
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(TTablet*, const TTableReplicaInfo*), ReplicationTransactionFinished);
    DEFINE_SIGNAL_OVERRIDE(void(), EpochStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), EpochStopped);

public:
    TTabletManager(
        TTabletManagerConfigPtr config,
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot->GetCellId(),
            slot->GetSimpleHydraManager(),
            slot->GetAutomaton(),
            slot->GetAutomatonInvoker(),
            slot->GetMutationForwarder())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
        , Config_(config)
        , TabletContext_(this)
        , TabletMap_(TTabletMapTraits(this))
        , DecommissionCheckExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(),
            BIND(&TTabletManager::OnCheckTabletCellDecommission, MakeWeak(this)),
            Config_->TabletCellDecommissionCheckPeriod))
        , SuspensionCheckExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(),
            BIND(&TTabletManager::OnCheckTabletCellSuspension, MakeWeak(this)),
            Config_->TabletCellSuspensionCheckPeriod))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
        , BackupManager_(CreateBackupManager(
            Slot_,
            Bootstrap_))
        , RowDigestFetcher_(CreateRowDigestFetcher(
            slot->GetCellId(),
            Slot_->GetAutomatonInvoker(),
            Bootstrap_->GetDynamicConfigManager()->GetConfig()))
        , ChunkViewSizeFetcher_(CreateChunkViewSizeFetcher(
            slot->GetCellId(),
            Bootstrap_->GetNodeDirectory(),
            Slot_->GetAutomatonInvoker(),
            Bootstrap_->GetStorageHeavyInvoker(),
            Bootstrap_->GetClient(),
            Bootstrap_->GetConnection()->GetChunkReplicaCache(),
            Bootstrap_->GetDynamicConfigManager()->GetConfig()))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TTabletManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TTabletManager::LoadValues, Unretained(this)));
        RegisterLoader(
            "TabletManager.Async",
            BIND(&TTabletManager::LoadAsync, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TTabletManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TTabletManager::SaveValues, Unretained(this)));
        RegisterSaver(
            EAsyncSerializationPriority::Default,
            "TabletManager.Async",
            BIND(&TTabletManager::SaveAsync, Unretained(this)));

        RegisterMethod(BIND(&TTabletManager::HydraMountTablet, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraUnmountTablet, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraRemountTablet, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraUpdateTabletSettings, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraFreezeTablet, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraUnfreezeTablet, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraSetTabletState, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraTrimRows, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraLockTablet, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraReportTabletLocked, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraUnlockTablet, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraRotateStore, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraSplitPartition, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraMergePartitions, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraUpdatePartitionSampleKeys, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraAddTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraDecommissionTabletCell, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraSuspendTabletCell, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraResumeTabletCell, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraOnTabletCellDecommissioned, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraOnTabletCellSuspended, Unretained(this)));
        RegisterMethod(BIND(&TTabletManager::HydraReplicateTabletContent, Unretained(this)));
        RegisterForwardedMethod(BIND(&TTabletManager::HydraOnDynamicStoreAllocated, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();

        transactionManager->RegisterTransactionActionHandlers<TReqReplicateRows>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareReplicateRows, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitReplicateRows, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortReplicateRows, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqWritePulledRows>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareWritePulledRows, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitWritePulledRows, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortWritePulledRows, Unretained(this)),
            .Serialize = BIND_NO_PROPAGATE(&TTabletManager::HydraSerializeWritePulledRows, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqAdvanceReplicationProgress>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareAdvanceReplicationProgress, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortAdvanceReplicationProgress, Unretained(this)),
            .Serialize = BIND_NO_PROPAGATE(&TTabletManager::HydraSerializeAdvanceReplicationProgress, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqUpdateTabletStores>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareUpdateTabletStores, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitUpdateTabletStores, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortUpdateTabletStores, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqBoggleHunkTabletStoreLock>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareBoggleHunkTabletStoreLock, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitBoggleHunkTabletStoreLock, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortBoggleHunkTabletStoreLock, Unretained(this)),
        });

        BackupManager_->Initialize();

        const auto& tableConfigManager = Bootstrap_->GetTableDynamicConfigManager();
        tableConfigManager->SubscribeConfigChanged(TableDynamicConfigChangedCallback_);

        const auto& configManager = Bootstrap_->GetDynamicConfigManager();
        configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    }

    void Finalize() override
    {
        const auto& tableConfigManager = Bootstrap_->GetTableDynamicConfigManager();
        tableConfigManager->UnsubscribeConfigChanged(TableDynamicConfigChangedCallback_);
    }

    void OnStartLeading() override
    {
        const auto& tableConfigManager = Bootstrap_->GetTableDynamicConfigManager();
        if (tableConfigManager->IsConfigLoaded()) {
            OnTableDynamicConfigChanged(nullptr, tableConfigManager->GetConfig());
        }
    }

    void UpdateTabletSnapshot(TTablet* tablet, std::optional<TLockManagerEpoch> epoch = std::nullopt) override
    {
        if (!IsRecovery()) {
            const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
            snapshotStore->RegisterTabletSnapshot(Slot_, tablet, epoch);
        }
    }

    bool AllocateDynamicStoreIfNeeded(TTablet* tablet) override
    {
        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead &&
            tablet->DynamicStoreIdPool().empty() &&
            !tablet->GetDynamicStoreIdRequested())
        {
            AllocateDynamicStore(tablet);
            return true;
        }

        return false;
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

    ITabletCellWriteManagerHostPtr GetTabletCellWriteManagerHost() override
    {
        return this;
    }

    ISmoothMovementTrackerHostPtr GetSmoothMovementTrackerHost() override
    {
        return this;
    }

    std::vector<TTabletMemoryStatistics> GetMemoryStatistics() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TTabletMemoryStatistics> results;
        results.reserve(Tablets().size());

        for (const auto& [tabletId, tablet] : Tablets()) {
            auto& tabletMemory = results.emplace_back();
            tabletMemory.TabletId = tabletId;
            tabletMemory.TablePath = tablet->GetTablePath();

            auto& statistics = tabletMemory.Statistics;

            if (tablet->IsPhysicallySorted()) {
                for (const auto& store : tablet->GetEden()->Stores()) {
                    CountStoreMemoryStatistics(&statistics, store);
                }

                for (const auto& partition : tablet->PartitionList()) {
                    for (const auto& store : partition->Stores()) {
                        CountStoreMemoryStatistics(&statistics, store);
                    }
                }
            } else if (tablet->IsPhysicallyOrdered()) {
                for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                    CountStoreMemoryStatistics(&statistics, store);
                }
            }

            auto error = tablet->RuntimeData()->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Preload].Load();
            if (!error.IsOK()) {
                statistics.PreloadErrors.push_back(error);
            }

            if (const auto& rowCache = tablet->GetRowCache()) {
                statistics.RowCache.Usage = rowCache->GetUsedBytesCount();
            }
        }

        return results;
    }

    // COMPAT(aleksandra-zh)
    void ValidateHunkLocks() override
    {
        YT_LOG_INFO("Validating hunk locks");
        for (auto [tabletId, tablet] : TabletMap_) {
            for (auto [id, hunkChunk] : tablet->HunkChunkMap()) {
                auto lockCount = hunkChunk->GetLockCount();
                auto preparedLockCount = hunkChunk->GetPreparedStoreRefCount();
                if (lockCount != preparedLockCount) {
                    YT_LOG_INFO("Hunk lock count differs (TabletId: %v, HunkChunkId: %v, LockingStateLockCount: %v, PreparedStoreRefCount: %v)",
                        tabletId,
                        id,
                        lockCount,
                        preparedLockCount);
                }
            }
        }
    }

    // COMPAT(aleksandra-zh)
    void RestoreHunkLocks(
        TTransaction* transaction,
        TReqUpdateTabletStores* request) override
    {
        try {
            DoRestoreHunkLocks(transaction, request);
        } catch (const std::exception& ex) {
            auto tabletId = FromProto<TTabletId>(request->tablet_id());
            YT_LOG_ALERT(
                ex,
                "Error restoring hunk locks (TabletId: %v, TransactionId: %v)",
                tabletId,
                transaction->GetId());
            throw;
        }
    }

    TFuture<void> Trim(
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 trimmedRowCount) override
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

            if (tablet->GetReplicationCardId()) {
                ValidateTrimmedRowCountPrecedeReplication(tablet, trimmedRowCount);
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

    void ScheduleStoreRotation(TTablet* tablet, EStoreRotationReason reason) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!tablet->IsActiveServant()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsRotationPossible()) {
            return;
        }

        storeManager->ScheduleRotation(reason);

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_reason(static_cast<int>(reason));
        // Out of band immediate rotation may happen when this mutation is scheduled but not applied.
        // This rotation request will become obsolete and may lead to an empty active store
        // being rotated.
        ToProto(request.mutable_expected_active_store_id(), tablet->GetActiveStore()->GetId());
        Slot_->CommitTabletMutation(request);
    }

    void ReleaseBackingStore(const IChunkStorePtr& store) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto backingStore = store->GetBackingStore()) {
            store->SetBackingStore(nullptr);
            YT_LOG_DEBUG("Backing store released (StoreId: %v, BackingStoreId: %v)",
                store->GetId(),
                backingStore->GetId());
            // XXX(ifsmirnov): uncomment when tablet id is stored in TStoreBase.
            // store->GetTablet()->GetStructuredLogger()->OnBackingStoreReleased(store);
        }
    }

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const ITransactionPtr& transaction) override
    {
        YT_LOG_DEBUG("Acquiring tablet stores commit semaphore (%v, TransactionId: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId());

        return tablet
            ->GetStoresUpdateCommitSemaphore()
            ->AsyncAcquire()
            .ApplyUnique(
                BIND(
                    ThrowOnDestroyed(&TTabletManager::OnStoresUpdateCommitSemaphoreAcquired),
                    MakeWeak(this),
                    tablet,
                    transaction)
                .AsyncVia(tablet->GetEpochAutomatonInvoker()));
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    ETabletCellLifeStage GetTabletCellLifeStage() const override
    {
        return CellLifeStage_;
    }

    ITransactionManagerPtr GetTransactionManager() const override
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
        return Slot_->GetLatestTimestamp();
    }

    void RegisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId,
        TCellId siblingCellId) override
    {
        Slot_->RegisterSiblingTabletAvenue(siblingEndpointId, siblingCellId);
    }

    void UnregisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId) override
    {
        Slot_->UnregisterSiblingTabletAvenue(siblingEndpointId);
    }

    void RegisterMasterAvenue(
        TTabletId tabletId,
        NHiveServer::TAvenueEndpointId masterEndpointId,
        NHiveServer::TPersistentMailboxState&& cookie) override
    {
        Slot_->RegisterMasterAvenue(tabletId, masterEndpointId, std::move(cookie));
    }

    NHiveServer::TPersistentMailboxState UnregisterMasterAvenue(
        NHiveServer::TAvenueEndpointId masterEndpointId) override
    {
        return Slot_->UnregisterMasterAvenue(masterEndpointId);
    }

    void PostAvenueMessage(
        TAvenueEndpointId endpointId,
        const ::google::protobuf::MessageLite& message) override
    {
        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(endpointId);
        hiveManager->PostMessage(mailbox, message);
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Tablet, TTablet);

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TTabletManager> impl, IInvokerPtr invoker)
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
                    auto producer = BIND(&TTabletManager::BuildTabletOrchidYson, owner, tablet);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TTabletManager> Owner_;

        explicit TOrchidService(TWeakPtr<TTabletManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND()
    };

    const TTabletManagerConfigPtr Config_;

    class TTabletContext
        : public ITabletContext
    {
    public:
        explicit TTabletContext(TTabletManager* owner)
            : Owner_(owner)
        { }

        TCellId GetCellId() const override
        {
            return Owner_->Slot_->GetCellId();
        }

        NNative::IClientPtr GetClient() const override
        {
            return Owner_->Bootstrap_->GetClient();
        }

        NClusterNode::TClusterNodeDynamicConfigManagerPtr GetDynamicConfigManager() const override
        {
            return Owner_->Bootstrap_->GetDynamicConfigManager();
        }

        const TString& GetTabletCellBundleName() const override
        {
            return Owner_->Slot_->GetTabletCellBundleName();
        }

        EPeerState GetAutomatonState() const override
        {
            return Owner_->Slot_->GetAutomatonState();
        }

        int GetAutomatonTerm() const override
        {
            return Owner_->Slot_->GetAutomatonTerm();
        }

        IInvokerPtr GetControlInvoker() const override
        {
            return Owner_->Bootstrap_->GetControlInvoker();
        }

        IInvokerPtr GetAutomatonInvoker() const override
        {
            return Owner_->Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Default);
        }

        IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const override
        {
            return Owner_->Bootstrap_->GetColumnEvaluatorCache();
        }

        NQueryClient::IRowComparerProviderPtr GetRowComparerProvider() const override
        {
            return Owner_->Bootstrap_->GetRowComparerProvider();
        }

        TObjectId GenerateIdDeprecated(EObjectType type) const override
        {
            return Owner_->Slot_->GenerateId(type);
        }

        IStorePtr CreateStore(
            TTablet* tablet,
            EStoreType type,
            TStoreId storeId,
            const TAddStoreDescriptor* descriptor) const override
        {
            return Owner_->CreateStore(tablet, type, storeId, descriptor);
        }

        THunkChunkPtr CreateHunkChunk(
            TTablet* tablet,
            TChunkId chunkId,
            const TAddHunkChunkDescriptor* descriptor) const override
        {
            return Owner_->CreateHunkChunk(tablet, chunkId, descriptor);
        }

        ITransactionManagerPtr GetTransactionManager() const override
        {
            return Owner_->Slot_->GetTransactionManager();
        }

        NRpc::IServerPtr GetLocalRpcServer() const override
        {
            return Owner_->Bootstrap_->GetRpcServer();
        }

        INodeMemoryTrackerPtr GetMemoryUsageTracker() const override
        {
            return Owner_->Bootstrap_->GetMemoryUsageTracker();
        }

        NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() const override
        {
            return Owner_->Bootstrap_->GetConnection()->GetChunkReplicaCache();
        }

        IHedgingManagerRegistryPtr GetHedgingManagerRegistry() const override
        {
            return Owner_->Bootstrap_->GetHedgingManagerRegistry();
        }

        TString GetLocalHostName() const override
        {
            return Owner_->Bootstrap_->GetLocalHostName();
        }

        NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override
        {
            return Owner_->Bootstrap_->GetLocalDescriptor();
        }

        ITabletWriteManagerHostPtr GetTabletWriteManagerHost() const override
        {
            return Owner_;
        }

    private:
        TTabletManager* const Owner_;
    };

    class TTabletMapTraits
    {
    public:
        explicit TTabletMapTraits(TTabletManager* owner)
            : Owner_(owner)
        { }

        std::unique_ptr<TTablet> Create(TTabletId id) const
        {
            return std::make_unique<TTablet>(id, &Owner_->TabletContext_);
        }

    private:
        TTabletManager* const Owner_;
    };

    TTabletContext TabletContext_;
    TEntityMap<TTablet, TTabletMapTraits> TabletMap_;
    ETabletCellLifeStage CellLifeStage_ = ETabletCellLifeStage::Running;
    bool Suspending_ = false;

    // COMPAT(gritukan)
    ETabletReign Reign_ = CheckedEnumCast<ETabletReign>(GetCurrentReign());

    TRingQueue<TTablet*> PrelockedTablets_;

    THashSet<IDynamicStorePtr> OrphanedStores_;
    THashMap<TTabletId, std::unique_ptr<TTablet>> OrphanedTablets_;

    const TPeriodicExecutorPtr DecommissionCheckExecutor_;
    const TPeriodicExecutorPtr SuspensionCheckExecutor_;

    const IYPathServicePtr OrchidService_;

    IBackupManagerPtr BackupManager_;

    const TCompactionHintFetcherPtr RowDigestFetcher_;
    const TCompactionHintFetcherPtr ChunkViewSizeFetcher_;

    const TCallback<void(TClusterTableConfigPatchSetPtr, TClusterTableConfigPatchSetPtr)> TableDynamicConfigChangedCallback_ =
        BIND(&TTabletManager::OnTableDynamicConfigChanged, MakeWeak(this));

    const TCallback<void(TClusterNodeDynamicConfigPtr, TClusterNodeDynamicConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TTabletManager::OnDynamicConfigChanged, MakeWeak(this));

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
        Save(context, Suspending_);
    }

    TCallback<void(TSaveContext&)> SaveAsync()
    {
        std::vector<std::pair<TTabletId, TCallback<void(TSaveContext&)>>> capturedTablets;
        for (auto [tabletId, tablet] : TabletMap_) {
            capturedTablets.emplace_back(tabletId, tablet->AsyncSave());
        }
        SortBy(capturedTablets, [&] (const auto& pair) {
            return pair.first;
        });

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
        Load(context, Suspending_);

        Automaton_->RememberReign(static_cast<TReign>(context.GetVersion()));

        Reign_ = context.GetVersion();
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

        const auto& avenueDirectory = Slot_->GetAvenueDirectory();

        for (auto [tabletId, tablet] : TabletMap_) {
            InitializeTablet(tablet);

            tablet->Reconfigure(Slot_);
            tablet->OnAfterSnapshotLoaded();

            Bootstrap_->GetStructuredLogger()->OnHeartbeatRequest(
                Slot_->GetTabletManager(),
                /*initial*/ true);

            if (auto masterEndpointId = tablet->GetMasterAvenueEndpointId()) {
                auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
                avenueDirectory->UpdateEndpoint(masterEndpointId, masterCellId);
            }

            const auto& movementData = tablet->SmoothMovementData();
            if (auto siblingEndpointId = movementData.GetSiblingAvenueEndpointId()) {
                avenueDirectory->UpdateEndpoint(siblingEndpointId, movementData.GetSiblingCellId());
            }
        }

        // COMPAT(gritukan): Remove it after ETabletReign::TabletPrerequisites.
        if (CellLifeStage_ == ETabletCellLifeStage::DecommissioningOnNode || Suspending_) {
            Slot_->GetLeaseManager()->SetDecommission(/*decommission*/ true);
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        for (auto [tabletId, tablet] : TabletMap_) {
            tablet->Clear();
        }

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
        SuspensionCheckExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        StopEpoch();

        YT_UNUSED_FUTURE(DecommissionCheckExecutor_->Stop());
        YT_UNUSED_FUTURE(SuspensionCheckExecutor_->Stop());
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

        EpochStarted_.Fire();
    }

    void StopEpoch()
    {
        EpochStopped_.Fire();

        for (auto [tabletId, tablet] : TabletMap_) {
            StopTabletEpoch(tablet);
        }
    }

    // COMPAT(ifsmirnov)
    // Replace all callers with |request.set_mount_revision(...)| when removing the compat.
    template <class TResponse>
    void SetMountRevisionCompat(TResponse* response, TTablet* tablet)
    {
        const auto* context = GetCurrentMutationContext();
        if (context->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
            response->set_mount_revision(tablet->GetMountRevision());
        }
    }

    void HydraMountTablet(TReqMountTablet* request)
    {
        // COMPAT(ifsmirnov)
        #define GET_FROM_ESSENTIAL(field_name) \
            (request->has_essential_content() \
                ? request->essential_content().field_name() \
                : request->field_name ## _deprecated())

        #define GET_FROM_REPLICATABLE(field_name) \
            (request->has_replicatable_content() \
                ? request->replicatable_content().field_name() \
                : request->field_name ## _deprecated())

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto tableId = FromProto<TObjectId>(request->table_id());
        auto masterAvenueEndpointId = FromProto<TAvenueEndpointId>(request->master_avenue_endpoint_id());
        const auto& path = GET_FROM_ESSENTIAL(path);
        auto schemaId = FromProto<TObjectId>(GET_FROM_ESSENTIAL(schema_id));
        auto schema = FromProto<TTableSchemaPtr>(GET_FROM_ESSENTIAL(schema));
        auto pivotKey = GET_FROM_ESSENTIAL(has_pivot_key) ? FromProto<TLegacyOwningKey>(GET_FROM_ESSENTIAL(pivot_key)) : TLegacyOwningKey();
        auto nextPivotKey = GET_FROM_ESSENTIAL(has_next_pivot_key) ? FromProto<TLegacyOwningKey>(GET_FROM_ESSENTIAL(next_pivot_key)) : TLegacyOwningKey();
        auto rawSettings = request->has_essential_content()
            ? DeserializeTableSettings(&request->replicatable_content(), tabletId)
            : DeserializeTableSettings(request, tabletId);
        auto atomicity = FromProto<EAtomicity>(GET_FROM_ESSENTIAL(atomicity));
        auto commitOrdering = FromProto<ECommitOrdering>(GET_FROM_ESSENTIAL(commit_ordering));
        bool freeze = request->freeze();
        auto upstreamReplicaId = FromProto<TTableReplicaId>(GET_FROM_ESSENTIAL(upstream_replica_id));
        auto replicaDescriptors = FromProto<std::vector<TTableReplicaDescriptor>>(request->replicas());
        auto retainedTimestamp = GET_FROM_REPLICATABLE(has_retained_timestamp)
            ? FromProto<TTimestamp>(GET_FROM_REPLICATABLE(retained_timestamp))
            : MinTimestamp;
        const auto& mountHint = request->mount_hint();
        auto cumulativeDataWeight = GET_FROM_REPLICATABLE(cumulative_data_weight);
        bool isSmoothMoveTarget = request->has_movement_source_cell_id();

        rawSettings.DropIrrelevantExperiments(
            {
                .TableId = tableId,
                .TablePath = path,
                .TabletCellBundle = Slot_->GetTabletCellBundleName(),
                // NB: Generally InMemoryMode is taken from mount config, but it is not assembled yet at this point.
                // Experiments never affect in-memory mode, so it is safe to use the raw value.
                .InMemoryMode = rawSettings.Provided.MountConfigNode->GetChildValueOrDefault<EInMemoryMode>(
                    "in_memory_mode",
                    EInMemoryMode::None),
                .Sorted = schema->IsSorted(),
                .Replicated = TypeFromId(tableId) == EObjectType::ReplicatedTable,
            },
            static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) <
                ETabletReign::InMemoryModeAndBundleInExperimentDescriptor);

        std::vector<TError> configErrors;
        auto settings = rawSettings.BuildEffectiveSettings(&configErrors, nullptr);

        // COMPAT(gritukan, ifsmirnov)
        auto seed = [&] {
            auto* mutationContext = GetCurrentMutationContext();
            if (mutationContext->Request().Reign >= ToUnderlying(ETabletReign::TabletIdGenerator)) {
                return mutationContext->RandomGenerator()->Generate<ui64>();
            } else {
                auto preSeed = mutationContext->GetRandomSeed() ^ tabletId.Parts64[1] ^ tableId.Parts64[1] ^ mountRevision;
                // Seems random enough.
                return TRandomGenerator(preSeed).Generate<ui64>();
            }
        }();

        NTabletNode::TIdGenerator idGenerator(
            CellTagFromId(tabletId),
            // Make first ids look like 1-1-... rather than 0-1-...
            /*counter*/ 1ull << 32,
            /*seed*/ seed);

        auto tabletHolder = std::make_unique<TTablet>(
            tabletId,
            settings,
            mountRevision,
            tableId,
            path,
            &TabletContext_,
            idGenerator,
            schemaId,
            schema,
            pivotKey,
            nextPivotKey,
            atomicity,
            commitOrdering,
            upstreamReplicaId,
            retainedTimestamp,
            cumulativeDataWeight);
        tabletHolder->RawSettings() = rawSettings;

        InitializeTablet(tabletHolder.get());

        tabletHolder->Reconfigure(Slot_);

        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        SetTableConfigErrors(tablet, configErrors);

        if (tablet->IsPhysicallyOrdered() && !isSmoothMoveTarget) {
            tablet->SetTrimmedRowCount(GET_FROM_REPLICATABLE(trimmed_row_count));
        }

        PopulateDynamicStoreIdPool(tablet, request);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Mount(
            MakeRange(GET_FROM_REPLICATABLE(stores)),
            MakeRange(GET_FROM_REPLICATABLE(hunk_chunks)),
            /*createDynamicStore*/ !freeze && !isSmoothMoveTarget,
            mountHint);

        tablet->SetState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        // NB: We do not store previously attached dictionary chunk ids. We just build new ones upon mount.
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            tablet->SetCompressionDictionaryRebuildBackoffTime(policy, TInstant::Now());
        }

        if (isSmoothMoveTarget) {
            // Smooth movement target is being allocated.

            auto siblingCellId = FromProto<TTabletCellId>(
                request->movement_source_cell_id());

            auto siblingEndpointId = FromProto<TAvenueEndpointId>(
                request->movement_source_avenue_endpoint_id());

            auto& movementData = tablet->SmoothMovementData();
            movementData.SetSiblingCellId(siblingCellId);
            movementData.SetRole(ESmoothMovementRole::Target);
            movementData.SetStage(ESmoothMovementStage::TargetAllocated);
            movementData.SetSiblingMountRevision(
                FromProto<TRevision>(request->movement_source_mount_revision()));

            movementData.SetSiblingAvenueEndpointId(siblingEndpointId);
            Slot_->RegisterSiblingTabletAvenue(siblingEndpointId, siblingCellId);

            YT_VERIFY(!masterAvenueEndpointId);
        }

        if (masterAvenueEndpointId) {
            tablet->SetMasterAvenueEndpointId(masterAvenueEndpointId);
            Slot_->RegisterMasterAvenue(
                tablet->GetId(),
                masterAvenueEndpointId,
                /*cookie*/ {});
        }

        YT_LOG_INFO("Tablet mounted (%v, MountRevision: %x, Keys: %v .. %v, "
            "StoreCount: %v, HunkChunkCount: %v, PartitionCount: %v, TotalRowCount: %v, TrimmedRowCount: %v, Atomicity: %v, "
            "CommitOrdering: %v, Frozen: %v, UpstreamReplicaId: %v, RetainedTimestamp: %v, SchemaId: %v, "
            "MasterAvenueEndpointId: %v)",
            tablet->GetLoggingTag(),
            mountRevision,
            pivotKey,
            nextPivotKey,
            GET_FROM_REPLICATABLE(stores).size(),
            GET_FROM_REPLICATABLE(hunk_chunks).size(),
            tablet->IsPhysicallySorted() ? std::make_optional(tablet->PartitionList().size()) : std::nullopt,
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTotalRowCount()),
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTrimmedRowCount()),
            tablet->GetAtomicity(),
            tablet->GetCommitOrdering(),
            freeze,
            upstreamReplicaId,
            retainedTimestamp,
            schemaId,
            masterAvenueEndpointId);

        for (const auto& descriptor : request->replicas()) {
            AddTableReplica(tablet, descriptor);
        }

        if (request->has_replication_progress()) {
            auto replicationCardId = tablet->GetReplicationCardId();
            auto progress = FromProto<TReplicationProgress>(request->replication_progress());
            YT_LOG_DEBUG("Tablet bound for chaos replication (%v, ReplicationCardId: %v, ReplicationProgress: %v)",
                tablet->GetLoggingTag(),
                replicationCardId,
                progress);

            tablet->RuntimeData()->ReplicationProgress.Store(New<TRefCountedReplicationProgress>(std::move(progress)));
        }

        const auto& lockManager = tablet->GetLockManager();

        for (const auto& lock : GET_FROM_REPLICATABLE(locks)) {
            auto transactionId = FromProto<TTabletId>(lock.transaction_id());
            auto lockTimestamp = static_cast<TTimestamp>(lock.timestamp());
            lockManager->Lock(lockTimestamp, transactionId, true);
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            response.set_frozen(freeze);
            SetMountRevisionCompat(&response, tablet);
            PostMasterMessage(tablet, response, /*forceCellMailbox*/ true);
        }

        tablet->GetStructuredLogger()->OnTabletMounted();
        tablet->GetStructuredLogger()->OnFullHeartbeat();

        if (!IsRecovery()) {
            if (!isSmoothMoveTarget) {
                StartTabletEpoch(tablet);
            }
        }

        #undef GET_FROM_ESSENTIAL
        #undef GET_FROM_REPLICATABLE
    }

    void PopulateAddStoreDescriptor(
        TAddStoreDescriptor* descriptor,
        const IStorePtr& store)
    {
        YT_VERIFY(store->GetType() != EStoreType::HunkChunk);

        descriptor->set_store_type(ToProto<int>(store->GetType()));
        ToProto(descriptor->mutable_store_id(), store->GetId());

        if (store->IsChunk()) {
            const auto& chunkStore = store->AsChunk();

            ToProto(descriptor->mutable_chunk_meta(), chunkStore->GetChunkMeta());

            switch (store->GetType()) {
                case EStoreType::SortedChunk: {
                    auto sortedChunkStore = store->AsSortedChunk();
                    if (sortedChunkStore->GetId() != sortedChunkStore->GetChunkId()) {
                        THROW_ERROR_EXCEPTION("Chunk views are not supported");
                    }
                    break;
                }

                case EStoreType::OrderedChunk:
                    YT_ABORT();

                default:
                    YT_ABORT();
            }
        } else {
            // Initialize meta in the least possible way.
            auto* meta = descriptor->mutable_chunk_meta();
            meta->set_type(0);
            meta->set_format(0);
            meta->mutable_extensions();
        }
    }

    TReqReplicateTabletContent PrepareReplicateTabletContentRequest(TTablet* tablet) override
    {
        auto& movementData = tablet->SmoothMovementData();

        TReqReplicateTabletContent request;
        auto* replicatableContent = request.mutable_replicatable_content();

        // Validation against not implemented features.
        if (tablet->GetPhysicalSchema()->HasHunkColumns()) {
            THROW_ERROR_EXCEPTION("Hunks are not supported");
        }

        if (tablet->IsPhysicallyLog() ||
            tablet->IsPhysicallyOrdered() ||
            tablet->IsReplicated())
        {
            THROW_ERROR_EXCEPTION("Ordered and replicated tables are not supported");
        }

        if (!tablet->GetLockManager()->IsEmpty()) {
            THROW_ERROR_EXCEPTION("Bulk insert lock replication is not supported");
        }

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead) {
            THROW_ERROR_EXCEPTION("Tables with enabled dynamic store read are not supported");
        }

        // Essential stuff.
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(
            ToProto<ui64>(tablet->SmoothMovementData().GetSiblingMountRevision()));

        // Local tablet stuff: id generator, partition pivot keys, retained timestamp.
        tablet->PopulateReplicateTabletContentRequest(&request);

        // Stores.
        auto onStore = [&] (const IStorePtr& store) {
            if (store->IsDynamic() && store->GetStoreState() != EStoreState::ActiveDynamic) {
                movementData.CommonDynamicStoreIds().insert(store->GetId());
            }

            PopulateAddStoreDescriptor(replicatableContent->add_stores(), store);

            if (store->IsSorted()) {
                ToProto(
                    request.add_store_partition_ids(),
                    store->AsSorted()->GetPartition()->GetId());
            }
        };

        if (tablet->IsPhysicallyOrdered()) {
            for (const auto& [rowIndex, store] : tablet->StoreRowIndexMap()) {
                onStore(store);
            }
        } else {
            for (const auto& store : tablet->GetEden()->Stores()) {
                onStore(store);
            }
            for (const auto& partition : tablet->PartitionList()) {
                for (const auto& store : partition->Stores()) {
                    onStore(store);
                }
            }
        }

        if (const auto& activeStore = tablet->GetActiveStore()) {
            ToProto(request.mutable_active_store_id(), activeStore->GetId());
        }

        return request;
    }

    void HydraReplicateTabletContent(TReqReplicateTabletContent* request)
    {
        using NYT::FromProto;

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = FromProto<TRevision>(request->mount_revision());
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto& movementData = tablet->SmoothMovementData();
        YT_VERIFY(movementData.GetRole() == ESmoothMovementRole::Target);
        YT_VERIFY(movementData.GetStage() == ESmoothMovementStage::TargetAllocated);
        YT_VERIFY(movementData.GetSiblingCellId());

        const auto& replicatableContent = request->replicatable_content();

        YT_LOG_DEBUG("Tablet got replicated content (%v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            replicatableContent.stores().size());

        // Local tablet stuff.
        tablet->LoadReplicatedContent(request);

        // Stores.
        auto activeStoreId = FromProto<TStoreId>(request->active_store_id());

        const auto& storeManager = tablet->GetStoreManager();
        auto partitionIds = FromProto<std::vector<TPartitionId>>(request->store_partition_ids());
        YT_VERIFY(replicatableContent.stores().size() == ssize(partitionIds));
        for (const auto& [descriptor, partitionId] : Zip(replicatableContent.stores(), partitionIds)) {
            auto type = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());

            if (storeId == activeStoreId) {
                storeManager->CreateActiveStore(activeStoreId);
            } else {
                auto store = CreateStore(tablet, type, storeId, &descriptor);
                if (store->IsDynamic()) {
                    store->SetStoreState(EStoreState::PassiveDynamic);
                    InsertOrCrash(movementData.CommonDynamicStoreIds(), store->GetId());
                }

                store->Initialize();
                storeManager->AddStore(store, /*onMount*/ false, /*onFlush*/ false, partitionId);
            }
        }

        StartTabletEpoch(tablet);

        Slot_->GetSmoothMovementTracker()->OnGotReplicatedContent(tablet);
    }

    void HydraUnmountTablet(TReqUnmountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (request->force()) {
            YT_LOG_INFO("Tablet is forcefully unmounted (%v)",
                tablet->GetLoggingTag());

            auto tabletHolder = TabletMap_.Release(tabletId);

            if (tablet->GetTotalTabletLockCount() > 0) {
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

            if (auto endpointId = tablet->SmoothMovementData().GetSiblingAvenueEndpointId()) {
                UnregisterSiblingTabletAvenue(endpointId);
            }

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
            }
        } else {
            auto state = tablet->GetState();
            if (IsInUnmountWorkflow(state)) {
                YT_LOG_INFO("Requested to unmount a tablet in a wrong state, ignored (State: %v, %v)",
                    state,
                    tablet->GetLoggingTag());
                return;
            }

            YT_LOG_INFO("Unmounting tablet (%v)",
                tablet->GetLoggingTag());

            tablet->SetState(ETabletState::UnmountWaitingForLocks);

            YT_LOG_INFO("Waiting for all tablet locks to be released (%v)",
                tablet->GetLoggingTag());

            CheckIfTabletFullyUnlocked(tablet);
        }
    }

    void ReconfigureTablet(TTablet* tablet, TRawTableSettings rawSettings)
    {
        std::vector<TError> configErrors;
        auto settings = rawSettings.BuildEffectiveSettings(&configErrors, nullptr);

        bool rowDigestChanged = *settings.MountConfig->RowDigestCompaction !=
            *tablet->GetSettings().MountConfig->RowDigestCompaction;

        bool chunkViewSizeChanged = settings.MountConfig->MaxChunkViewSizeRatio !=
            tablet->GetSettings().MountConfig->MaxChunkViewSizeRatio;

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Remount(settings);

        SetTableConfigErrors(tablet, configErrors);

        tablet->RawSettings() = std::move(rawSettings);

        tablet->Reconfigure(Slot_);
        UpdateTabletSnapshot(tablet);

        if (!IsRecovery()) {
            for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                StopTableReplicaEpoch(&replicaInfo);
                StartTableReplicaEpoch(tablet, &replicaInfo);
            }
        }

        if (chunkViewSizeChanged) {
            ChunkViewSizeFetcher_->ResetCompactionHints(tablet);
            ChunkViewSizeFetcher_->FetchStoreInfos(tablet);
        }
        if (rowDigestChanged) {
            RowDigestFetcher_->ResetCompactionHints(tablet);
            RowDigestFetcher_->FetchStoreInfos(tablet);
        }
    }

    void HydraRemountTablet(TReqRemountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto rawSettings = DeserializeTableSettings(request, tabletId);

        auto descriptor = GetTableConfigExperimentDescriptor(tablet);
        rawSettings.DropIrrelevantExperiments(
            descriptor,
            static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) <
                ETabletReign::InMemoryModeAndBundleInExperimentDescriptor);

        ReconfigureTablet(tablet, std::move(rawSettings));

        YT_LOG_INFO("Tablet remounted (%v)",
            tablet->GetLoggingTag());
    }

    void HydraUpdateTabletSettings(TReqUpdateTabletSettings* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        TRawTableSettings newRawSettings(tablet->RawSettings());

        newRawSettings.Experiments = ConvertTo<std::map<TString, TTableConfigExperimentPtr>>(
            TYsonString(request->experiments()));
        newRawSettings.GlobalPatch = ConvertTo<TTableConfigPatchPtr>(TYsonString(request->global_patch()));

        auto descriptor = GetTableConfigExperimentDescriptor(tablet);
        newRawSettings.DropIrrelevantExperiments(
            descriptor,
            static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) <
                ETabletReign::InMemoryModeAndBundleInExperimentDescriptor);

        auto& oldExperiments = tablet->RawSettings().Experiments;
        auto& newExperiments = newRawSettings.Experiments;

        // Revert experiments that should not be auto-applied.
        for (auto newIt = newExperiments.begin(); newIt != newExperiments.end(); ) {
            auto& [name, experiment] = *newIt;

            if (!experiment->AutoApply) {
                auto oldIt = oldExperiments.find(name);

                if (oldIt == oldExperiments.end()) {
                    newExperiments.erase(newIt++);
                    continue;
                }

                experiment = oldIt->second;
            }
            ++newIt;
        }

        ReconfigureTablet(tablet, std::move(newRawSettings));

        YT_LOG_DEBUG("Tablet settings updated (%v, AppliedExperiments: %v)",
            tablet->GetLoggingTag(),
            MakeFormattableView(
                tablet->RawSettings().Experiments,
                [] (auto* builder, const auto& experiment) {
                    FormatValue(builder, experiment.first, /*format*/ TStringBuf{});
                }));
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
            YT_LOG_ALERT("Requested to freeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO("Freezing tablet (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::FreezeWaitingForLocks);

        YT_LOG_INFO("Waiting for all tablet locks to be released (%v)",
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
            YT_LOG_INFO("Requested to unfreeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO("Tablet unfrozen (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::Mounted);

        PopulateDynamicStoreIdPool(tablet, request);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Rotate(true, EStoreRotationReason::None);
        storeManager->InitializeRotation();

        UpdateTabletSnapshot(tablet);

        auto dynamicStoreIds = FromProto<std::vector<TDynamicStoreId>>(request->dynamic_store_ids());
        tablet->GetStructuredLogger()->OnTabletUnfrozen(dynamicStoreIds);

        TRspUnfreezeTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        SetMountRevisionCompat(&response, tablet);
        PostMasterMessage(tablet, response);
    }

    void HydraLockTablet(TReqLockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        auto transactionId = FromProto<TTransactionId>(request->lock().transaction_id());
        auto lockTimestamp = static_cast<TTimestamp>(request->lock().timestamp());

        const auto& lockManager = tablet->GetLockManager();
        lockManager->Lock(lockTimestamp, transactionId, /*confirmed*/ false);

        YT_LOG_INFO("Tablet locked by bulk insert (TabletId: %v, TransactionId: %v)",
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

        YT_LOG_INFO("Tablet bulk insert lock confirmed (TabletId: %v, TransactionIds: %v)",
            tabletId,
            transactionIds);

        if (tablet->IsActiveServant()) {
            TRspLockTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            ToProto(response.mutable_transaction_ids(), transactionIds);
            PostMasterMessage(tablet, response);
        }
    }

    void HydraUnlockTablet(TReqUnlockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        const auto* context = GetCurrentMutationContext();
        if (static_cast<ETabletReign>(context->Request().Reign) < ETabletReign::NoMountRevisionCheckInBulkInsert) {
            if (request->has_mount_revision() && request->mount_revision() != 0) {
                auto mountRevision = request->mount_revision();
                if (mountRevision != tablet->GetMountRevision()) {
                    return;
                }
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
            YT_LOG_INFO(
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

        // COMPAT(ifsmirnov)
        bool shouldUnlock;
        if (GetCurrentMutationContext()->Request().Reign >=
            static_cast<int>(ETabletReign::FixBulkInsertAtomicityNone))
        {
            shouldUnlock = tablet->GetLockManager()->HasTransaction(transactionId);
        } else {
            shouldUnlock = tablet->GetAtomicity() == EAtomicity::Full;
        }

        if (shouldUnlock) {
            auto nextEpoch = lockManager->GetEpoch() + 1;
            UpdateTabletSnapshot(tablet, nextEpoch);

            auto commitTimestamp = request->commit_timestamp();
            lockManager->Unlock(commitTimestamp, transactionId);
        } else {
            UpdateTabletSnapshot(tablet);
        }

        FetchCompactionHints(tablet, storesToAdd);

        YT_LOG_INFO(
            "Tablet unlocked by bulk insert (%v, TransactionId: %v, AddedStoreIds: %v, LockManagerEpoch: %v)",
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

        auto requestedState = FromProto<ETabletState>(request->state());

        switch (requestedState) {
            case ETabletState::FreezeFlushing: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO("Improper tablet state transition requested, ignored (CurrentState: %v, RequestedState: %v, %v)",
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

                YT_LOG_INFO("Waiting for all tablet stores to be flushed (%v, NewState: %v)",
                    tablet->GetLoggingTag(),
                    requestedState);

                CheckIfTabletFullyFlushed(tablet);
                break;
            }

            case ETabletState::Unmounted: {
                tablet->SetState(ETabletState::Unmounted);

                YT_LOG_INFO("Tablet unmounted (%v)",
                    tablet->GetLoggingTag());

                if (!IsRecovery()) {
                    StopTabletEpoch(tablet);
                }

                for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                    PostTableReplicaStatistics(tablet, replicaInfo);
                }

                tablet->GetStructuredLogger()->OnTabletUnmounted();

                TRspUnmountTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();
                if (auto replicationProgress = tablet->RuntimeData()->ReplicationProgress.Acquire()) {
                    ToProto(response.mutable_replication_progress(), *replicationProgress);
                }
                SetMountRevisionCompat(&response, tablet);

                if (auto masterEndpointId = tablet->GetMasterAvenueEndpointId()) {
                    Slot_->UnregisterMasterAvenue(masterEndpointId);
                }

                PostMasterMessage(tablet, response, /*forceCellMailbox*/ true);

                TabletMap_.Remove(tabletId);

                break;
            }

            case ETabletState::Frozen: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO("Improper tablet state transition requested, ignored (CurrentState %v, RequestedState: %v, %v)",
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

                YT_LOG_INFO("Tablet frozen (%v)",
                    tablet->GetLoggingTag());

                tablet->GetStructuredLogger()->OnTabletFrozen();

                TRspFreezeTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();
                SetMountRevisionCompat(&response, tablet);
                PostMasterMessage(tablet, response);
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
        auto mountRevision = request->mount_revision();
        auto reason = static_cast<EStoreRotationReason>(request->reason());
        TStoreId expectedActiveStoreId;
        if (request->has_expected_active_store_id()) {
            FromProto(&expectedActiveStoreId, request->expected_active_store_id());
        }

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        // COMPAT(ifsmirnov)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
            ETabletReign::SendDynamicStoreInBackup)
        {
            if (tablet->GetActiveStore() &&
                expectedActiveStoreId &&
                tablet->GetActiveStore()->GetId() != expectedActiveStoreId)
            {
                YT_LOG_DEBUG(
                    "Active store id mismatch in rotation attempt "
                    "(ExpectedActiveStoreId: %v, ActualActiveStoreId: %v, Reason: %v, %v)",
                    expectedActiveStoreId,
                    tablet->GetActiveStore()->GetId(),
                    reason,
                    tablet->GetLoggingTag());
                storeManager->UnscheduleRotation();
                return;
            }
        }

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead && tablet->DynamicStoreIdPool().empty()) {
            if (!tablet->GetDynamicStoreIdRequested()) {
                AllocateDynamicStore(tablet);
            }
            // TODO(ifsmirnov): Store flusher will try making unsuccessful mutations if response
            // from master comes late. Maybe should optimize.
            storeManager->UnscheduleRotation();
            return;
        }

        storeManager->Rotate(true, reason);
        UpdateTabletSnapshot(tablet);

        if (tablet->IsPhysicallyOrdered()) {
            if (AllocateDynamicStoreIfNeeded(tablet)) {
                YT_LOG_DEBUG(
                    "Dynamic store id for ordered tablet allocated after rotation (%v)",
                    tablet->GetLoggingTag());

            }
        }
    }

    void HydraPrepareUpdateTabletStores(
        TTransaction* transaction,
        TReqUpdateTabletStores* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto reason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        if (transaction->IsExternalizedToThisCell()) {
            YT_LOG_DEBUG("Preparing tablet stores update under externalized transaction "
                "(TransactionId: %v, TabletId: %v, Reason: %v)",
                transaction->GetId(),
                tabletId,
                reason);
        }

        auto* tablet = GetTabletOrThrow(tabletId);
        const auto& structuredLogger = tablet->GetStructuredLogger();

        if (!tablet->IsActiveServant() && !transaction->IsExternalizedToThisCell()) {
            THROW_ERROR_EXCEPTION("Cannot prepare tablet stores update of a non-active servant "
                "with non-externalized transaction %v, transaction may be stale")
                << TErrorAttribute("tablet_id", tabletId);
        }

        if (tablet->IsActiveServant() &&
            tablet->SmoothMovementData().ShouldForwardMutation() &&
            !transaction->IsExternalizedFromThisCell())
        {
            THROW_ERROR_EXCEPTION("Transaction %v is not externalized but must be forwarded, "
                "transaction may be stale",
                transaction->GetId())
                << TErrorAttribute("tablet_id", tabletId);
        }

        YT_VERIFY(tablet->IsActiveServant() == !transaction->IsExternalizedToThisCell());

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        // Validate.
        auto mountRevision = request->mount_revision();
        if (tablet->IsActiveServant()) {
            tablet->ValidateMountRevision(mountRevision);
        } else {
            if (tablet->SmoothMovementData().GetSiblingMountRevision() != mountRevision) {
                THROW_ERROR_EXCEPTION("Invalid sibling mount revision, expected %x, got %x",
                    tablet->SmoothMovementData().GetSiblingMountRevision(),
                    mountRevision);
            }
        }

        // COMPAT(dave11ar)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
            ETabletReign::FixRaceBetweenCompactionUnmount)
        {
            if (IsInUnmountWorkflow(tablet->GetState()) && updateReason != ETabletStoresUpdateReason::Flush) {
                THROW_ERROR_EXCEPTION("Tablet is in %Qlv state", tablet->GetState())
                    << TErrorAttribute("update_reason", updateReason);
            }
        }

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            InsertOrCrash(hunkChunkIdsToAdd, chunkId);
        }

        if (request->create_hunk_chunks_during_prepare()) {
            for (auto chunkId : hunkChunkIdsToAdd) {
                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (hunkChunk && hunkChunk->GetState() != EHunkChunkState::Active) {
                    THROW_ERROR_EXCEPTION("Referenced hunk chunk %v is in %Qlv state",
                        chunkId,
                        hunkChunk->GetState());
                }
            }
        }

        std::vector<TStoreId> storeIdsToAdd;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
                descriptor.chunk_meta().extensions()))
            {
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

        const auto& movementData = tablet->SmoothMovementData();
        bool isCommonFlush = reason == ETabletStoresUpdateReason::Flush &&
            movementData.CommonDynamicStoreIds().contains(storeIdsToRemove[0]);

        // Do not perform the validation for the sibling servant since own tablet stores update
        // is not allowed for the non-active servant.
        if (tablet->IsActiveServant()) {
            if (!movementData.IsTabletStoresUpdateAllowed(isCommonFlush)) {
                THROW_ERROR_EXCEPTION("Tablet stores update is not allowed "
                    "(%v, SmoothMovementRole: %v, SmoothMovementStage: %v, UpdateReason: %v)",
                    tablet->GetLoggingTag(),
                    movementData.GetRole(),
                    movementData.GetStage(),
                    reason);
            }
        }

        // COMPAT(ifsmirnov)
        if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
            if (tablet->GetStoresUpdatePreparedTransactionId()) {
                THROW_ERROR_EXCEPTION("Cannot prepare stores update since it is already prepared by transaction %v",
                    tablet->GetStoresUpdatePreparedTransactionId());
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

            hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Exclusive);
            // Probably we do not need these during prepare, but why not.
            tablet->UpdateDanglingHunkChunks(hunkChunk);

            structuredLogger->OnHunkChunkStateChanged(hunkChunk);
        }

        // COMPAT(aleksandra-zh)
        if (request->create_hunk_chunks_during_prepare()) {
            for (const auto& descriptor : request->hunk_chunks_to_add()) {
                auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());

                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (!hunkChunk) {
                    hunkChunk = CreateHunkChunk(tablet, chunkId, &descriptor);
                    hunkChunk->Initialize();
                    tablet->AddHunkChunk(hunkChunk);
                }

                hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Shared);
                tablet->UpdateDanglingHunkChunks(hunkChunk);

                YT_LOG_DEBUG(
                    "Hunk chunk added (%v, ChunkId: %v)",
                    tablet->GetLoggingTag(),
                    chunkId);
            }
        }

        THashSet<TChunkId> existingReferencedHunks;
        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
                descriptor.chunk_meta().extensions()))
            {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunk(chunkId);
                        tablet->UpdatePreparedStoreRefCount(hunkChunk, +1);

                        if (!existingReferencedHunks.contains(chunkId)) {
                            hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Shared);
                            tablet->UpdateDanglingHunkChunks(hunkChunk);
                            existingReferencedHunks.insert(chunkId);
                        }
                    }
                }
            }
        }

        // COMPAT(ifsmirnov)
        if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
            tablet->SetStoresUpdatePreparedTransactionId(transaction->GetId());
        }

        // TODO(ifsmirnov): log preparation errors as well.
        structuredLogger->OnTabletStoresUpdatePrepared(
            storeIdsToAdd,
            storeIdsToRemove,
            updateReason,
            transaction->GetId());

        YT_LOG_INFO("Tablet stores update prepared "
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

    void HydraPrepareBoggleHunkTabletStoreLock(
        TTransaction* /*transaction*/,
        TReqBoggleHunkTabletStoreLock* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& /*options*/)
    {
        const auto* context = GetCurrentMutationContext();
        // TODO(aleksandra-zh): maybe move that validation to Hydra some day.
        if (context->GetTerm() != request->term()) {
            THROW_ERROR_EXCEPTION("Request term %v does not match mutation term %v",
                request->term(),
                context->GetTerm());
        }

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto hunkStoreId = FromProto<THunkStoreId>(request->store_id());
        auto lock = request->lock();
        if (lock) {
            return;
        }

        const auto& hunkLockManager = tablet->GetHunkLockManager();
        auto lockCount = hunkLockManager->GetPersistentLockCount(hunkStoreId);
        if (lockCount > 0) {
            THROW_ERROR_EXCEPTION("Hunk store %v has positive lock count %v",
                hunkStoreId,
                lockCount);
        }

        // Set transient flags and create futures once again if we are in recovery,
        // as they were lost.
        hunkLockManager->OnBoggleLockPrepared(hunkStoreId, lock);
    }

    void HydraCommitBoggleHunkTabletStoreLock(
        TTransaction* /*transaction*/,
        TReqBoggleHunkTabletStoreLock* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
    {
        const auto* context = GetCurrentMutationContext();
        YT_VERIFY(context->GetTerm() == request->term());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto hunkCellId = FromProto<TCellId>(request->hunk_cell_id());
        auto hunkTabletId = FromProto<TTabletId>(request->hunk_tablet_id());
        auto hunkMountRevision = request->mount_revision();
        auto hunkStoreId = FromProto<THunkStoreId>(request->store_id());
        auto lock = request->lock();

        const auto& hunkLockManager = tablet->GetHunkLockManager();
        if (lock) {
            hunkLockManager->RegisterHunkStore(hunkStoreId, hunkCellId, hunkTabletId, hunkMountRevision);
        } else {
            hunkLockManager->UnregisterHunkStore(hunkStoreId);
            CheckIfTabletFullyFlushed(tablet);
        }
    }

    void HydraAbortBoggleHunkTabletStoreLock(
        TTransaction* /*transaction*/,
        TReqBoggleHunkTabletStoreLock* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        const auto* context = GetCurrentMutationContext();
        if (context->GetTerm() != request->term()) {
            // We do not need to discard transient flags in that case, as they were discarded during restart.
            return;
        }

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto lock = request->lock();
        auto hunkStoreId = FromProto<THunkStoreId>(request->store_id());

        const auto& hunkLockManager = tablet->GetHunkLockManager();
        hunkLockManager->OnBoggleLockAborted(hunkStoreId, lock);
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

    void HydraAbortUpdateTabletStores(
        TTransaction* transaction,
        TReqUpdateTabletStores* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto reason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        if (transaction->IsExternalizedToThisCell()) {
            YT_LOG_DEBUG("Aborting tablet stores update under externalized transaction "
                "(TransactionId: %v, TabletId: %v, Reason: %v)",
                transaction->GetId(),
                tabletId,
                reason);
        }

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto actualMountRevision = request->mount_revision();
        auto expectedMountRevision = tablet->IsActiveServant()
            ? tablet->GetMountRevision()
            : tablet->SmoothMovementData().GetSiblingMountRevision();
        if (actualMountRevision != expectedMountRevision) {
            return;
        }

        // COMPAT(ifsmirnov)
        if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
            auto expectedTransactionId = tablet->GetStoresUpdatePreparedTransactionId();

            if (expectedTransactionId == transaction->GetId()) {
                tablet->SetStoresUpdatePreparedTransactionId({});
            } else {
                // This is fine because out-of-order aborts may come for transactions
                // that were not even prepared.
                YT_LOG_DEBUG("Unexpected stores update transaction aborted, ignored "
                    "(%v, TransactionId: %v, PreparedTransactionId: %v)",
                    tablet->GetLoggingTag(),
                    transaction->GetId(),
                    expectedTransactionId);

                // Continue nevertheless to mimic old behaviour.
            }
        }

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            InsertOrCrash(hunkChunkIdsToAdd, chunkId);
        }

        // COMPAT(aleksandra-zh)
        if (request->create_hunk_chunks_during_prepare()) {
            for (auto chunkId : hunkChunkIdsToAdd) {
                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (!hunkChunk) {
                    continue;
                }
                hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Shared);

                if (!hunkChunk->GetCommitted() && hunkChunk->IsDangling()) {
                    YT_LOG_DEBUG("Removing dangling uncommitted hunk chunk (HunkChunkId: %v)",
                        hunkChunk->GetId());
                    // This hunk chunk was never attached in master, so just remove it here without 2pc.
                    tablet->RemoveHunkChunk(hunkChunk);
                    hunkChunk->SetState(EHunkChunkState::Removed);
                }
            }
        }

        THashSet<TChunkId> existingReferencedHunks;
        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
                descriptor.chunk_meta().extensions()))
            {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->FindHunkChunk(chunkId);
                        if (!hunkChunk) {
                            continue;
                        }

                        tablet->UpdatePreparedStoreRefCount(hunkChunk, -1);

                        if (!existingReferencedHunks.contains(chunkId)) {
                            hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Shared);
                            tablet->UpdateDanglingHunkChunks(hunkChunk);
                            existingReferencedHunks.insert(chunkId);
                        }
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

            hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);
            tablet->UpdateDanglingHunkChunks(hunkChunk);
        }

        CheckIfTabletFullyFlushed(tablet);
        Slot_->GetSmoothMovementTracker()->CheckTablet(tablet);

        YT_LOG_INFO("Tablet stores update aborted "
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

    void HydraCommitUpdateTabletStores(
        TTransaction* transaction,
        TReqUpdateTabletStores* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto reason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        if (transaction->IsExternalizedToThisCell()) {
            YT_LOG_DEBUG("Committing tablet stores update under externalized transaction "
                "(TransactionId: %v, TabletId: %v, Reason: %v)",
                transaction->GetId(),
                tabletId,
                reason);
        }

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto actualMountRevision = request->mount_revision();
        auto expectedMountRevision = tablet->IsActiveServant()
            ? tablet->GetMountRevision()
            : tablet->SmoothMovementData().GetSiblingMountRevision();
        if (actualMountRevision != expectedMountRevision) {
            return;
        }

        // COMPAT(ifsmirnov)
        if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
            auto expectedTransactionId = tablet->GetStoresUpdatePreparedTransactionId();
            if (expectedTransactionId != transaction->GetId()) {
                YT_LOG_ALERT("Unexpected stores update transaction committed "
                    "(%v, TransactionId: %v, PreparedTransactionId: %v)",
                    tablet->GetLoggingTag(),
                    transaction->GetId(),
                    expectedTransactionId);

                // Continue nevertheless to mimic old behaviour.
            }
        }

        tablet->SetStoresUpdatePreparedTransactionId({});

        if (auto discardStoresRevision = tablet->GetLastDiscardStoresRevision()) {
            auto prepareRevision = transaction->GetPrepareRevision();
            if (prepareRevision < discardStoresRevision) {
                YT_LOG_DEBUG(
                    "Tablet stores update commit interrupted by stores discard, ignored "
                    "(%v, TransactionId: %v, DiscardStoresRevision: %x, "
                    "PrepareUpdateTabletStoresRevision: %x)",
                    tablet->GetLoggingTag(),
                    transaction->GetId(),
                    discardStoresRevision,
                    prepareRevision);

                // Validate that all prepared-for-removal stores were indeed discarded.
                for (const auto& descriptor : request->stores_to_remove()) {
                    auto storeId = FromProto<TStoreId>(descriptor.store_id());
                    if (const auto& store = tablet->FindStore(storeId)) {
                        YT_LOG_ALERT(
                            "Store prepared for removal was not discarded while tablet "
                            "stores update commit was interrupted by the discard "
                            "(%v, StoreId: %v, TransactionId: %v, DiscardStoresRevision: %x, "
                            "PrepareUpdateTabletStoresRevision: %x)",
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

        std::vector<TChunkId> compressionDictionaryIds;
        THashSet<THunkChunkPtr> addedHunkChunks;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            if (request->create_hunk_chunks_during_prepare()) {
                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (!hunkChunk) {
                    YT_LOG_ALERT("Hunk chunk is missing (%v, ChunkId: %v)",
                        tablet->GetLoggingTag(),
                        chunkId);
                    continue;
                }

                hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Shared);
                hunkChunk->SetCommitted(true);

                // This one is also useless.
                tablet->UpdateDanglingHunkChunks(hunkChunk);

                InsertOrCrash(addedHunkChunks, hunkChunk);
            } else {
                auto hunkChunk = CreateHunkChunk(tablet, chunkId, &descriptor);
                hunkChunk->SetCommitted(true);

                hunkChunk->Initialize();
                tablet->AddHunkChunk(hunkChunk);

                YT_LOG_DEBUG("Hunk chunk added (%v, ChunkId: %v)",
                    tablet->GetLoggingTag(),
                    chunkId);
                InsertOrCrash(addedHunkChunks, hunkChunk);
            }

            if (auto miscExt = FindProtoExtension<TMiscExt>(descriptor.chunk_meta().extensions())) {
                if (miscExt->has_dictionary_compression_policy()) {
                    auto policy = CheckedEnumCast<EDictionaryCompressionPolicy>(
                        miscExt->dictionary_compression_policy());
                    tablet->AttachCompressionDictionary(policy, chunkId);
                    compressionDictionaryIds.push_back(chunkId);
                }
            }
        }

        std::vector<TStoreId> removedStoreIds;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            removedStoreIds.push_back(storeId);

            auto store = tablet->GetStore(storeId);
            storeManager->RemoveStore(store);

            YT_LOG_DEBUG("Store removed (%v, StoreId: %v, DynamicMemoryUsage: %v)",
                tablet->GetLoggingTag(),
                storeId,
                store->GetDynamicMemoryUsage());

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, -1);

                    const auto& hunkChunk = ref.HunkChunk;

                    YT_LOG_DEBUG("Hunk chunk unreferenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
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
            hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);

            YT_LOG_DEBUG("Hunk chunk removed (%v, ChunkId: %v)",
                tablet->GetLoggingTag(),
                chunkId);
        }

        std::vector<IStorePtr> addedStores;
        THashSet<THunkChunkPtr> existingReferencedHunks;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storeManager->AddStore(store, /*onMount*/ false, /*onFlush*/ updateReason == ETabletStoresUpdateReason::Flush);
            addedStores.push_back(store);

            TStoreId backingStoreId;
            if (!IsRecovery() &&
                descriptor.has_backing_store_id() &&
                IsBackingStoreRequired(tablet) &&
                tablet->IsActiveServant())
            {
                backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                const auto& backingStore = GetOrCrash(idToBackingStore, backingStoreId);
                SetBackingStore(tablet, store, backingStore);
            }

            YT_LOG_DEBUG("Chunk store added (%v, StoreId: %v, MaxTimestamp: %v, BackingStoreId: %v)",
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

                        if (!existingReferencedHunks.contains(hunkChunk)) {
                            hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Shared);
                            tablet->UpdateDanglingHunkChunks(hunkChunk);
                            existingReferencedHunks.insert(hunkChunk);
                        }
                    }

                    YT_LOG_DEBUG("Hunk chunk referenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
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
            YT_LOG_DEBUG("Dynamic store id added to the pool (%v, StoreId: %v)",
                tablet->GetLoggingTag(),
                storeId);

            allocatedDynamicStoreId = storeId;
        }

        auto& movementData = tablet->SmoothMovementData();
        bool isCommonFlush = updateReason == ETabletStoresUpdateReason::Flush &&
            movementData.CommonDynamicStoreIds().contains(removedStoreIds[0]);

        if (isCommonFlush) {
            movementData.CommonDynamicStoreIds().erase(removedStoreIds[0]);
        }

        YT_LOG_INFO("Tablet stores update committed "
            "(%v, TransactionId: %v, AddedStoreIds: %v, RemovedStoreIds: %v, AddedHunkChunkIds: %v, RemovedHunkChunkIds: %v, "
            "AddedCompressionDictionaryIds: %v, RetainedTimestamp: %v, UpdateReason: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId(),
            MakeFormattableView(addedStores, TStoreIdFormatter()),
            removedStoreIds,
            MakeFormattableView(addedHunkChunks, THunkChunkIdFormatter()),
            removedHunkChunkIds,
            compressionDictionaryIds,
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
        Slot_->GetSmoothMovementTracker()->CheckTablet(tablet);

        FetchCompactionHints(tablet, {{addedStores.begin(), addedStores.end()}});
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
            YT_LOG_INFO("Partition split failed (%v, PartitionId: %v, Keys: %v)",
                tablet->GetLoggingTag(),
                partitionId,
                JoinToString(pivotKeys, TStringBuf(" .. ")));
            return;
        }

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO("Partition split (%v, OriginalPartitionId: %v, "
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

        YT_LOG_INFO("Partitions merged (%v, OriginalPartitionIds: %v, "
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

        auto reader = CreateWireProtocolReader(TSharedRef::FromString(request->sample_keys()));
        auto sampleKeys = reader->ReadUnversionedRowset(true);

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->UpdatePartitionSampleKeys(partition, sampleKeys);

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO("Partition sample keys updated (%v, PartitionId: %v, SampleKeyCount: %v)",
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

        YT_LOG_INFO("Table replica updated (%v, ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId(),
            enabled,
            mode,
            atomicity,
            preserveTimestamps);
    }

    void HydraPrepareWritePulledRows(
        TTransaction* transaction,
        TReqWritePulledRows* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        ui64 round = request->replication_round();
        auto* tablet = GetTabletOrThrow(tabletId);

        const auto& chaosData = tablet->ChaosData();
        auto replicationRound = chaosData->ReplicationRound.load();
        if (replicationRound != round) {
            THROW_ERROR_EXCEPTION("Replication round mismatch: expected %v, got %v",
                replicationRound,
                round);
        }

        if (IsInUnmountWorkflow(tablet->GetState())) {
            THROW_ERROR_EXCEPTION("Cannot write pulled rows since tablet is in %Qlv state",
                tablet->GetState());
        }

        if (chaosData->PreparedWritePulledRowsTransactionId) {
            THROW_ERROR_EXCEPTION("Another pulled rows write is in progress")
                << TErrorAttribute("transaction_id", transaction->GetId())
                << TErrorAttribute("write_pull_rows_transaction_id", chaosData->PreparedWritePulledRowsTransactionId);
        }
        chaosData->PreparedWritePulledRowsTransactionId = transaction->GetId();

        const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
        tabletCellWriteManager->AddPersistentAffectedTablet(transaction, tablet);

        YT_LOG_DEBUG("Write pulled rows prepared (TabletId: %v, TransactionId: %v, ReplicationRound: %v)",
            tabletId,
            transaction->GetId(),
            round);
    }

    void HydraCommitWritePulledRows(
        TTransaction* transaction,
        TReqWritePulledRows* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        ui64 round = request->replication_round();
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (transaction->IsSerializationNeeded()) {
            YT_LOG_DEBUG("Write pull rows committed and is waiting for serialization (TabletId: %v, TransactionId: %v, ReplicationRound: %v)",
                tabletId,
                transaction->GetId(),
                round);
            return;
        }

        FinalizeWritePulledRows(transaction, request, true);
    }

    void HydraSerializeWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request)
    {
        FinalizeWritePulledRows(transaction, request, false);
    }

    void FinalizeWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request, bool inCommit)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        ui64 round = request->replication_round();
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& chaosData = tablet->ChaosData();
        if (chaosData->PreparedWritePulledRowsTransactionId != transaction->GetId()) {
            YT_LOG_ALERT(
                "Unexpected write pull rows transaction finalized, ignored "
                "(TransactionId: %v, ExpectedTransactionId: %v, TabletId: %v)",
                transaction->GetId(),
                chaosData->PreparedWritePulledRowsTransactionId,
                tablet->GetId());
            return;
        }

        chaosData->PreparedWritePulledRowsTransactionId = NullTransactionId;

        auto replicationRound = chaosData->ReplicationRound.load();
        YT_VERIFY(replicationRound == round);

        auto tabletProgress = tablet->RuntimeData()->ReplicationProgress.Acquire();

        auto progress = New<TRefCountedReplicationProgress>(FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress()));
        bool isStrictlyAdvanced = IsReplicationProgressGreaterOrEqual(*progress, *tabletProgress);
        THashMap<TTabletId, i64> currentReplicationRowIndexes;

        if (isStrictlyAdvanced) {
            tablet->RuntimeData()->ReplicationProgress.Store(progress);
            for (auto protoEndReplicationRowIndex : request->new_replication_row_indexes()) {
                auto tabletId = FromProto<TTabletId>(protoEndReplicationRowIndex.tablet_id());
                auto endReplicationRowIndex = protoEndReplicationRowIndex.replication_row_index();
                YT_VERIFY(currentReplicationRowIndexes.insert(std::pair(tabletId, endReplicationRowIndex)).second);
            }

            chaosData->CurrentReplicationRowIndexes.Store(currentReplicationRowIndexes);

            YT_LOG_DEBUG("Write pulled rows %v (TabletId: %v, TransactionId: %v, ReplicationProgress: %v, "
                "ReplicationRowIndexes: %v, NewReplicationRound: %v)",
                inCommit ? "committed" : "serialized",
                tabletId,
                transaction->GetId(),
                static_cast<NChaosClient::TReplicationProgress>(*progress),
                currentReplicationRowIndexes,
                replicationRound + 1);
        } else {
            YT_LOG_ALERT("Skip writing pulled rows due to not strictly advanced progress %v "
                "(TabletId: %v, TransactionId: %v, NewReplicationProgress: %v, TabletProgress: %v, "
                "ReplicationRowIndexes: %v, NewReplicationRound: %v)",
                inCommit ? "committed" : "serialized",
                tabletId,
                transaction->GetId(),
                static_cast<NChaosClient::TReplicationProgress>(*progress),
                static_cast<NChaosClient::TReplicationProgress>(*tabletProgress),
                currentReplicationRowIndexes,
                replicationRound + 1);
        }


        chaosData->ReplicationRound = round + 1;
    }

    void HydraAbortWritePulledRows(
        TTransaction* transaction,
        TReqWritePulledRows* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& chaosData = tablet->ChaosData();
        auto& expectedTransactionId = chaosData->PreparedWritePulledRowsTransactionId;
        if (expectedTransactionId != transaction->GetId()) {
            return;
        }

        expectedTransactionId = NullTransactionId;

        YT_LOG_DEBUG("Write pulled rows aborted (TabletId: %v, TransactionId: %v)",
            tabletId,
            transaction->GetId());
    }

    void HydraPrepareAdvanceReplicationProgress(
        TTransaction* transaction,
        TReqAdvanceReplicationProgress* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        // COMPAT(savrus)
        auto round = request->has_replication_round()
            ? std::make_optional(request->replication_round())
            : std::nullopt;
        auto* tablet = GetTabletOrThrow(tabletId);
        auto newProgress = FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress());

        const auto& chaosData = tablet->ChaosData();
        auto replicationRound = chaosData->ReplicationRound.load();
        // COMPAT(savrus)
        if (round && replicationRound != *round) {
            THROW_ERROR_EXCEPTION("Replication round mismatch: expected %v, got %v",
                replicationRound,
                round);
        }

        auto progress = tablet->RuntimeData()->ReplicationProgress.Acquire();
        if (!IsReplicationProgressGreaterOrEqual(newProgress, *progress)) {
            THROW_ERROR_EXCEPTION("Tablet %v replication progress is not strictly behind",
                tabletId);
        }

        if (IsInUnmountWorkflow(tablet->GetState())) {
            THROW_ERROR_EXCEPTION("Cannot advance replication progress since tablet is in %Qlv state",
                tablet->GetState());
        }

        if (chaosData->PreparedAdvanceReplicationProgressTransactionId) {
            THROW_ERROR_EXCEPTION("Another replication progress advance is in progress")
                << TErrorAttribute("transaction_id", transaction->GetId())
                << TErrorAttribute("advance_replication_progress_transaction_id", chaosData->PreparedAdvanceReplicationProgressTransactionId);
        }
        chaosData->PreparedAdvanceReplicationProgressTransactionId = transaction->GetId();

        const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
        tabletCellWriteManager->AddPersistentAffectedTablet(transaction, tablet);

        transaction->ForceSerialization(tabletId);

        YT_LOG_DEBUG("Prepared replication progress advance transaction (TabletId: %v, TransactionId: %v)",
            tabletId,
            transaction->GetId());
    }

    void HydraSerializeAdvanceReplicationProgress(TTransaction* transaction, TReqAdvanceReplicationProgress* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        // COMPAT(savrus)
        auto round = request->has_replication_round()
            ? std::make_optional(request->replication_round())
            : std::nullopt;
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& chaosData = tablet->ChaosData();
        auto replicationRound = chaosData->ReplicationRound.load();
        YT_VERIFY(!round || replicationRound == *round);

        if (chaosData->PreparedAdvanceReplicationProgressTransactionId != transaction->GetId()) {
            YT_LOG_ALERT(
                "Unexpected replication progress advance transaction serialized, ignored "
                "(TransactionId: %v, ExpectedTransactionId: %v, TabletId: %v)",
                transaction->GetId(),
                chaosData->PreparedAdvanceReplicationProgressTransactionId,
                tablet->GetId());
            return;
        }

        chaosData->PreparedAdvanceReplicationProgressTransactionId = NullTransactionId;

        auto progress = New<TRefCountedReplicationProgress>(FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress()));
        bool validateStrictAdvance = request->validate_strict_advance();

        // NB: It is legitimate for `progress` to be less than `tabletProgress`: tablet progress could have been
        // updated by some recent transaction while `progress` has been constructed even before `transaction` started.
        auto tabletProgress = tablet->RuntimeData()->ReplicationProgress.Acquire();
        bool isStrictlyAdvanced = IsReplicationProgressGreaterOrEqual(*progress, *tabletProgress);

        YT_LOG_DEBUG("Serializing advance replication progress transaction "
            "(TabletId: %v, TransactionId: %v, IsStrictlyAdvanced: %v, CurrentProgress: %v, NewProgress: %v, ReplicationRound: %v)",
            tabletId,
            transaction->GetId(),
            isStrictlyAdvanced,
            static_cast<NChaosClient::TReplicationProgress>(*tabletProgress),
            static_cast<NChaosClient::TReplicationProgress>(*progress),
            round);

        if (isStrictlyAdvanced) {
            tablet->RuntimeData()->ReplicationProgress.Store(progress);

            YT_LOG_DEBUG("Updated tablet replication progress (TabletId: %v, TransactionId: %v, ReplicationProgress: %v)",
                tabletId,
                transaction->GetId(),
                static_cast<NChaosClient::TReplicationProgress>(*progress));
        } else if (validateStrictAdvance) {
            YT_LOG_ALERT("Failed to advance tablet replication progress because current tablet progress is greater (TabletId: %v, TransactionId: %v, CurrentProgress: %v, NewProgress: %v)",
                tabletId,
                transaction->GetId(),
                static_cast<NChaosClient::TReplicationProgress>(*tabletProgress),
                static_cast<NChaosClient::TReplicationProgress>(*progress));
        }

        // COMPAT(savrus)
        if (round) {
            chaosData->ReplicationRound = *round + 1;
        }

        YT_LOG_DEBUG("Serialized replication progress advance transaction (TabletId: %v, TransactionId: %v)",
            tabletId,
            transaction->GetId());
    }

    void HydraAbortAdvanceReplicationProgress(
        TTransaction* transaction,
        TReqAdvanceReplicationProgress* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& chaosData = tablet->ChaosData();
        auto& expectedTransactionId = chaosData->PreparedAdvanceReplicationProgressTransactionId;
        if (expectedTransactionId != transaction->GetId()) {
            return;
        }

        expectedTransactionId = NullTransactionId;

        YT_LOG_DEBUG(
            "Replication progress advance aborted (TabletId: %v, TransactionId: %v)",
            tabletId,
            transaction->GetId());
    }

    void HydraPrepareReplicateRows(
        TTransaction* transaction,
        TReqReplicateRows* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->GetReplicaInfoOrThrow(replicaId);

        if (replicaInfo->GetState() != ETableReplicaState::Enabled) {
            THROW_ERROR_EXCEPTION("Replica %v is in %Qlv state",
                replicaId,
                replicaInfo->GetState());
        }

        if (IsInUnmountWorkflow(tablet->GetState())) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows replication since tablet is in %Qlv state",
                tablet->GetState());
        }

        if (replicaInfo->GetPreparedReplicationTransactionId()) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v since these are already "
                "prepared by transaction %v",
                transaction->GetId(),
                replicaId,
                tabletId,
                replicaInfo->GetPreparedReplicationTransactionId());
        }

        if (auto checkpointTimestamp = tablet->GetBackupCheckpointTimestamp()) {
            if (transaction->GetStartTimestamp() >= checkpointTimestamp) {
                THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v since tablet %v participates in backup",
                    replicaId,
                    tabletId)
                    << TErrorAttribute("checkpoint_timestamp", checkpointTimestamp)
                    << TErrorAttribute("start_timestamp", transaction->GetStartTimestamp());
            }
        }

        if (auto lastPassedCheckpointTimestamp = tablet->BackupMetadata().GetLastPassedCheckpointTimestamp()) {
            if (transaction->GetStartTimestamp() <= lastPassedCheckpointTimestamp) {
                THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v since tablet %v has passed "
                    "backup checkpoint exceeding transaction start timestamp",
                    replicaId,
                    tabletId)
                    << TErrorAttribute("last_passed_checkpoint_timestamp", lastPassedCheckpointTimestamp)
                    << TErrorAttribute("start_timestamp", transaction->GetStartTimestamp());
            }
        }

        if (tablet->GetBackupStage() == EBackupStage::AwaitingReplicationFinish) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v since tablet %v is in backup stage %Qlv",
                replicaId,
                tabletId,
                tablet->GetBackupStage());
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

        const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
        tabletCellWriteManager->AddPersistentAffectedTablet(transaction, tablet);

        YT_LOG_DEBUG("Async replicated rows prepared (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %v -> %v)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            newReplicationRowIndex,
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            newReplicationTimestamp);
    }

    void HydraCommitReplicateRows(
        TTransaction* transaction,
        TReqReplicateRows* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
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

        if (replicaInfo->GetPreparedReplicationTransactionId() != transaction->GetId()) {
            YT_LOG_ALERT(
                "Unexpected replication transaction finalized, ignored "
                "(TransactionId: %v, ExpectedTransactionId: %v, TabletId: %v)",
                transaction->GetId(),
                replicaInfo->GetPreparedReplicationTransactionId(),
                tablet->GetId());
            return;
        }

        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        BackupManager_->ValidateReplicationTransactionCommit(tablet, transaction);

        // COMPAT(babenko)
        if (request->has_prev_replication_row_index()) {
            YT_VERIFY(replicaInfo->GetCurrentReplicationRowIndex() == request->prev_replication_row_index());
        }
        YT_VERIFY(replicaInfo->GetPreparedReplicationRowIndex() == request->new_replication_row_index());
        replicaInfo->SetPreparedReplicationRowIndex(-1);

        auto prevCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
        auto prevCommittedReplicationRowIndex = replicaInfo->GetCommittedReplicationRowIndex();
        auto prevCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();

        auto newCurrentReplicationRowIndex = request->new_replication_row_index();
        auto newCurrentReplicationTimestamp = request->new_replication_timestamp();

        if (newCurrentReplicationRowIndex < prevCurrentReplicationRowIndex) {
            YT_LOG_ALERT("CurrentReplicationIndex went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationRowIndex: %v -> %v)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationRowIndex,
                newCurrentReplicationRowIndex);
            newCurrentReplicationRowIndex = prevCurrentReplicationRowIndex;
        }
        if (newCurrentReplicationTimestamp < prevCurrentReplicationTimestamp) {
            YT_LOG_ALERT("CurrentReplicationTimestamp went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationTimestamp: %v -> %v)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationTimestamp,
                newCurrentReplicationTimestamp);
            newCurrentReplicationTimestamp = prevCurrentReplicationTimestamp;
        }

        replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCommittedReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);
        replicaInfo->RecomputeReplicaStatus();

        AdvanceReplicatedTrimmedRowCount(tablet, transaction);

        YT_LOG_DEBUG("Async replicated rows committed (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, CommittedReplicationRowIndex: %v -> %v, CurrentReplicationTimestamp: %v -> %v, "
            "TrimmedRowCount: %v -> %v, TotalRowCount: %v)",
            tabletId,
            replicaId,
            transaction->GetId(),
            prevCurrentReplicationRowIndex,
            replicaInfo->GetCurrentReplicationRowIndex(),
            prevCommittedReplicationRowIndex,
            replicaInfo->GetCommittedReplicationRowIndex(),
            prevCurrentReplicationTimestamp,
            replicaInfo->GetCurrentReplicationTimestamp(),
            prevTrimmedRowCount,
            tablet->GetTrimmedRowCount(),
            tablet->GetTotalRowCount());

        ReplicationTransactionFinished_.Fire(tablet, replicaInfo);
    }

    void HydraAbortReplicateRows(
        TTransaction* transaction,
        TReqReplicateRows* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
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

        YT_LOG_DEBUG("Async replicated rows aborted (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %v -> %v)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            request->new_replication_row_index(),
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            request->new_replication_timestamp());

        ReplicationTransactionFinished_.Fire(tablet, replicaInfo);
    }

    void HydraDecommissionTabletCell(TReqDecommissionTabletCellOnNode* /*request*/)
    {
        YT_LOG_INFO("Tablet cell is decommissioning");

        CellLifeStage_ = ETabletCellLifeStage::DecommissioningOnNode;
        SetTabletCellSuspend(/*suspend*/ true);

        Slot_->GetTransactionManager()->SetRemoving();
    }

    void HydraSuspendTabletCell(NTabletServer::NProto::TReqSuspendTabletCell* /*request*/)
    {
        YT_VERIFY(HasHydraContext());

        YT_LOG_INFO("Suspending tablet cell");

        SetTabletCellSuspend(/*suspend*/ true);
        Suspending_ = true;
    }

    void HydraResumeTabletCell(NTabletServer::NProto::TReqResumeTabletCell* /*request*/)
    {
        YT_VERIFY(HasHydraContext());

        YT_LOG_INFO("Resuming tablet cell");

        SetTabletCellSuspend(/*suspend*/ false);
        Suspending_ = false;

        PostTabletCellSuspensionToggledMessage(/*suspended*/ false);
    }

    void SetTabletCellSuspend(bool suspend)
    {
        YT_VERIFY(HasHydraContext());

        Slot_->GetTransactionManager()->SetDecommission(suspend);
        Slot_->GetTransactionSupervisor()->SetDecommission(suspend);
        Slot_->GetLeaseManager()->SetDecommission(suspend);
    }

    void PostTabletCellSuspensionToggledMessage(bool suspended)
    {
        YT_VERIFY(HasHydraContext());

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = Slot_->GetMasterMailbox();
        TRspOnTabletCellSuspensionToggled response;
        ToProto(response.mutable_cell_id(), Slot_->GetCellId());
        response.set_suspended(suspended);
        hiveManager->PostMessage(mailbox, response);
    }

    void OnCheckTabletCellDecommission()
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        if (Slot_->GetDynamicOptions()->SuppressTabletCellDecommission.value_or(false)) {
            return;
        }

        auto transactionManagerDecommissioned = Slot_->GetTransactionManager()->IsDecommissioned();
        auto transactionSupervisorDecommissioned = Slot_->GetTransactionSupervisor()->IsDecommissioned();
        auto leaseManagerDecommissioned = Slot_->GetLeaseManager()->IsFullyDecommissioned();

        YT_LOG_INFO("Checking if tablet cell is decommissioned "
            "(LifeStage: %v, TabletMapEmpty: %v, TransactionManagerDecommissined: %v, "
            "TransactionSupervisorDecommissioned: %v, LeaseManagerDecommissioned: %v)",
            transactionManagerDecommissioned,
            transactionSupervisorDecommissioned,
            leaseManagerDecommissioned);

        if (!TabletMap_.empty() ||
            !transactionManagerDecommissioned ||
            !transactionSupervisorDecommissioned ||
            !leaseManagerDecommissioned)
        {
            return;
        }

        YT_UNUSED_FUTURE(CreateMutation(Slot_->GetHydraManager(), TReqOnTabletCellDecommissioned())
            ->CommitAndLog(Logger));
    }

    void HydraOnTabletCellDecommissioned(TReqOnTabletCellDecommissioned* /*request*/)
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        YT_LOG_INFO("Tablet cell decommissioned");

        CellLifeStage_ = ETabletCellLifeStage::Decommissioned;

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = Slot_->GetMasterMailbox();
        TRspDecommissionTabletCellOnNode response;
        ToProto(response.mutable_cell_id(), Slot_->GetCellId());
        hiveManager->PostMessage(mailbox, response);
    }

    void OnCheckTabletCellSuspension()
    {
        if (!Suspending_) {
            return;
        }

        auto transactionManagerDecommissioned = Slot_->GetTransactionManager()->IsDecommissioned();
        auto transactionSupervisorDecommissioned = Slot_->GetTransactionSupervisor()->IsDecommissioned();
        auto leaseManagerDecommissioned = Slot_->GetLeaseManager()->IsFullyDecommissioned();

        YT_LOG_INFO(
            "Checking if tablet cell is suspended"
            "(TransactionManagerDecommissioned: %v, TransactionSupervisorDecommissioned: %v, "
            "LeaseManagerDecommissioned: %v)",
            transactionManagerDecommissioned,
            transactionSupervisorDecommissioned,
            leaseManagerDecommissioned);

        if (!transactionManagerDecommissioned ||
            !transactionSupervisorDecommissioned ||
            !leaseManagerDecommissioned)
        {
            return;
        }

        YT_UNUSED_FUTURE(CreateMutation(Slot_->GetHydraManager(), TReqOnTabletCellSuspended())
            ->CommitAndLog(Logger));
    }

    void HydraOnTabletCellSuspended(TReqOnTabletCellSuspended* /*request*/)
    {
        YT_VERIFY(HasHydraContext());

        YT_LOG_INFO(
            "Tablet cell is suspended (Suspending: %v, TransactionManagerDecommissioned: %v, TransactionSupervisorDecommissioned: %v)",
            Suspending_,
            Slot_->GetTransactionManager()->IsDecommissioned(),
            Slot_->GetTransactionSupervisor()->IsDecommissioned());

        // Double check.
        if (!Suspending_ ||
            !Slot_->GetTransactionManager()->IsDecommissioned() ||
            !Slot_->GetTransactionSupervisor()->IsDecommissioned())
        {
            return;
        }

        Suspending_ = false;
        PostTabletCellSuspensionToggledMessage(/*suspended*/ true);
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
        if (!tablet->IsActiveServant()) {
            return;
        }

        TReqAllocateDynamicStore req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());
        tablet->SetDynamicStoreIdRequested(true);
        PostMasterMessage(tablet, req);
    }

    void HydraOnDynamicStoreAllocated(TRspAllocateDynamicStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        tablet->SetDynamicStoreIdRequested(false);

        auto state = tablet->GetState();
        if (state == ETabletState::Frozen ||
            state == ETabletState::Unmounted ||
            state == ETabletState::Orphaned)
        {
            YT_LOG_DEBUG(
                "Dynamic store id sent to a tablet in a wrong state, ignored (%v, State: %v)",
                tablet->GetLoggingTag(),
                state);
            return;
        }

        auto dynamicStoreId = FromProto<TDynamicStoreId>(request->dynamic_store_id());
        tablet->PushDynamicStoreIdToPool(dynamicStoreId);
        tablet->SetDynamicStoreIdRequested(false);
        UpdateTabletSnapshot(tablet);

        YT_LOG_DEBUG("Dynamic store allocated for a tablet (%v, DynamicStoreId: %v)",
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
            YT_LOG_INFO("Dynamic memory store is orphaned and will be kept "
                "(StoreId: %v, TabletId: %v, LockCount: %v)",
                store->GetId(),
                tablet->GetId(),
                lockCount);
        }
    }

    bool ValidateRowRef(const TSortedDynamicRowRef& rowRef) override
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
            YT_LOG_INFO("Store unlocked and will be dropped (StoreId: %v)",
                store->GetId());
            YT_VERIFY(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }

    const NLeaseServer::ILeaseManagerPtr& GetLeaseManager() const override
    {
        return Slot_->GetLeaseManager();
    }

    TFuture<void> IssueLeases(const std::vector<TLeaseId>& leaseIds) override
    {
        THashMap<TCellTag, std::vector<TLeaseId>> cellTagToLeaseIds;

        const auto& leaseManager = Slot_->GetLeaseManager();
        for (auto leaseId : leaseIds) {
            if (!leaseManager->FindLease(leaseId)) {
                auto cellTag = CellTagFromId(leaseId);
                cellTagToLeaseIds[cellTag].push_back(leaseId);
            }
        }

        const auto& connection = Bootstrap_->GetConnection();
        const auto& hiveManager = Slot_->GetHiveManager();

        std::vector<TFuture<void>> futures;
        futures.reserve(cellTagToLeaseIds.size());
        for (const auto& [cellTag, leaseIds] : cellTagToLeaseIds) {
            auto cellId = connection->GetMasterCellId(cellTag);
            auto masterChannel = connection->GetMasterChannelOrThrow(
                EMasterChannelKind::Leader,
                cellTag);
            TTransactionServiceProxy proxy(std::move(masterChannel));
            auto req = proxy.IssueLeases();
            ToProto(req->mutable_transaction_ids(), leaseIds);
            ToProto(req->mutable_cell_id(), GetCellId());

            auto future = req->Invoke().AsVoid().Apply(BIND([=] {
                return hiveManager->SyncWith(cellId, /*enableBatching*/ true);
            }));
            futures.push_back(std::move(future));
        }

        return AllSucceeded(std::move(futures));
    }

    void SetTabletOrphaned(std::unique_ptr<TTablet> tabletHolder)
    {
        auto id = tabletHolder->GetId();
        tabletHolder->SetState(ETabletState::Orphaned);
        YT_LOG_DEBUG("Tablet is orphaned and will be kept (TabletId: %v, LockCount: %v)",
            id,
            tabletHolder->GetTotalTabletLockCount());
        YT_VERIFY(OrphanedTablets_.emplace(id, std::move(tabletHolder)).second);
    }

    void OnTabletUnlocked(TTablet* tablet)
    {
        CheckIfTabletFullyUnlocked(tablet);
        if (tablet->GetState() == ETabletState::Orphaned && tablet->GetTotalTabletLockCount() == 0) {
            auto id = tablet->GetId();
            YT_LOG_INFO("Tablet unlocked and will be dropped (TabletId: %v)",
                id);
            YT_VERIFY(OrphanedTablets_.erase(id) == 1);
        }
    }

    void OnTabletRowUnlocked(TTablet* tablet) override
    {
        CheckIfTabletFullyUnlocked(tablet);
    }

    ISimpleHydraManagerPtr GetHydraManager() const override
    {
        return Slot_->GetSimpleHydraManager();
    }

    void CheckIfTabletFullyUnlocked(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        if (tablet->GetTotalTabletLockCount() > 0) {
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

        Slot_->GetSmoothMovementTracker()->CheckTablet(tablet);

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

        YT_LOG_INFO("All tablet locks released (%v, NewState: %v)",
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

        if (tablet->GetHunkLockManager()->GetTotalLockedHunkStoreCount() > 0) {
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

        YT_LOG_INFO("All tablet stores flushed (%v, NewState: %v)",
            tablet->GetLoggingTag(),
            newTransientState);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_state(static_cast<int>(newPersistentState));
        Slot_->CommitTabletMutation(request);
    }

    void PostMasterMessage(
        TTablet* tablet,
        const ::google::protobuf::MessageLite& message,
        bool forceCellMailbox = false) override
    {
        // Used in tests only. NB: synchronous sleep is required since we don't expect
        // context switches here.
        if (auto sleepDuration = Config_->SleepBeforePostToMaster) {
            Sleep(*sleepDuration);
        }

        if (!forceCellMailbox) {
            YT_VERIFY(tablet->IsActiveServant());
        }

        auto avenueEndpointId = tablet->GetMasterAvenueEndpointId();
        if (avenueEndpointId && !forceCellMailbox) {
            const auto& hiveManager = Slot_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(avenueEndpointId);
            hiveManager->PostMessage(mailbox, message);
        } else {
            Slot_->PostMasterMessage(tablet->GetId(), message);
        }
    }

    void InitializeTablet(TTablet* tablet)
    {
        auto structuredLogger = Bootstrap_->GetStructuredLogger()->CreateLogger(tablet);
        tablet->SetStructuredLogger(structuredLogger);

        auto storeManager = CreateStoreManager(tablet);
        tablet->SetStoreManager(storeManager);

        tablet->RecomputeNonActiveStoresUnmergedRowCount();
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

        if (auto replicationCardId = tablet->GetReplicationCardId()) {
            StartChaosReplicaEpoch(tablet, replicationCardId);
        }

        if (tablet->GetSettings().MountConfig->PrecacheChunkReplicasOnMount) {
            PrecacheChunkReplicas(tablet);
        }

        tablet->SmoothMovementData().SetStageChangeScheduled(false);

        FetchCompactionHints(tablet);

        YT_VERIFY(tablet->GetTransientTabletLockCount() == 0);
    }

    void PrecacheChunkReplicas(TTablet* tablet)
    {
        std::vector<TChunkId> storeChunkIds;
        storeChunkIds.reserve(std::ssize(tablet->StoreIdMap()));
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            if (store->IsChunk()) {
                storeChunkIds.push_back(store->AsChunk()->GetChunkId());
            }
        }
        auto hunkChunkIds = GetKeys(tablet->HunkChunkMap());

        YT_LOG_DEBUG("Started precaching chunk replicas (StoreChunkCount: %v, HunkChunkCount: %v)",
            storeChunkIds.size(),
            hunkChunkIds.size());

        const auto& chunkReplicaCache = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetChunkReplicaCache();

        auto storeChunkFutures = chunkReplicaCache->GetReplicas(storeChunkIds);
        auto hunkChunkFutures = chunkReplicaCache->GetReplicas(hunkChunkIds);

        auto futures = std::move(storeChunkFutures);
        std::move(hunkChunkFutures.begin(), hunkChunkFutures.end(), std::back_inserter(futures));
        AllSet(std::move(futures))
            .AsVoid()
            .Subscribe(BIND([Logger = Logger] (const TError& /*error*/) {
                YT_LOG_DEBUG("Finished precaching chunk replicas");
            }));
    }

    void StopTabletEpoch(TTablet* tablet)
    {
        ResetCompactionHints(tablet);

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

        if (auto replicationCardId = tablet->GetReplicationCardId()) {
            StopChaosReplicaEpoch(tablet);
        }

        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            tablet->SetDictionaryBuildingInProgress(policy, false);
        }

        tablet->SmoothMovementData().SetStageChangeScheduled(false);
    }


    void StartTableReplicaEpoch(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_VERIFY(!replicaInfo->GetReplicator());

        if (IsLeader()) {
            auto replicator = New<TTableReplicator>(
                Config_,
                tablet,
                replicaInfo,
                Bootstrap_->GetClient()->GetNativeConnection(),
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

    void AddChaosAgent(TTablet* tablet, TReplicationCardId replicationCardId)
    {
        if (tablet->GetChaosAgent()) {
            return;
        }

        tablet->SetChaosAgent(CreateChaosAgent(
            tablet,
            Slot_,
            replicationCardId,
            Bootstrap_->GetClient()->GetNativeConnection()));
        tablet->SetTablePuller(CreateTablePuller(
            Config_,
            tablet,
            Bootstrap_->GetClient()->GetNativeConnection(),
            Slot_,
            Bootstrap_->GetTabletSnapshotStore(),
            CreateSerializedInvoker(Bootstrap_->GetTableReplicatorPoolInvoker()),
            Bootstrap_->GetInThrottler(EWorkloadCategory::SystemTabletReplication)));
    }

    void StartChaosReplicaEpoch(TTablet* tablet, TReplicationCardId replicationCardId)
    {
        if (!IsLeader()) {
            return;
        }

        AddChaosAgent(tablet, replicationCardId);
        tablet->GetChaosAgent()->Enable();
        tablet->GetTablePuller()->Enable();
    }

    void StopChaosReplicaEpoch(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        tablet->GetChaosAgent()->Disable();
        tablet->GetTablePuller()->Disable();
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
            BIND(&TTabletManager::ReleaseBackingStoreWeak, MakeWeak(this), MakeWeak(store))
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
        const auto& storeManager = tablet->GetStoreManager();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("table_id").Value(tablet->GetTableId())
                .Item("state").Value(tablet->GetState())
                .Item("total_lock_count").Value(tablet->GetTotalTabletLockCount())
                .Item("lock_count").DoMapFor(
                    TEnumTraits<ETabletLockType>::GetDomainValues(),
                    [&] (auto fluent, auto lockType) {
                        fluent.Item(FormatEnum(lockType)).Value(tablet->GetTabletLockCount(lockType));
                    })
                .Item("hash_table_size").Value(tablet->GetHashTableSize())
                .Item("overlapping_store_count").Value(tablet->GetOverlappingStoreCount())
                .Item("dynamic_store_count").Value(tablet->GetDynamicStoreCount())
                .Item("retained_timestamp").Value(tablet->GetRetainedTimestamp())
                .Item("last_periodic_rotation_time").Value(storeManager->GetLastPeriodicRotationTime())
                .Item("in_flight_user_mutation_count").Value(tablet->GetInFlightUserMutationCount())
                .Item("in_flight_replicator_mutation_count").Value(tablet->GetInFlightReplicatorMutationCount())
                .Item("pending_user_write_record_count").Value(tablet->GetPendingUserWriteRecordCount())
                .Item("pending_replicator_write_record_count").Value(tablet->GetPendingReplicatorWriteRecordCount())
                .Item("upstream_replica_id").Value(tablet->GetUpstreamReplicaId())
                .Item("replication_card").Value(tablet->RuntimeData()->ReplicationCard.Acquire())
                .Item("replication_progress").Value(tablet->RuntimeData()->ReplicationProgress.Acquire())
                .Item("replication_era").Value(tablet->RuntimeData()->ReplicationEra.load())
                .Item("replication_round").Value(tablet->ChaosData()->ReplicationRound.load())
                .Item("write_mode").Value(tablet->RuntimeData()->WriteMode.load())
                .Item("lsm_statistics")
                    .BeginMap()
                    .Item("pending_compaction_store_count").DoMapFor(
                        TEnumTraits<NLsm::EStoreCompactionReason>::GetDomainValues(),
                        [&] (auto fluent, auto reason) {
                            auto value = tablet->LsmStatistics().PendingCompactionStoreCount[reason];
                            if (reason != NLsm::EStoreCompactionReason::None &&
                                reason != NLsm::EStoreCompactionReason::DiscardByTtl)
                            {
                                fluent
                                    .Item(Format("%lv", reason)).Value(value);
                            }
                        })
                    .EndMap()
                .Do([tablet] (auto fluent) {
                    BuildTableSettingsOrchidYson(tablet->GetSettings(), fluent);
                })
                .Item("raw_settings").BeginMap()
                    .Item("global_patch").Value(tablet->RawSettings().GlobalPatch)
                    .Item("experiments").Value(tablet->RawSettings().Experiments)
                    .Item("provided_config").Value(tablet->RawSettings().Provided.MountConfigNode)
                    .Item("provided_extra_config").Value(tablet->RawSettings().Provided.ExtraMountConfig)
                .EndMap()
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("pivot_key").Value(tablet->GetPivotKey())
                        .Item("next_pivot_key").Value(tablet->GetNextPivotKey())
                        .Item("eden").DoMap(BIND(&TTabletManager::BuildPartitionOrchidYson, Unretained(this), tablet->GetEden()))
                        .Item("partitions").DoListFor(
                            tablet->PartitionList(), [&] (auto fluent, const std::unique_ptr<TPartition>& partition) {
                                fluent
                                    .Item()
                                    .DoMap(BIND(&TTabletManager::BuildPartitionOrchidYson, Unretained(this), partition.get()));
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
                                    .Do(BIND(&TTabletManager::BuildStoreOrchidYson, Unretained(this), store));
                            })
                        .Item("total_row_count").Value(tablet->GetTotalRowCount())
                        .Item("trimmed_row_count").Value(tablet->GetTrimmedRowCount());
                })
                .Item("hunk_chunks").DoMapFor(tablet->HunkChunkMap(), [&] (auto fluent, const auto& pair) {
                    const auto& [chunkId, hunkChunk] = pair;
                    fluent
                        .Item(ToString(chunkId))
                        .Do(BIND(&TTabletManager::BuildHunkChunkOrchidYson, Unretained(this), hunkChunk));
                })
                .Item("hunk_lock_manager").Do(BIND(&IHunkLockManager::BuildOrchid, tablet->GetHunkLockManager()))
                .DoIf(tablet->IsReplicated(), [&] (auto fluent) {
                    fluent
                        .Item("replicas").DoMapFor(
                            tablet->Replicas(),
                            [&] (auto fluent, const auto& pair) {
                                const auto& [replicaId, replica] = pair;
                                fluent
                                    .Item(ToString(replicaId))
                                    .Do(BIND(&TTabletManager::BuildReplicaOrchidYson, Unretained(this), replica));
                            });
                })
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("dynamic_table_locks").DoMap(
                            BIND(&TLockManager::BuildOrchidYson, tablet->GetLockManager()));
                })
                .Item("errors").DoList([&] (auto fluentList) {
                    tablet->RuntimeData()->Errors.ForEachError([&] (const TError& error) {
                        if (!error.IsOK()) {
                            fluentList.Item().Value(error);
                        }
                    });
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
                .DoIf(tablet->SmoothMovementData().GetRole() != ESmoothMovementRole::None, [&] (auto fluent) {
                    fluent
                        .Item("smooth_movement").DoMap(
                            BIND(&TSmoothMovementData::BuildOrchidYson, &tablet->SmoothMovementData()));
                })
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
            .Item("allowed_merge_time").Value(partition->GetAllowedMergeTime())
            .Item("uncompressed_data_size").Value(partition->GetUncompressedDataSize())
            .Item("compressed_data_size").Value(partition->GetCompressedDataSize())
            .Item("unmerged_row_count").Value(partition->GetUnmergedRowCount())
            .Item("stores").DoMapFor(partition->Stores(), [&] (auto fluent, const IStorePtr& store) {
                fluent
                    .Item(ToString(store->GetId()))
                    .Do(BIND(&TTabletManager::BuildStoreOrchidYson, Unretained(this), store));
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
                .Item("committed_replication_row_index").Value(replica.GetCommittedReplicationRowIndex())
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


    template <class TRequest>
    TRawTableSettings DeserializeTableSettings(TRequest* request, TTabletId tabletId)
    {
        // COMPAT(ifsmirnov)
        auto extractTableSettings = [&] () -> const auto& {
            if constexpr (requires { request->table_settings(); }) {
                return request->table_settings();
            } else {
                return request->table_settings_deprecated();
            }
        };
        const auto& tableSettings = extractTableSettings();

        auto extraMountConfigAttributes = tableSettings.has_extra_mount_config_attributes()
            ? ConvertTo<IMapNodePtr>(TYsonString(tableSettings.extra_mount_config_attributes()))
            : nullptr;

        TRawTableSettings settings{
            .Provided = {
                .MountConfigNode = ConvertTo<IMapNodePtr>(TYsonString(tableSettings.mount_config())),
                .ExtraMountConfig = extraMountConfigAttributes,
                .StoreReaderConfig = DeserializeTabletStoreReaderConfig(
                    TYsonString(tableSettings.store_reader_config()), tabletId),
                .HunkReaderConfig = DeserializeTabletHunkReaderConfig(
                    TYsonString(tableSettings.hunk_reader_config()), tabletId),
                .StoreWriterConfig = DeserializeTabletStoreWriterConfig(
                    TYsonString(tableSettings.store_writer_config()), tabletId),
                .StoreWriterOptions = DeserializeTabletStoreWriterOptions(
                    TYsonString(tableSettings.store_writer_options()), tabletId),
                .HunkWriterConfig = DeserializeTabletHunkWriterConfig(
                    TYsonString(tableSettings.hunk_writer_config()), tabletId),
                .HunkWriterOptions = DeserializeTabletHunkWriterOptions(
                    TYsonString(tableSettings.hunk_writer_options()), tabletId)
            },
            // COMPAT(ifsmirnov)
            .GlobalPatch = tableSettings.has_global_patch()
                ? ConvertTo<TTableConfigPatchPtr>(TYsonString(tableSettings.global_patch()))
                : New<TTableConfigPatch>(),
        };

        // COMPAT(ifsmirnov)
        if (tableSettings.has_experiments()) {
            settings.Experiments = ConvertTo<std::map<TString, TTableConfigExperimentPtr>>(
                TYsonString(tableSettings.experiments()));
        }

        return settings;
    }

    TTableMountConfigPtr DeserializeTableMountConfig(
        const TYsonString& str,
        const IMapNodePtr& extraAttributes,
        TTabletId tabletId)
    {
        try {
            if (!extraAttributes) {
                return ConvertTo<TTableMountConfigPtr>(str);
            }

            auto mountConfigMap = ConvertTo<IMapNodePtr>(str);
            auto patchedMountConfigMap = PatchNode(mountConfigMap, extraAttributes);

            try {
                return ConvertTo<TTableMountConfigPtr>(patchedMountConfigMap);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex,
                    "Error deserializing tablet mount config with extra attributes patch (TabletId: %v)",
                    tabletId);
                return ConvertTo<TTableMountConfigPtr>(mountConfigMap);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing tablet mount config (TabletId: %v)",
                tabletId);
            return New<TTableMountConfig>();
        }
    }

    TTabletStoreReaderConfigPtr DeserializeTabletStoreReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing store reader config (TabletId: %v)",
                tabletId);
            return New<TTabletStoreReaderConfig>();
        }
    }

    TTabletHunkReaderConfigPtr DeserializeTabletHunkReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing hunk reader config (TabletId: %v)",
                tabletId);
            return New<TTabletHunkReaderConfig>();
        }
    }

    TTabletStoreWriterConfigPtr DeserializeTabletStoreWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing store writer config (TabletId: %v)",
                tabletId);
            return New<TTabletStoreWriterConfig>();
        }
    }

    TTabletStoreWriterOptionsPtr DeserializeTabletStoreWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing store writer options (TabletId: %v)",
                tabletId);
            return New<TTabletStoreWriterOptions>();
        }
    }

    TTabletHunkWriterConfigPtr DeserializeTabletHunkWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing hunk writer config (TabletId: %v)",
                tabletId);
            return New<TTabletHunkWriterConfig>();
        }
    }

    TTabletHunkWriterOptionsPtr DeserializeTabletHunkWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error deserializing hunk writer options (TabletId: %v)",
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
            Bootstrap_->GetClient());
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
                    CreateBackendChunkReadersHolder(
                        Bootstrap_,
                        Bootstrap_->GetClient(),
                        Bootstrap_->GetLocalDescriptor(),
                        Bootstrap_->GetChunkRegistry(),
                        tablet->GetSettings().StoreReaderConfig));
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
                    Config_,
                    storeId,
                    tablet,
                    descriptor,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetVersionedChunkMetaManager(),
                    CreateBackendChunkReadersHolder(
                        Bootstrap_,
                        Bootstrap_->GetClient(),
                        Bootstrap_->GetLocalDescriptor(),
                        Bootstrap_->GetChunkRegistry(),
                        tablet->GetSettings().StoreReaderConfig));
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
        const TAddHunkChunkDescriptor* descriptor = nullptr)
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
            YT_LOG_WARNING("Requested to add an already existing table replica (TabletId: %v, ReplicaId: %v)",
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

        YT_LOG_INFO("Table replica added (%v, ReplicaId: %v, ClusterName: %v, ReplicaPath: %v, "
            "Mode: %v, StartReplicationTimestamp: %v, CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %v)",
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
            YT_LOG_WARNING("Requested to remove a non-existing table replica (TabletId: %v, ReplicaId: %v)",
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

        YT_LOG_INFO("Table replica removed (%v, ReplicaId: %v)",
            tablet->GetLoggingTag(),
            replicaId);
    }


    void EnableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO("Table replica enabled (%v, ReplicaId: %v)",
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
            PostMasterMessage(tablet, response);
        }
    }

    void DisableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO("Table replica disabled (%v, ReplicaId: %v, "
            "CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %v)",
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
            PostMasterMessage(tablet, response);
        }
    }

    void PostTableReplicaStatistics(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        TReqUpdateTableReplicaStatistics request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_replica_id(), replicaInfo.GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        replicaInfo.PopulateStatistics(request.mutable_statistics());
        PostMasterMessage(tablet, request);
    }


    void UpdateTrimmedRowCount(TTablet* tablet, i64 trimmedRowCount)
    {
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();
        if (trimmedRowCount <= prevTrimmedRowCount) {
            return;
        }
        tablet->SetTrimmedRowCount(trimmedRowCount);

        if (tablet->IsActiveServant()) {
            TReqUpdateTabletTrimmedRowCount masterRequest;
            ToProto(masterRequest.mutable_tablet_id(), tablet->GetId());
            masterRequest.set_mount_revision(tablet->GetMountRevision());
            masterRequest.set_trimmed_row_count(trimmedRowCount);
            PostMasterMessage(tablet, masterRequest);
        }

        YT_LOG_DEBUG("Rows trimmed (TabletId: %v, TrimmedRowCount: %v -> %v)",
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

    const IBackupManagerPtr& GetBackupManager() const override
    {
        return BackupManager_;
    }


    TFuture<void> OnStoresUpdateCommitSemaphoreAcquired(
        TTablet* tablet,
        const ITransactionPtr& transaction,
        TAsyncSemaphoreGuard&&)
    {
        try {
            YT_LOG_DEBUG("Started committing tablet stores update transaction (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            const auto& movementData = tablet->SmoothMovementData();
            if (movementData.ShouldForwardMutation()) {
                YT_LOG_DEBUG("Externalizing tablet stores update transaction (%v, TransactionId: %v)",
                    tablet->GetLoggingTag(),
                    transaction->GetId());

                NProto::TReqExternalizeTransaction req;
                ToProto(req.mutable_transaction_id(), transaction->GetId());
                req.set_transaction_start_timestamp(transaction->GetStartTimestamp());
                req.set_transaction_timeout(ToProto<i64>(transaction->GetTimeout()));
                ToProto(req.mutable_externalizer_tablet_id(), tablet->GetId());

                NRpc::WriteAuthenticationIdentityToProto(
                    &req,
                    NRpc::GetCurrentAuthenticationIdentity());

                WaitFor(CreateMutation(Slot_->GetHydraManager(), req)
                    ->CommitAndLog(Logger))
                    .ThrowOnError();

                YT_LOG_DEBUG("Tablet stores update transaction externalized (%v, TransactionId: %v)",
                    tablet->GetLoggingTag(),
                    transaction->GetId());
            }

            NApi::TTransactionCommitOptions commitOptions{
                .GeneratePrepareTimestamp = false
            };

            WaitFor(transaction->Commit(commitOptions))
                .ThrowOnError();

            YT_LOG_DEBUG("Tablet stores update transaction committed (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            return VoidFuture;
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
    }

    TCellId GetCellId() const override
    {
        return Slot_->GetCellId();
    }

    i64 LockTablet(TTablet* tablet, ETabletLockType lockType) override
    {
        // After lock barrier is does not make any sense to lock tablet, since
        // lock will not prevent tablet from being unmounted or frozen,
        // so such locks are forbidden.
        auto state = tablet->GetPersistentState();
        auto lockAllowed = !(state > ETabletState::UnmountWaitingForLocks && state <= ETabletState::UnmountLast);
        YT_LOG_ALERT_UNLESS(lockAllowed,
            "Tablet was locked in unexpected state "
            "(TabletId: %v, TabletState: %v, LockType: %v, LockCount: %v)",
            tablet->GetId(),
            state,
            lockType,
            tablet->GetTotalTabletLockCount());

        return tablet->Lock(lockType);
    }

    i64 UnlockTablet(TTablet* tablet, ETabletLockType lockType) override
    {
        auto lockCount = tablet->Unlock(lockType);
        OnTabletUnlocked(tablet);
        return lockCount;
    }

    TTabletNodeDynamicConfigPtr GetDynamicConfig() const override
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        return dynamicConfigManager->GetConfig()->TabletNode;
    }

    const TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return Bootstrap_->GetDynamicConfigManager();
    }

    void OnTableDynamicConfigChanged(
        TClusterTableConfigPatchSetPtr /*oldConfig*/,
        TClusterTableConfigPatchSetPtr newConfig)
    {
        YT_UNUSED_FUTURE(BIND(&TTabletManager::DoTableDynamicConfigChanged, MakeWeak(this), Passed(std::move(newConfig)))
            .AsyncVia(Slot_->GetEpochAutomatonInvoker())
            .Run());
    }

    void OnDynamicConfigChanged(
        TClusterNodeDynamicConfigPtr oldConfig,
        TClusterNodeDynamicConfigPtr newConfig)
    {
        Slot_->GetAutomatonInvoker()->Invoke(BIND(
            &TTabletManager::DoDynamicConfigChanged,
            MakeWeak(this),
            std::move(oldConfig),
            std::move(newConfig)));
    }

    void DoTableDynamicConfigChanged(TClusterTableConfigPatchSetPtr patch)
    {
        if (!IsLeader()) {
            return;
        }

        auto globalPatch = static_cast<TTableConfigPatchPtr>(patch);

        YT_LOG_DEBUG("Observing new table dynamic config (ExperimentNames: %v)",
            MakeFormattableView(
                patch->TableConfigExperiments,
                [] (auto* builder, const auto& experiment) {
                    FormatValue(builder, experiment.first, /*format*/ TStringBuf{});
                })
        );

        auto globalPatchYson = ConvertToYsonString(globalPatch).ToString();
        auto experimentsYson = ConvertToYsonString(patch->TableConfigExperiments).ToString();

        for (const auto& [id, tablet] : Tablets()) {
            ScheduleTabletConfigUpdate(tablet, patch, globalPatchYson, experimentsYson);
        }
    }

    void DoDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& oldConfig,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        RowDigestFetcher_->Reconfigure(newConfig);
        ChunkViewSizeFetcher_->Reconfigure(newConfig);

        if (!IsRecovery() &&
            IsLeader() &&
            newConfig->TabletNode->StoreCompactor->UseRowDigests &&
            !oldConfig->TabletNode->StoreCompactor->UseRowDigests)
        {
            for (auto& [tabletId, tablet] : Tablets()) {
                RowDigestFetcher_->FetchStoreInfos(tablet);
            }
        }
        if (!newConfig->TabletNode->StoreCompactor->UseRowDigests &&
            oldConfig->TabletNode->StoreCompactor->UseRowDigests)
        {
            for (auto& [tabletId, tablet] : Tablets()) {
                RowDigestFetcher_->ResetCompactionHints(tablet);
            }
        }
    }

    void ScheduleTabletConfigUpdate(
        TTablet* tablet,
        const TClusterTableConfigPatchSetPtr& patch,
        const TString& globalPatchYson,
        const TString& experimentsYson)
    {
        // Applying new settings is a rather expensive operation: it is a mutation to say the least.
        // Even more, this mutation restarts replication pipelines and other background processes,
        // so we'd like to avoid unnecessary reconfigurations. It is necessary if:
        //   - global config has changed;
        //   - the set of matching experiments has changed;
        //   - a patch of a matching auto-applied experiment has changed.

        auto scheduleUpdate = [&] {
            TReqUpdateTabletSettings req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            req.set_mount_revision(tablet->GetMountRevision());
            req.set_global_patch(globalPatchYson);
            req.set_experiments(experimentsYson);
            Slot_->CommitTabletMutation(req);
        };

        const auto& currentSettings = tablet->RawSettings();

        // Check for global config changes.
        if (!static_cast<TTableConfigPatchPtr>(patch)->IsEqual(currentSettings.GlobalPatch)) {
            return scheduleUpdate();
        }

        // Check for changes in experiments.

        // NB: Fixed-order container is crucial for simultaneous traversal.
        static_assert(std::is_same_v<
            decltype(currentSettings.Experiments),
            std::map<TString, TTableConfigExperimentPtr>>);

        auto it = currentSettings.Experiments.begin();
        auto jt = patch->TableConfigExperiments.begin();
        auto itEnd = currentSettings.Experiments.end();
        auto jtEnd = patch->TableConfigExperiments.end();

        // Fast path.
        if (it == itEnd && jt == jtEnd) {
            return;
        }

        auto descriptor = GetTableConfigExperimentDescriptor(tablet);

        while (it != itEnd || jt != jtEnd) {
            if (it != itEnd && jt != jtEnd && it->first == jt->first) {
                // Same experiment.
                const auto& currentExperiment = it->second;
                const auto& newExperiment = jt->second;

                if (!newExperiment->AutoApply) {
                    ++it;
                    ++jt;
                    continue;
                }

                YT_ASSERT(currentExperiment->Matches(descriptor));
                if (!newExperiment->Matches(descriptor)) {
                    // Experiment is not applied anymore.
                    return scheduleUpdate();
                }

                if (!newExperiment->Patch->IsEqual(currentExperiment->Patch)) {
                    // Experiment patch has changed.
                    return scheduleUpdate();
                }

                ++it;
                ++jt;
            } else if (jt == jtEnd || (it != itEnd && it->first < jt->first)) {
                // Previously matching experiment is now gone.
                return scheduleUpdate();
            } else {
                // There is a new experiment that possibly can be applied.
                const auto& newExperiment = jt->second;
                if (newExperiment->Matches(descriptor) && newExperiment->AutoApply) {
                    // New experiment can be applied.
                    return scheduleUpdate();
                }
                ++jt;
            }

        }
    }

    TTableConfigExperiment::TTableDescriptor GetTableConfigExperimentDescriptor(TTablet* tablet) const
    {
        return {
            .TableId = tablet->GetTableId(),
            .TablePath = tablet->GetTablePath(),
            .TabletCellBundle = Slot_->GetTabletCellBundleName(),
            // NB: Experiments never affect in-memory mode.
            .InMemoryMode = tablet->GetSettings().MountConfig->InMemoryMode,
            .Sorted = tablet->GetTableSchema()->IsSorted(),
            .Replicated = tablet->IsReplicated(),
        };
    }

    static void SetTableConfigErrors(TTablet* tablet, const std::vector<TError>& configErrors)
    {
        if (configErrors.empty()) {
            tablet->RuntimeData()->Errors.ConfigError.Store(TError{});
            return;
        }

        auto error = TError("Errors occurred while deserializing tablet config")
            << TErrorAttribute("tablet_id", tablet->GetId())
            << configErrors;
        tablet->RuntimeData()->Errors.ConfigError.Store(error);
    }


    void DoRestoreHunkLocks(
        TTransaction* transaction,
        TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        YT_LOG_INFO("Restoring hunk locks (TabletId: %v)", tabletId);

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto hunkChunk = tablet->FindHunkChunk(chunkId);
            if (!hunkChunk) {
                YT_LOG_ALERT("Trying to remove unexisting hunk chunk (TabletId: %v, HunkChunkId: %v)",
                    tabletId,
                    chunkId);
                continue;
            }
            hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Exclusive);
        }

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            InsertOrCrash(hunkChunkIdsToAdd, chunkId);
        }

        if (request->create_hunk_chunks_during_prepare()) {
            for (auto chunkId : hunkChunkIdsToAdd) {
                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (!hunkChunk) {
                    continue;
                }

                hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Shared);
            }
        }

        THashSet<TChunkId> existingReferencedHunks;
        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(
                descriptor.chunk_meta().extensions()))
            {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId) && !existingReferencedHunks.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunk(chunkId);
                        hunkChunk->Lock(transaction->GetId(), EObjectLockMode::Shared);
                        existingReferencedHunks.insert(chunkId);
                    }
                }
            }
        }
    }

    void CountStoreMemoryStatistics(TMemoryStatistics* statistics, const IStorePtr& store) const
    {
        if (store->IsDynamic()) {
            auto usage = store->GetDynamicMemoryUsage();
            if (store->GetStoreState() == EStoreState::ActiveDynamic) {
                statistics->DynamicActive += usage;
            } else if (store->GetStoreState() == EStoreState::PassiveDynamic) {
                statistics->DynamicPassive += usage;
            }
        } else if (store->IsChunk()) {
            auto chunk = store->AsChunk();

            if (auto backing = chunk->GetBackingStore()) {
                statistics->DynamicBacking += backing->GetDynamicMemoryUsage();
            }

            auto countChunkStoreMemory = [&] (i64 bytes) {
                statistics->PreloadStoreCount += 1;
                switch (chunk->GetPreloadState()) {
                    case EStorePreloadState::Scheduled:
                    case EStorePreloadState::Running:
                        if (chunk->IsPreloadAllowed()) {
                            statistics->PreloadPendingStoreCount += 1;
                        } else {
                            statistics->PreloadFailedStoreCount += 1;
                        }
                        statistics->PreloadPendingBytes += bytes;
                        break;

                    case EStorePreloadState::Complete:
                        statistics->Static.Usage += bytes;
                        break;

                    case EStorePreloadState::Failed:
                        statistics->PreloadFailedStoreCount += 1;
                        break;

                    case EStorePreloadState::None:
                        break;

                    default:
                        YT_ABORT();
                }
            };

            if (chunk->GetInMemoryMode() != EInMemoryMode::None) {
                countChunkStoreMemory(chunk->GetMemoryUsage());
            }
        }
    }

    void ValidateTrimmedRowCountPrecedeReplication(TTablet* tablet, i64 trimmedRowCount)
    {
        auto replicationTimestamp = tablet->GetOrderedChaosReplicationMinTimestamp();

        auto it = tablet->StoreRowIndexMap().lower_bound(trimmedRowCount);
        if (it == tablet->StoreRowIndexMap().end() || replicationTimestamp < it->second->GetMinTimestamp()) {
            THROW_ERROR_EXCEPTION("Could not trim tablet since some replicas may not be replicated up to this point")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("trimmed_row_count", trimmedRowCount)
                << TErrorAttribute("replication_timestamp", replicationTimestamp);
        }
    }

    void FetchCompactionHints(TTablet* tablet, const std::optional<TRange<IStorePtr>>& stores = {})
    {
        if (IsLeader()) {
            ChunkViewSizeFetcher_->FetchStoreInfos(tablet, stores);
            RowDigestFetcher_->FetchStoreInfos(tablet, stores);
        }
    }

    void ResetCompactionHints(TTablet* tablet)
    {
        ChunkViewSizeFetcher_->ResetCompactionHints(tablet);
        RowDigestFetcher_->ResetCompactionHints(tablet);
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TabletMap_);

////////////////////////////////////////////////////////////////////////////////

ITabletManagerPtr CreateTabletManager(
    TTabletManagerConfigPtr config,
    ITabletSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TTabletManager>(
        std::move(config),
        std::move(slot),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
