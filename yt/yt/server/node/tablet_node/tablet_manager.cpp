#include "tablet_manager.h"

#include "alien_cluster_client_cache.h"
#include "automaton.h"
#include "backup_manager.h"
#include "bootstrap.h"
#include "config.h"
#include "hunk_chunk.h"
#include "hunk_lock_manager.h"
#include "in_memory_manager.h"
#include "ordered_chunk_store.h"
#include "ordered_dynamic_store.h"
#include "ordered_store_manager.h"
#include "partition.h"
#include "private.h"
#include "replicated_store_manager.h"
#include "serialize.h"
#include "slot_manager.h"
#include "smooth_movement_tracker.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "sorted_store_manager.h"
#include "structured_logger.h"
#include "table_config_manager.h"
#include "table_puller.h"
#include "table_replicator.h"
#include "tablet.h"
#include "tablet_cell_write_manager.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "compaction_hint_fetching.h"

#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>

#include <yt/yt/server/node/tablet_node/transaction_manager.pb.h>

#include <yt/yt/server/lib/hive/avenue_directory.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/persistent_mailbox_state_cookie.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/helpers.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>
#include <yt/yt/server/lib/lease_server/helpers.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

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
#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/rpc/authentication_identity.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/yt/containers/ring_queue.h>

#include <library/cpp/yt/string/string.h>

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
        , StoreContext_(New<TStoreContext>(Config_, Bootstrap_))
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
        , TabletOrchidService_(TTabletOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
        , BackupManager_(CreateBackupManager(
            Slot_,
            Bootstrap_))
        , MinHashDigestCache_(Bootstrap_->GetTabletNodeDynamicConfig()->StoreCompactor->MinHashDigestCacheCapacity)
        , CompactionHintFetchers_{
            {
                NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow,
                New<TCompactionHintFetcher>(
                    Slot_->GetCellId(),
                    TabletNodeLogger().WithTag("Fetcher", "ChunkViewSize"),
                    TabletNodeProfiler().WithPrefix("/compaction_hints/chunk_view_size"),
                    Bootstrap_->GetTabletNodeDynamicConfig()
                        ->StoreCompactor->CompactionHintFetchers[NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow],
                    Bootstrap_->GetCompactionHintFetchThrottlers()
                        ->RequestThrottlers()[NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow]),
            },
            {
                NLsm::EStoreCompactionHintKind::VersionedRowDigest,
                New<TCompactionHintFetcher>(
                    Slot_->GetCellId(),
                    TabletNodeLogger().WithTag("Fetcher", "RowDigest"),
                    TabletNodeProfiler().WithPrefix("/compaction_hints/row_digest"),
                    Bootstrap_->GetTabletNodeDynamicConfig()
                        ->StoreCompactor->CompactionHintFetchers[NLsm::EStoreCompactionHintKind::VersionedRowDigest],
                    Bootstrap_->GetCompactionHintFetchThrottlers()
                        ->RequestThrottlers()[NLsm::EStoreCompactionHintKind::VersionedRowDigest]),
            },
            {
                NLsm::EStoreCompactionHintKind::MinHashDigest,
                New<TCompactionHintFetcher>(
                    Slot_->GetCellId(),
                    TabletNodeLogger().WithTag("Fetcher", "MinHashDigest"),
                    TabletNodeProfiler().WithPrefix("/compaction_hints/min_hash_digest"),
                    Bootstrap_->GetTabletNodeDynamicConfig()
                        ->StoreCompactor->CompactionHintFetchers[NLsm::EStoreCompactionHintKind::MinHashDigest],
                    Bootstrap_->GetCompactionHintFetchThrottlers()
                        ->RequestThrottlers()[NLsm::EStoreCompactionHintKind::MinHashDigest]),
            },
        }
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND_NO_PROPAGATE(&TTabletManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND_NO_PROPAGATE(&TTabletManager::LoadValues, Unretained(this)));
        RegisterLoader(
            "TabletManager.Async",
            BIND_NO_PROPAGATE(&TTabletManager::LoadAsync, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND_NO_PROPAGATE(&TTabletManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND_NO_PROPAGATE(&TTabletManager::SaveValues, Unretained(this)));
        RegisterSaver(
            EAsyncSerializationPriority::Default,
            "TabletManager.Async",
            BIND_NO_PROPAGATE(&TTabletManager::SaveAsync, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraMountTablet, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUnmountTablet, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraRemountTablet, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUpdateTabletSettings, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraSetReshardRedirectionHint, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraFreezeTablet, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUnfreezeTablet, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraProvisionalFlush, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraReportTabletProvisionallyFlushed, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraCancelTabletTransition, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraSetTabletState, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraTrimRows, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraLockTablet, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraReportTabletLocked, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUnlockTablet, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraRotateStore, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraSplitPartition, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraMergePartitions, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUpdatePartitionSampleKeys, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraAddTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraDecommissionTabletCell, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraSuspendTabletCell, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraResumeTabletCell, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraOnTabletCellDecommissioned, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraOnTabletCellSuspended, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraReplicateTabletContent, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraOnDynamicStoreAllocated, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraSetCustomRuntimeData, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraUnregisterMasterAvenueEndpoint, Unretained(this)));
        RegisterForwardedMethod(BIND_NO_PROPAGATE(&TTabletManager::HydraAdvanceReplicationEra, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();

        transactionManager->RegisterTransactionActionHandlers<TReqReplicateRows>({
            {
                .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareReplicateRows, Unretained(this)),
                .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitReplicateRows, Unretained(this)),
                .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortReplicateRows, Unretained(this)),
            },
            BIND_NO_PROPAGATE(&TTabletManager::HydraNeedExternalizeReplicateRows, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqWritePulledRows>({
            {
                .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareWritePulledRows, Unretained(this)),
                .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitWritePulledRows, Unretained(this)),
                .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortWritePulledRows, Unretained(this)),
                .Serialize = BIND_NO_PROPAGATE(&TTabletManager::HydraSerializeWritePulledRows, Unretained(this)),
            },
            BIND_NO_PROPAGATE(&TTabletManager::HydraNeedExternalizeWritePullRows, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqAdvanceReplicationProgress>({
            {
                .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareAdvanceReplicationProgress, Unretained(this)),
                .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortAdvanceReplicationProgress, Unretained(this)),
                .Serialize = BIND_NO_PROPAGATE(&TTabletManager::HydraSerializeAdvanceReplicationProgress, Unretained(this)),
            },
            BIND_NO_PROPAGATE(&TTabletManager::HydraNeedExternalizeAdvanceReplicationProgress, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqUpdateTabletStores>({{
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareUpdateTabletStores, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletManager::HydraCommitUpdateTabletStores, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletManager::HydraAbortUpdateTabletStores, Unretained(this)),
        }});
        // Coordinator: TReqBoggleHunkTabletStoreLock, late prepare.
        transactionManager->RegisterTransactionActionHandlers<TReqBoggleHunkTabletStoreLock>({{
            .Prepare = BIND_NO_PROPAGATE(&TTabletManager::HydraPrepareAndCommitBoggleHunkTabletStoreLock, Unretained(this)),
        }});

        BackupManager_->Initialize();

        const auto& tableConfigManager = Bootstrap_->GetTableDynamicConfigManager();
        tableConfigManager->SubscribeAfterConfigChanged(TableDynamicConfigChangedCallback_);

        Bootstrap_->SubscribeTabletNodeConfigChanged(DynamicConfigChangedCallback_);
        OnDynamicConfigChanged(
            Bootstrap_->GetTabletNodeDynamicConfig(),
            Bootstrap_->GetTabletNodeDynamicConfig());
    }

    void Finalize() override
    {
        const auto& tableConfigManager = Bootstrap_->GetTableDynamicConfigManager();
        tableConfigManager->UnsubscribeAfterConfigChanged(TableDynamicConfigChangedCallback_);
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
            tablet->GetUnreservedDynamicStoreIdCount() == 0 &&
            !tablet->GetDynamicStoreIdRequested())
        {
            AllocateDynamicStore(tablet);
            return true;
        }

        return false;
    }

    void ExternalizeTransactionIfNeeded(
        TTablet* tablet,
        ITransactionPtr transaction,
        TStringBuf transactionKind) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& movementData = tablet->SmoothMovementData();
        if (!movementData.ShouldForwardMutation()) {
            return;
        }

        auto token = movementData.GetSiblingAvenueEndpointId();

        YT_TLOG_DEBUG("Externalizing transaction")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId())
            .With("ExternalizationToken", token)
            .With("Kind", transactionKind);

        NProto::TReqExternalizeTransaction req;
        ToProto(req.mutable_transaction_id(), transaction->GetId());
        req.set_transaction_start_timestamp(ToProto(transaction->GetStartTimestamp()));
        req.set_transaction_timeout(ToProto(transaction->GetTimeout()));
        ToProto(req.mutable_externalizer_tablet_id(), tablet->GetId());
        ToProto(req.mutable_externalization_token(), token);

        NRpc::WriteAuthenticationIdentityToProto(
            &req,
            NRpc::GetCurrentAuthenticationIdentity());

        WaitFor(CreateMutation(Slot_->GetHydraManager(), req)
            ->CommitAndLog(Logger))
            .ThrowOnError();

        YT_TLOG_DEBUG("Transaction externalized")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId())
            .With("ExternalizationToken", token)
            .With("Kind", transactionKind);
    }

    void ExternalizeTransactionIfNeeded(
        const TTabletSnapshotPtr& tabletSnapshot,
        ITransactionPtr transaction,
        TStringBuf transactionKind) override
    {
        // Optimistic check to avoid unnecessary calls in automaton invoker.
        if (tabletSnapshot->TabletRuntimeData->SmoothMovementData.Role.load() != ESmoothMovementRole::Source) {
            return;
        }

        auto callback = [=, transaction = std::move(transaction), this, this_ = MakeStrong(this)] {
            auto* tablet = FindTablet(tabletSnapshot->TabletId);
            if (!tablet) {
                THROW_ERROR_EXCEPTION("Tablet %v does not exist, cannot externalize transaction",
                    tabletSnapshot->TabletId);
            }
            if (tablet->GetMountRevision() != tabletSnapshot->MountRevision) {
                THROW_ERROR_EXCEPTION("Tablet %v has invalid mount revision, cannot externalize transaction",
                    tabletSnapshot->TabletId)
                    .With("expected_mount_revision", tabletSnapshot->MountRevision)
                    .With("actual_mount_revision", tablet->GetMountRevision());
            }

            ExternalizeTransactionIfNeeded(tablet, std::move(transaction), transactionKind);
        };

        WaitFor(BIND(callback)
            .AsyncVia(Slot_->GetAutomatonInvoker())
            .Run())
            .ThrowOnError();
    }

    TTablet* GetTabletOrThrow(TTabletId id) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                id)
                .With("tablet_id", id);
        }
        return tablet;
    }

    TTablet* FindOrphanedTablet(TTabletId id) const final
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (auto it = OrphanedTablets_.find(id); it != OrphanedTablets_.end()) {
            return it->second.get();
        }

        return nullptr;
    }

    ITabletCellWriteManagerHostPtr GetTabletCellWriteManagerHost() override
    {
        return this;
    }

    ISmoothMovementTrackerHostPtr GetSmoothMovementTrackerHost() override
    {
        return this;
    }

    std::vector<TTabletMemoryStatistics> GetMemoryStatistics() const final
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

    TRowCacheControllerContext GetRowCacheControllerContext() const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TRowCacheControllerContext context;

        for (const auto& [tabletId, tablet] : Tablets()) {
            if (!tablet->IsPhysicallySorted()) {
                continue;
            }

            if (tablet->GetSettings().MountConfig->LookupCacheRowsRatio <= 0) {
                continue;
            }

            i64 unmergedRowCount = tablet->GetNonActiveStoresUnmergedRowCount();
            if (const auto& store = tablet->GetActiveStore()) {
                unmergedRowCount += store->GetRowCount();
            }

            const auto& rowCache = tablet->GetRowCache();
            if (!rowCache) {
                continue;
            }

            context.Tablets[tabletId] = {
                .RowCache = rowCache,
                .TabletDataWeight = tablet->GetTotalDataWeight(),
                .TabletRowCount = unmergedRowCount,
                .LookupCacheRowsRatio = tablet->GetSettings().MountConfig->LookupCacheRowsRatio,
            };
        }

        return context;
    }

    TFuture<void> Trim(
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 trimmedRowCount) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
                ValidateTrimmedRowCountPrecedesReplication(tablet, trimmedRowCount);
            }

            NProto::TReqTrimRows hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
            hydraRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
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
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
        request.set_mount_revision(ToProto(tablet->GetMountRevision()));
        request.set_reason(ToProto(reason));

        auto activeStore = tablet->GetActiveStore();
        // Out of band immediate rotation may happen when this mutation is scheduled but not applied.
        // This rotation request will become obsolete and may lead to an empty active store
        // being rotated.
        ToProto(request.mutable_expected_active_store_id(), activeStore->GetId());

        // NB: Some aborted transactions that created skip list entries could be dropped on store reserialization.
        // NB: Comparison with 1 is correct since sorted dynamic store always _contains_ NullTimestamp.
        request.set_allow_empty_store(activeStore->IsSorted() && activeStore->GetRowCount() > 0 && activeStore->GetTimestampCount() == 1);

        Slot_->CommitTabletMutation(request);
    }

    void ReleaseBackingStore(const IChunkStorePtr& store) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (auto backingStore = store->GetBackingStore()) {
            store->SetBackingStore(nullptr);
            YT_TLOG_DEBUG("Backing store released")
                .With("StoreId", store->GetId())
                .With("BackingStoreId", backingStore->GetId());

            if (auto* tablet = FindTablet(store->GetTabletId())) {
                tablet->GetStructuredLogger()->OnBackingStoreReleased(store, backingStore);
            }
        }
    }

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const ITransactionPtr& transaction) override
    {
        YT_TLOG_DEBUG("Acquiring tablet stores commit semaphore")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId());

        return tablet
            ->GetStoresUpdateCommitSemaphore()
            ->AsyncAcquire()
            .AsUnique().Apply(
                BIND(
                    ThrowOnDestroyed(&TTabletManager::OnStoresUpdateCommitSemaphoreAcquired),
                    MakeWeak(this),
                    tablet,
                    transaction)
                .AsyncVia(tablet->GetEpochAutomatonInvoker()));
    }

    IYPathServicePtr GetTabletOrchidService() override
    {
        return TabletOrchidService_;
    }

    IYPathServicePtr GetTabletReplicationOrchidService() override
    {
        return IYPathService::FromMethod(&TTabletManager::BuildTabletReplicationOrchid, MakeWeak(this))
            ->Via(Slot_->GetAutomatonInvoker());
    }

    ETabletCellLifeStage GetTabletCellLifeStage() const final
    {
        return CellLifeStage_;
    }

    NHiveClient::ICellDirectoryPtr GetCellDirectory() const final
    {
        return Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetCellDirectory();
    }

    ITransactionManagerPtr GetTransactionManager() const final
    {
        return Slot_->GetTransactionManager();
    }

    TDynamicTabletCellOptionsPtr GetDynamicOptions() const final
    {
        return Slot_->GetDynamicOptions();
    }

    TTabletManagerConfigPtr GetConfig() const final
    {
        return Config_;
    }

    TTimestamp GetLatestTimestamp() const final
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
        NHiveServer::TAvenueEndpointId siblingEndpointId,
        bool allowDestructionInMessageToSelf = false) override
    {
        Slot_->UnregisterSiblingTabletAvenue(
            siblingEndpointId,
            allowDestructionInMessageToSelf);
        Slot_->GetTransactionManager()->AbortTransactionsExternalizedToThisCell(
            TTransactionExternalizationToken(GetSiblingAvenueEndpointId(siblingEndpointId)));
    }

    void RegisterMasterAvenue(
        TTabletId tabletId,
        NHiveServer::TAvenueEndpointId masterEndpointId,
        NHiveServer::TPersistentMailboxStateCookie&& cookie) override
    {
        Slot_->RegisterMasterAvenue(tabletId, masterEndpointId, std::move(cookie));
    }

    NHiveServer::TPersistentMailboxStateCookie UnregisterMasterAvenue(
        NHiveServer::TAvenueEndpointId masterEndpointId) override
    {
        return Slot_->UnregisterMasterAvenue(masterEndpointId);
    }

    void PostAvenueMessage(
        TAvenueEndpointId endpointId,
        const ::google::protobuf::MessageLite& message) override
    {
        const auto& hiveManager = Slot_->GetHiveManager();
        auto mailbox = hiveManager->GetMailbox(endpointId);
        hiveManager->PostMessage(mailbox, message);
    }

    void BuildTabletReplicationOrchid(IYsonConsumer* consumer) const
    {
        auto perClusterReplicationStatus = GetPerClusterReplicationStatus();

        BuildYsonFluently(consumer)
            .BeginMap()
                .DoFor(
                    perClusterReplicationStatus.begin(),
                    perClusterReplicationStatus.end(),
                    [&] (TFluentMap fluent, const auto& replicationStatusEntry) {
                        const auto& [clusterName, replicationStatus] = *replicationStatusEntry;
                        bool hasReplicationActivity = replicationStatus.PreparedReplicatorTransactionCount != 0 ||
                            replicationStatus.ActiveReplicatorIterationCount != 0;
                        bool hasSyncReplicas = replicationStatus.SyncReplicaCount != 0 ||
                            replicationStatus.SyncToAsyncReplicaCount != 0 ||
                            replicationStatus.AsyncToSyncReplicaCount != 0;

                        fluent.Item(clusterName)
                            .BeginMap()
                                .Item("prepared_replicator_transaction_count").Value(replicationStatus.PreparedReplicatorTransactionCount)
                                .Item("active_replicator_iteration_count").Value(replicationStatus.ActiveReplicatorIterationCount)
                                .Item("has_replication_activity").Value(hasReplicationActivity)
                                .Item("sync_replica_count").Value(replicationStatus.SyncReplicaCount)
                                .Item("sync_to_async_replica_count").Value(replicationStatus.SyncToAsyncReplicaCount)
                                .Item("async_to_sync_replica_count").Value(replicationStatus.AsyncToSyncReplicaCount)
                                .Item("has_sync_replicas").Value(hasSyncReplicas)
                            .EndMap();
                    })
            .EndMap();
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Tablet, TTablet);

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;
    const TTabletManagerConfigPtr Config_;

    class TStoreContext
        : public IStoreContext
    {
    public:
        TStoreContext(TTabletManagerConfigPtr config, IBootstrap* bootstrap)
            : Config_(std::move(config))
            , Bootstrap_(bootstrap)
        { }

        const NChunkClient::IBlockCachePtr& GetBlockCache() override
        {
            return Bootstrap_->GetBlockCache();
        }

        const IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() override
        {
            return Bootstrap_->GetVersionedChunkMetaManager();
        }

        const NQueryClient::IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() override
        {
            return Bootstrap_->GetColumnEvaluatorCache();
        }

        const TTabletManagerConfigPtr& GetTabletManagerConfig() override
        {
            return Config_;
        }

        bool GetAccountActiveStoreLookupHashTableToTabletStatic() const override
        {
            return Bootstrap_
                ->GetTabletNodeDynamicConfig()
                ->TabletManager
                ->AccountActiveStoreLookupHashTableToTabletStatic;
        }

    private:
        const TTabletManagerConfigPtr Config_;
        IBootstrap* const Bootstrap_;
    };

    const IStoreContextPtr StoreContext_;

    class TTabletOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TTabletManager> impl, IInvokerPtr invoker)
        {
            return New<TTabletOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<std::string> GetKeys(i64 limit) const final
        {
            std::vector<std::string> keys;
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

        i64 GetSize() const final
        {
            if (auto owner = Owner_.Lock()) {
                return owner->Tablets().size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(const std::string& key) const final
        {
            if (auto owner = Owner_.Lock()) {
                if (auto tablet = owner->FindTablet(TTabletId::FromString(key))) {
                    return BuildYsonNodeFluently()
                        .BeginMap()
                            .Do(BIND(&TTablet::BuildOrchidYson, tablet))
                        .EndMap();
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TTabletManager> Owner_;

        explicit TTabletOrchidService(TWeakPtr<TTabletManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND()
    };

    class TTabletContext
        : public ITabletContext
    {
    public:
        explicit TTabletContext(TTabletManager* owner)
            : Owner_(owner)
        { }

        TCellId GetCellId() const final
        {
            return Owner_->Slot_->GetCellId();
        }

        NNative::IClientPtr GetClient() const final
        {
            return Owner_->Bootstrap_->GetClient();
        }

        TTabletNodeDynamicConfigPtr GetDynamicConfig() const final
        {
            return Owner_->Bootstrap_->GetTabletNodeDynamicConfig();
        }

        const std::string& GetTabletCellBundleName() const final
        {
            return Owner_->Slot_->GetTabletCellBundleName();
        }

        EPeerState GetAutomatonState() const final
        {
            return Owner_->Slot_->GetAutomatonState();
        }

        int GetAutomatonTerm() const final
        {
            return Owner_->Slot_->GetAutomatonTerm();
        }

        IInvokerPtr GetControlInvoker() const final
        {
            return Owner_->Bootstrap_->GetControlInvoker();
        }

        IInvokerPtr GetAutomatonInvoker() const final
        {
            return Owner_->Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Default);
        }

        IInvokerPtr GetStorageHeavyInvoker() const override
        {
            return Owner_->Bootstrap_->GetStorageHeavyInvoker();
        }

        IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const final
        {
            return Owner_->Bootstrap_->GetColumnEvaluatorCache();
        }

        NQueryClient::IRowComparerProviderPtr GetRowComparerProvider() const final
        {
            return Owner_->Bootstrap_->GetRowComparerProvider();
        }

        IStorePtr CreateStore(
            TTablet* tablet,
            EStoreType type,
            TStoreId storeId,
            const TAddStoreDescriptor* descriptor) const final
        {
            return Owner_->CreateStore(tablet, type, storeId, descriptor);
        }

        THunkChunkPtr CreateHunkChunk(
            TTablet* tablet,
            TChunkId chunkId,
            const TAddHunkChunkDescriptor* descriptor) const final
        {
            return Owner_->CreateHunkChunk(tablet, chunkId, descriptor);
        }

        ITransactionManagerPtr GetTransactionManager() const final
        {
            return Owner_->Slot_->GetTransactionManager();
        }

        NRpc::IServerPtr GetLocalRpcServer() const final
        {
            return Owner_->Bootstrap_->GetRpcServer();
        }

        INodeMemoryTrackerPtr GetNodeMemoryUsageTracker() const final
        {
            return Owner_->Bootstrap_->GetNodeMemoryUsageTracker();
        }

        TRowCacheControllerPtr GetRowCacheController() const final
        {
            return Owner_->Bootstrap_->GetRowCacheController();
        }

        NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() const final
        {
            return Owner_->Bootstrap_->GetConnection()->GetChunkReplicaCache();
        }

        IHedgingManagerRegistryPtr GetHedgingManagerRegistry() const final
        {
            return Owner_->Bootstrap_->GetHedgingManagerRegistry();
        }

        std::string GetLocalHostName() const final
        {
            return Owner_->Bootstrap_->GetLocalHostName();
        }

        NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const final
        {
            return Owner_->Bootstrap_->GetLocalDescriptor();
        }

        ITabletWriteManagerHostPtr GetTabletWriteManagerHost() const final
        {
            return Owner_;
        }

        IVersionedChunkMetaManagerPtr GetVersionedChunkMetaManager() const final
        {
            return Owner_->Bootstrap_->GetVersionedChunkMetaManager();
        }

        const TCompactionHintFetcherPtr& GetCompactionHintFetcher(NLsm::EStoreCompactionHintKind kind) const override
        {
            return Owner_->CompactionHintFetchers_[kind];
        }

        TSimpleLruCache<NChunkClient::TChunkId, TMinHashDigestPtr>* GetMinHashDigestCache() const override
        {
            return &Owner_->MinHashDigestCache_;
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

    struct TClusterReplicationStatus
    {
        int PreparedReplicatorTransactionCount = 0;
        int ActiveReplicatorIterationCount = 0;
        int SyncReplicaCount = 0;
        int AsyncToSyncReplicaCount = 0;
        int SyncToAsyncReplicaCount = 0;
    };

    TTabletContext TabletContext_;
    TEntityMap<TTablet, TTabletMapTraits> TabletMap_;
    ETabletCellLifeStage CellLifeStage_ = ETabletCellLifeStage::Running;
    bool Suspending_ = false;

    TRingQueue<TTablet*> PrelockedTablets_;

    THashSet<IDynamicStorePtr> OrphanedStores_;
    THashMap<TTabletId, std::unique_ptr<TTablet>> OrphanedTablets_;

    const TPeriodicExecutorPtr DecommissionCheckExecutor_;
    const TPeriodicExecutorPtr SuspensionCheckExecutor_;

    const IYPathServicePtr TabletOrchidService_;

    IBackupManagerPtr BackupManager_;

    mutable TSimpleLruCache<TChunkId, TMinHashDigestPtr> MinHashDigestCache_;

    const NLsm::TStoreCompactionHintArray<TCompactionHintFetcherPtr> CompactionHintFetchers_;

    const TCallback<void(TClusterTableConfigPatchSetPtr)> TableDynamicConfigChangedCallback_ =
        BIND(&TTabletManager::OnTableDynamicConfigChanged, MakeWeak(this));

    const TCallback<void(TTabletNodeDynamicConfigPtr, TTabletNodeDynamicConfigPtr)> DynamicConfigChangedCallback_ =
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
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        TabletMap_.LoadValues(context);

        Load(context, CellLifeStage_);
        Load(context, Suspending_);
    }

    void LoadAsync(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

    void OnAfterSnapshotLoaded() noexcept final
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderRecoveryComplete();

        auto storeCompactorConfig = GetDynamicConfig()->StoreCompactor;
        for (auto [storeKind, partitionKind] : NLsm::StoreCompactionHintKinds) {
            CompactionHintFetchers_[storeKind]->Start(
                // NB(dave11ar): Do not take epoch automaton invoker from Slot_, it might be initialized later.
                EpochAutomatonInvoker_,
                storeCompactorConfig->CompactionHintFetchers[storeKind]);
        }

        StartEpoch();
    }

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderActive();

        // Serialize executions of OnTableDynamicConfigChanged via control invoker
        // to avoid reordering.
        Bootstrap_->GetControlInvoker()->Invoke(BIND(
            [
                tableConfigManager = Bootstrap_->GetTableDynamicConfigManager(),
                weakThis = MakeWeak(this),
                automatonInvoker = Slot_->GetAutomatonInvoker()
            ] {
                if (!tableConfigManager->IsConfigLoaded()) {
                    return;
                }

                // OnTableDynamicConfigChanged schedules a callback via
                // guarded automaton invoker. It will not execute anything
                // until OnLeaderActive finishes execution, so we introduce
                // a barrier.
                WaitFor(BIND([] {}).AsyncVia(automatonInvoker).Run())
                    .ThrowOnError();

                if (auto this_ = weakThis.Lock()) {
                    this_->OnTableDynamicConfigChanged(/*oldConfig*/ nullptr);
                }
            }
        ));

        for (auto [tabletId, tablet] : TabletMap_) {
            CheckIfTabletFullyUnlocked(tablet);
            CheckIfTabletFullyFlushed(tablet);
        }

        DecommissionCheckExecutor_->Start();
        SuspensionCheckExecutor_->Start();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        StopEpoch();

        YT_UNUSED_FUTURE(DecommissionCheckExecutor_->Stop());
        YT_UNUSED_FUTURE(SuspensionCheckExecutor_->Stop());

        for (auto& compactionHintFetchers : CompactionHintFetchers_) {
            compactionHintFetchers->Stop();
        }
    }

    void OnFollowerRecoveryComplete() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnFollowerRecoveryComplete();

        StartEpoch();
    }

    void OnStopFollowing() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopFollowing();

        StopEpoch();
    }

    void StartEpoch()
    {
        for (auto [tabletId, tablet] : TabletMap_) {
            const auto& movementData = tablet->SmoothMovementData();
            auto role = movementData.GetRole();
            auto stage = movementData.GetStage();

            if (role == ESmoothMovementRole::None ||
                role == ESmoothMovementRole::Source ||
                (role == ESmoothMovementRole::Target &&
                    stage >= ESmoothMovementStage::TargetActivated))
            {
                StartTabletEpoch(tablet);
            }
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

        auto* mutationContext = GetCurrentMutationContext();
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
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
        auto replicaDescriptors = request->replicatable_content().has_replicas_and_replication_progress()
            ? FromProto<std::vector<TTableReplicaDescriptor>>(request->replicatable_content().replicas())
            : FromProto<std::vector<TTableReplicaDescriptor>>(request->replicas_deprecated());
        auto retainedTimestamp = GET_FROM_REPLICATABLE(has_retained_timestamp)
            ? FromProto<TTimestamp>(GET_FROM_REPLICATABLE(retained_timestamp))
            : MinTimestamp;
        auto conflictHorizonTimestamp = MinTimestamp;
        // COMPAT(ponasenko-rs)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
            ETabletReign::AddConflictHorizon)
        {
            if (request->replicatable_content().has_conflict_horizon_timestamp()) {
                conflictHorizonTimestamp = FromProto<TTimestamp>(
                    request->replicatable_content().conflict_horizon_timestamp());
            }
        }

        const auto& mountHint = request->mount_hint();
        auto cumulativeDataWeight = GET_FROM_REPLICATABLE(cumulative_data_weight);
        bool isSmoothMovementTarget = request->has_movement_source_cell_id();
        auto useRetainedPreloadedChunks = request->use_retained_preloaded_chunks();
        auto originatorTablets = FromProto<std::vector<NTabletServer::TOriginatorTablet>>(request->replicatable_content().originator_tablets());
        auto customRuntimeData = request->has_replicatable_content() && request->replicatable_content().has_custom_runtime_data()
            ? TYsonString(request->replicatable_content().custom_runtime_data())
            : TYsonString();
        auto serializationType = FromProto<ETabletTransactionSerializationType>(request->serialization_type());

        // COMPAT(alexelexa)
        TInstant mountTime;
        if (mutationContext->Request().Reign >= static_cast<int>(ETabletReign::AddTabletMountTime)) {
            mountTime = mutationContext->GetTimestamp();
        }

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
            });

        std::vector<TError> configErrors;
        auto settings = rawSettings.BuildEffectiveSettings(&configErrors, nullptr);

        NTabletNode::TIdGenerator idGenerator(
            CellTagFromId(tabletId),
            // Make first ids look like 1-1-... rather than 0-1-...
            /*counter*/ 1ull << 32,
            /*seed*/ mutationContext->RandomGenerator()->Generate<ui64>());

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
            cumulativeDataWeight,
            serializationType,
            mountTime,
            conflictHorizonTimestamp);
        tabletHolder->RawSettings() = rawSettings;

        tabletHolder->CustomRuntimeData() = std::move(customRuntimeData);
        // COMPAT(atalmenev)
        auto reign = GetCurrentMutationContext()->Request().Reign;
        if (static_cast<ETabletReign>(reign) >= ETabletReign::SaveOriginatorTabletsAfterReshard) {
            tabletHolder->OriginatorTablets() = std::move(originatorTablets);
        }

        InitializeTablet(tabletHolder.get());

        tabletHolder->Reconfigure(Slot_);

        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        SetTableConfigErrors(tablet, configErrors);

        if (tablet->IsPhysicallyOrdered() && !isSmoothMovementTarget) {
            tablet->SetTrimmedRowCount(GET_FROM_REPLICATABLE(trimmed_row_count));
        }

        PopulateDynamicStoreIdPool(tablet, request);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Mount(
            TRange(GET_FROM_REPLICATABLE(stores)),
            TRange(GET_FROM_REPLICATABLE(hunk_chunks)),
            TMountOptions{
                .CreateDynamicStore = !freeze && !isSmoothMovementTarget,
                .UseRetainedPreloadedChunks = useRetainedPreloadedChunks,
                .MountHint = &mountHint,
            });

        tablet->SetState(freeze ? ETabletState::Frozen : ETabletState::Mounted);
        tablet->SetLastStableState(tablet->GetState());

        // NB: We do not store previously attached dictionary chunk ids. We just build new ones upon mount.
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            tablet->SetCompressionDictionaryRebuildBackoffTime(policy, TInstant::Now());
        }

        if (isSmoothMovementTarget) {
            // Smooth movement target is being allocated.

            auto siblingCellId = FromProto<TTabletCellId>(
                request->movement_source_cell_id());
            auto siblingEndpointId = FromProto<TAvenueEndpointId>(
                request->movement_source_avenue_endpoint_id());
            auto siblingMountRevision = FromProto<TRevision>(
                request->movement_source_mount_revision());

            auto& movementData = tablet->SmoothMovementData();
            movementData.SetSiblingCellId(siblingCellId);
            movementData.SetRole(ESmoothMovementRole::Target);
            movementData.SetStage(ESmoothMovementStage::TargetAllocated);
            movementData.SetSiblingMountRevision(siblingMountRevision);
            movementData.SetReign(GetCurrentMutationEffectiveReign());

            movementData.SetSiblingAvenueEndpointId(siblingEndpointId);
            Slot_->RegisterSiblingTabletAvenue(siblingEndpointId, siblingCellId);

            auto& runtimeData = tablet->RuntimeData()->SmoothMovementData;
            runtimeData.Role.store(ESmoothMovementRole::Target);
            runtimeData.IsActiveServant.store(false);
            runtimeData.SiblingServantCellId.Store(siblingCellId);
            runtimeData.SiblingServantMountRevision.store(
                siblingMountRevision);

            tablet->InitializeTargetServantActivationFuture();

            YT_VERIFY(!masterAvenueEndpointId);
        } else {
            tablet->RuntimeData()->SmoothMovementData.IsActiveServant.store(true);
        }

        if (masterAvenueEndpointId) {
            tablet->SetMasterAvenueEndpointId(masterAvenueEndpointId);
            Slot_->RegisterMasterAvenue(
                tablet->GetId(),
                masterAvenueEndpointId,
                /*cookie*/ {});
        }

        YT_TLOG_INFO("Tablet mounted")
            .With(tablet->GetLoggingTags())
            .WithFormat("MountRevision", "%x", mountRevision)
            .WithFormat("Keys", "%v .. %v", pivotKey, nextPivotKey)
            .With("StoreCount", GET_FROM_REPLICATABLE(stores).size())
            .With("HunkChunkCount", GET_FROM_REPLICATABLE(hunk_chunks).size())
            .With("PartitionCount", tablet->IsPhysicallySorted() ? std::make_optional(tablet->PartitionList().size()) : std::nullopt)
            .With("TotalRowCount", tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTotalRowCount()))
            .With("TrimmedRowCount", tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTrimmedRowCount()))
            .With("CumulativeDataWeight", cumulativeDataWeight)
            .With("Atomicity", tablet->GetAtomicity())
            .With("CommitOrdering", tablet->GetCommitOrdering())
            .With("Frozen", freeze)
            .With("UpstreamReplicaId", upstreamReplicaId)
            .With("RetainedTimestamp", retainedTimestamp)
            .With("SchemaId", schemaId)
            .With("MasterAvenueEndpointId", masterAvenueEndpointId)
            .With("SerializationType", serializationType)
            .With("ConflictHorizonTimestamp", conflictHorizonTimestamp);

        for (const auto& descriptor : replicaDescriptors) {
            AddTableReplica(tablet, descriptor);
        }

        {
            bool hasReplicationProgress = request->replicatable_content().has_replicas_and_replication_progress()
                ? request->replicatable_content().has_replication_progress()
                : request->has_replication_progress_deprecated();
            if (hasReplicationProgress) {
                auto replicationCardId = tablet->GetReplicationCardId();
                auto progress = FromProto<TReplicationProgress>(
                    request->replicatable_content().has_replicas_and_replication_progress()
                        ? request->replicatable_content().replication_progress()
                        : request->replication_progress_deprecated());
                YT_TLOG_DEBUG("Tablet bound for chaos replication")
                    .With(tablet->GetLoggingTags())
                    .With("ReplicationCardId", replicationCardId)
                    .With("ReplicationProgress", progress);

                tablet->RuntimeData()->ReplicationProgress.Store(New<TRefCountedReplicationProgress>(std::move(progress)));
            }
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
            response.set_mount_revision(ToProto(tablet->GetMountRevision()));
            PostMasterMessage(tablet, response, /*forceCellMailbox*/ true);
        }

        tablet->GetStructuredLogger()->OnTabletMounted();
        tablet->GetStructuredLogger()->OnFullHeartbeat();

        if (!IsRecovery() && !isSmoothMovementTarget) {
            StartTabletEpoch(tablet);
        }

        #undef GET_FROM_ESSENTIAL
        #undef GET_FROM_REPLICATABLE
    }

    TReqReplicateTabletContent PrepareReplicateTabletContentRequest(TTablet* tablet) override
    {
        // COMPAT(ifsmirnov)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) < ETabletReign::SmoothMovementOrdered) {
            if (tablet->IsPhysicallyOrdered()) {
                THROW_ERROR_EXCEPTION("Ordered and replicated tables are not supported");
            }
        }

        // Validation against not implemented features.
        if (tablet->IsReplicated()) {
            THROW_ERROR_EXCEPTION("Replicated tables are not supported");
        }

        if (!tablet->GetLockManager()->IsEmpty()) {
            THROW_ERROR_EXCEPTION("Bulk insert lock replication is not supported");
        }

        // Restart chaos replica epoch to avoid puller transactions intersecting
        // smooth movement barrier.
        if (!IsRecovery()) {
            if (auto replicationCardId = tablet->GetReplicationCardId()) {
                StopChaosReplicaEpoch(tablet);
                RemoveChaosAgent(tablet);
                StartChaosReplicaEpoch(tablet, replicationCardId);
            }
        }

        TReqReplicateTabletContent request;

        // Essential stuff.
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(
            ToProto(tablet->SmoothMovementData().GetSiblingMountRevision()));

        // Local tablet stuff: id generator, partition pivot keys, retained timestamp.
        tablet->PopulateReplicateTabletContentRequest(&request);

        // Stores.
        tablet->GetStoreManager()->PopulateReplicateTabletContentRequest(&request);

        // Settings.
        SerializeTableSettings(
            request.mutable_replicatable_content()->mutable_table_settings(),
            tablet->RawSettings());

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

        // Check that source and target reigns are the same.
        // NB: Target reign could have changed after target tablet was mounted. In this case
        // reign mismatch is still reported.
        auto mutationReign = static_cast<ETabletReign>(GetHiveMutationSenderReign());
        if (movementData.GetReign() != mutationReign) {
            YT_TLOG_DEBUG("Got replicate tablet content request from servant with different reign")
                .With(tablet->GetLoggingTags())
                .With("SenderReign", movementData.GetReign())
                .With("ReceiverReign", mutationReign);

            Slot_->GetSmoothMovementTracker()->RejectMovement(
                tablet,
                TError("Replicated content reign %Qv differs from current reign %Qv",
                    mutationReign,
                    movementData.GetReign()));
            return;
        }

        if (tablet->GetSettings().MountConfig->Testing.RejectReplicatedContentReceiving) {
            YT_TLOG_DEBUG("Target servant rejected replicated content for testing purposes")
                .With(tablet->GetLoggingTags());

            Slot_->GetSmoothMovementTracker()->RejectMovement(
                tablet,
                TError("Smooth movement rejected by target for testing purposes"));
            return;
        }

        const auto& replicatableContent = request->replicatable_content();

        YT_TLOG_DEBUG("Tablet got replicated content")
            .With(tablet->GetLoggingTags())
            .With("StoreCount", replicatableContent.stores().size());

        // Local tablet stuff.
        tablet->LoadReplicatedContent(request);

        // Stores.
        tablet->GetStoreManager()->LoadReplicatedContent(request);

        // Settings.
        // COMPAT(ifsmirnov): remove conditional when everything is 25.3.
        if (request->replicatable_content().has_table_settings()) {
            // NB: We cannot call ReconfigureTablet because epoch is not started yet.
            auto rawSettings = DeserializeTableSettings(&request->replicatable_content(), tabletId);
            auto descriptor = GetTableConfigExperimentDescriptor(tablet);
            rawSettings.DropIrrelevantExperiments(descriptor);

            std::vector<TError> configErrors;
            auto settings = rawSettings.BuildEffectiveSettings(&configErrors, nullptr);

            tablet->SetSettings(std::move(settings));
            tablet->RawSettings() = std::move(rawSettings);

            tablet->Reconfigure(Slot_);
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

        tablet->SetPreloadedChunkRetentionRequired(request->retain_preloaded_chunks());
        if (request->use_extended_snapshot_eviction_timeout()) {
            tablet->SetSnapshotEvictionTimeout(GetDynamicConfig()->TabletManager->ExtendedSnapshotEvictionTimeout);
        }

        if (request->force()) {
            YT_TLOG_INFO("Tablet is forcefully unmounted")
                .With(tablet->GetLoggingTags());

            auto tabletHolder = TabletMap_.Release(tabletId);

            if (auto endpointId = tablet->SmoothMovementData().GetSiblingAvenueEndpointId()) {
                UnregisterSiblingTabletAvenue(endpointId);
            }

            if (auto endpointId = tablet->GetMasterAvenueEndpointId()) {
                UnregisterMasterAvenue(endpointId);
            }

            // NB: UnregisterXxxAvenue methods may abort transactions that hold locks
            // to the tablet and cause its destruction, so we handle orphaned tablets
            // at the end.
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

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
            }
        } else {
            auto state = tablet->GetState();
            if (IsInUnmountWorkflow(state)) {
                YT_TLOG_INFO("Requested to unmount a tablet in a wrong state, ignored")
                    .With("State", state)
                    .With(tablet->GetLoggingTags());
                return;
            }

            YT_TLOG_INFO("Unmounting tablet")
                .With(tablet->GetLoggingTags());

            tablet->SetState(ETabletState::UnmountWaitingForLocks);

            YT_TLOG_INFO("Waiting for all tablet locks to be released")
                .With(tablet->GetLoggingTags());

            CheckIfTabletFullyUnlocked(tablet);
        }
    }

    void ReconfigureTablet(TTablet* tablet, TRawTableSettings rawSettings)
    {
        std::vector<TError> configErrors;
        auto settings = rawSettings.BuildEffectiveSettings(&configErrors, nullptr);

        auto oldSettings = tablet->GetSettings();

        tablet->LookupHeavyHitters().RowCount->Reconfigure(settings.MountConfig->LookupHeavyHitters);
        tablet->LookupHeavyHitters().DataWeight->Reconfigure(settings.MountConfig->LookupHeavyHitters);

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

            if (auto replicationCardId = tablet->GetReplicationCardId()) {
                StopChaosReplicaEpoch(tablet);
                RemoveChaosAgent(tablet);
                StartChaosReplicaEpoch(tablet, replicationCardId);
            }
        }

        Slot_->GetSmoothMovementTracker()->CheckTablet(tablet);
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
        rawSettings.DropIrrelevantExperiments(descriptor);

        ReconfigureTablet(tablet, std::move(rawSettings));

        YT_TLOG_INFO("Tablet remounted")
            .With(tablet->GetLoggingTags());
    }

    void HydraUpdateTabletSettings(TReqUpdateTabletSettings* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (tablet->GetActiveServantMountRevision() != mountRevision) {
            return;
        }

        TRawTableSettings newRawSettings(tablet->RawSettings());

        newRawSettings.Experiments = ConvertTo<std::map<std::string, TTableConfigExperimentPtr>>(
            TYsonString(request->experiments()));
        newRawSettings.GlobalPatch = ConvertTo<TTableConfigPatchPtr>(TYsonString(request->global_patch()));

        auto descriptor = GetTableConfigExperimentDescriptor(tablet);
        newRawSettings.DropIrrelevantExperiments(descriptor);

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

        YT_TLOG_DEBUG("Tablet settings updated")
            .With(tablet->GetLoggingTags())
            .With("AppliedExperiments", MakeFormattableView(
                    tablet->RawSettings().Experiments,
                    [] (auto* builder, const auto& experiment) {
                        FormatValue(builder, experiment.first, /*format*/ TStringBuf{});
                    }));
    }

    void HydraSetReshardRedirectionHint(TReqSetReshardRedirectionHint* request)
    {
        auto oldTabletIds = FromProto<std::vector<TTabletId>>(request->old_tablet_ids());
        auto oldTabletMountRevisions = FromProto<std::vector<NHydra::TRevision>>(request->old_tablet_mount_revisions());

        std::vector<TTabletSnapshotPtr> oldTabletSnapshots;
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        for (const auto& [tabletId, mountRevision] : Zip(oldTabletIds, oldTabletMountRevisions)) {
            if (auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tabletId, mountRevision)) {
                oldTabletSnapshots.push_back(tabletSnapshot);
            }
        }

        if (oldTabletSnapshots.empty()) {
            return;
        }

        auto reshardRedirectionHint = New<TReshardRedirectionHint>();
        reshardRedirectionHint->OldTabletIds = std::move(oldTabletIds);
        reshardRedirectionHint->OldTabletMountRevisions = std::move(oldTabletMountRevisions);
        reshardRedirectionHint->NewTabletIds = FromProto<std::vector<TTabletId>>(request->new_tablet_ids());
        reshardRedirectionHint->NewTabletPivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->new_tablet_pivot_keys());
        reshardRedirectionHint->NewTabletsMountRevision = FromProto<NHydra::TRevision>(request->new_tablets_mount_revision());

        YT_LOG_DEBUG("Set reshard redirection hint for tablets (TabletId: %v, "
            "ReshardRedirectionHint: [OldTabletIds: %v, OldTabletMountRevisions: %llx, "
            "NewTabletIds: %v, NewTabletsMountRevision: %llx])",
            MakeFormattableView(oldTabletSnapshots, [] (auto* builder, const auto& tabletSnapshot) {
                builder->AppendFormat("%v", tabletSnapshot->TabletId);
            }),
            reshardRedirectionHint->OldTabletIds,
            reshardRedirectionHint->OldTabletMountRevisions,
            reshardRedirectionHint->NewTabletIds,
            reshardRedirectionHint->NewTabletsMountRevision);

        for (auto& oldTabletSnapshot : oldTabletSnapshots) {
            oldTabletSnapshot->ReshardRedirectionHint = reshardRedirectionHint;
        }
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
            YT_TLOG_ALERT("Requested to freeze a tablet in a wrong state, ignored")
                .With("State", state)
                .With(tablet->GetLoggingTags());
            return;
        }

        YT_TLOG_INFO("Freezing tablet")
            .With(tablet->GetLoggingTags());

        tablet->SetState(ETabletState::FreezeWaitingForLocks);

        YT_TLOG_INFO("Waiting for all tablet locks to be released")
            .With(tablet->GetLoggingTags());

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
        if (state != ETabletState::Frozen) {
            YT_TLOG_INFO("Requested to unfreeze a tablet in a wrong state, ignored")
                .With("State", state)
                .With(tablet->GetLoggingTags());
            return;
        }

        YT_TLOG_INFO("Tablet unfrozen")
            .With(tablet->GetLoggingTags());

        tablet->SetState(ETabletState::Mounted);
        tablet->SetLastStableState(ETabletState::Mounted);

        PopulateDynamicStoreIdPool(tablet, request);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Rotate(/*createNewStore*/ true, EStoreRotationReason::None);
        storeManager->InitializeRotation();

        UpdateTabletSnapshot(tablet);

        auto dynamicStoreIds = FromProto<std::vector<TDynamicStoreId>>(request->dynamic_store_ids());
        tablet->GetStructuredLogger()->OnTabletUnfrozen(dynamicStoreIds);

        TRspUnfreezeTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        response.set_mount_revision(ToProto(tablet->GetMountRevision()));
        PostMasterMessage(tablet, response);
    }

    void HydraProvisionalFlush(TReqProvisionalFlush* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->SmoothMovementData().GetRole() == ESmoothMovementRole::None);

        auto state = tablet->GetState();
        if (state != ETabletState::Mounted) {
            YT_TLOG_DEBUG("Requested provisional flush of a tablet in a wrong state, ignored")
                .With("State", state)
                .With(tablet->GetLoggingTags());
            return;
        }

        if (tablet->GetProvisionallyFlushingStoreId()) {
            return;
        }

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead) {
            auto storeId = FromProto<TDynamicStoreId>(request->dynamic_store_id());
            tablet->PushDynamicStoreIdToPool(storeId);
        }

        tablet->SetProvisionallyFlushingStoreId(tablet->GetActiveStore()->GetId());

        YT_TLOG_DEBUG("Provisionally flushing tablet")
            .With(tablet->GetLoggingTags());

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Rotate(
            /*createNewStore*/ true,
            EStoreRotationReason::None,
            /*allowEmptyStore*/ true);
        UpdateTabletSnapshot(tablet);

        CheckIfTabletFullyFlushed(tablet);
    }

    void HydraReportTabletProvisionallyFlushed(TReqReportTabletProvisionallyFlushed* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_TLOG_DEBUG("Tablet provisionally flushed")
            .With(tablet->GetLoggingTags());

        TRspProvisionalFlush response;
        ToProto(response.mutable_tablet_id(), tablet->GetId());
        PostMasterMessage(tablet, response);
    }

    void HydraCancelTabletTransition(TReqCancelTabletTransition* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();

        auto stableState = tablet->GetLastStableState();

        YT_TLOG_DEBUG("Canceling tablet transition")
            .With(tablet->GetLoggingTags())
            .With("State", state)
            .With("LastStableState", stableState);

        // Add dynamic store ids to the pool even if the state is incorrect
        // because these stores are already added by master.
        PopulateDynamicStoreIdPool(tablet, request);

        if (state == ETabletState::Mounted || state == ETabletState::Frozen) {
            YT_TLOG_DEBUG("Requested to cancel transition of a tablet in a stable state, ignored")
                .With(tablet->GetLoggingTags())
                .With("State", state);
            return;
        }

        // Adding new active store to an ordered tablet when an empty passive store
        // is present will result in both stores having the same starting row index,
        // which is invalid. Since we cannot reliably check for the store emptiness,
        // we avoid canceling transition when active store is not present.
        if (tablet->IsPhysicallyOrdered() &&
            (tablet->GetState() != ETabletState::UnmountWaitingForLocks &&
                 tablet->GetState() != ETabletState::FreezeWaitingForLocks))
        {
            YT_TLOG_DEBUG("Will not cancel tablet transition since the tablet is already flushing: cannot cancel rotation of an ordered tablet")
                .With(tablet->GetLoggingTags())
                .With("State", tablet->GetState());

            return;
        }

        DoSetTabletState(tablet, stableState, /*cancelTransition*/ true);
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

        YT_TLOG_INFO("Tablet locked by bulk insert")
            .With("TabletId", tabletId)
            .With("TransactionId", transactionId);

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

        YT_TLOG_INFO("Tablet bulk insert lock confirmed")
            .With("TabletId", tabletId)
            .With("TransactionIds", transactionIds);

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
            YT_TLOG_INFO("All stores of tablet are going to be discarded")
                .With(tablet->GetLoggingTags());

            int reservedStoreIdCount = 0;
            for (auto value : tablet->ReservedDynamicStoreIdCount()) {
                reservedStoreIdCount += value;
            }

            tablet->ClearDynamicStoreIdPool(/*keepReservations*/ true);
            PopulateDynamicStoreIdPool(tablet, request);

            if (reservedStoreIdCount > ssize(tablet->DynamicStoreIdPool())) {
                YT_TLOG_ALERT("Tablet unlock request did not provide enough dynamic store ids to guarantee all reservations")
                    .With(tablet->GetLoggingTags())
                    .With("ProvidedStoreCount", ssize(tablet->DynamicStoreIdPool()))
                    .With("Reservations", MakeFormattableView(
                            TEnumTraits<EDynamicStoreIdReservationReason>::GetDomainValues(),
                            [&] (auto* builder, auto key) {
                                builder->AppendFormat(
                                    "%v:%v",
                                    key,
                                    tablet->ReservedDynamicStoreIdCount()[key]);
                            }));
            }

            storeManager->DiscardAllStores();
        }

        const auto& structuredLogger = tablet->GetStructuredLogger();
        structuredLogger->OnTabletUnlocked(
            TRange(storesToAdd),
            updateMode == EUpdateMode::Overwrite,
            transactionId);

        storeManager->BulkAddStores(TRange(storesToAdd));

        const auto& lockManager = tablet->GetLockManager();
        if (tablet->GetLockManager()->HasTransaction(transactionId)) {
            auto nextEpoch = lockManager->GetEpoch() + 1;
            UpdateTabletSnapshot(tablet, nextEpoch);

            auto commitTimestamp = FromProto<NTransactionClient::TTimestamp>(request->commit_timestamp());
            lockManager->Unlock(commitTimestamp, transactionId);
        } else {
            UpdateTabletSnapshot(tablet);
        }

        YT_TLOG_INFO("Tablet unlocked by bulk insert")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transactionId)
            .With("AddedStoreIds", addedStoreIds)
            .With("LockManagerEpoch", lockManager->GetEpoch());
    }

    void HydraSetTabletState(TReqSetTabletState* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto requestedState = FromProto<ETabletState>(request->state());
        DoSetTabletState(tablet, requestedState);
    }

    void DoSetTabletState(
        TTablet* tablet,
        ETabletState requestedState,
        bool cancelTransition = false)
    {
        if (tablet->GetState() == ETabletState::Mounted || tablet->GetState() == ETabletState::Frozen) {
            YT_TLOG_INFO("Improper tablet state transition requested after transition cancelation, ignored")
                .With(tablet->GetLoggingTags())
                .With("CurrentState", tablet->GetState())
                .With("RequestedState", requestedState);
            return;
        }

        switch (requestedState) {
            case ETabletState::FreezeFlushing: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_TLOG_INFO("Improper tablet state transition requested, ignored")
                        .With("CurrentState", state)
                        .With("RequestedState", requestedState)
                        .With(tablet->GetLoggingTags());
                    return;
                }
                [[fallthrough]];
            }

            case ETabletState::UnmountFlushing: {
                tablet->SetState(requestedState);

                const auto& storeManager = tablet->GetStoreManager();
                storeManager->Rotate(/*createNewStore*/ false, EStoreRotationReason::None);

                YT_TLOG_INFO("Waiting for all tablet stores to be flushed")
                    .With(tablet->GetLoggingTags())
                    .With("NewState", requestedState);

                CheckIfTabletFullyFlushed(tablet);
                break;
            }

            case ETabletState::Unmounted: {
                tablet->SetState(ETabletState::Unmounted);

                YT_TLOG_INFO("Tablet unmounted")
                    .With(tablet->GetLoggingTags());

                if (!IsRecovery()) {
                    StopTabletEpoch(tablet);
                }

                for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                    PostTableReplicaStatistics(tablet, replicaInfo);
                }

                tablet->GetStructuredLogger()->OnTabletUnmounted();

                TRspUnmountTablet response;
                ToProto(response.mutable_tablet_id(), tablet->GetId());
                *response.mutable_mount_hint() = tablet->GetMountHint();
                if (auto replicationProgress = tablet->RuntimeData()->ReplicationProgress.Acquire()) {
                    ToProto(response.mutable_replication_progress(), *replicationProgress);
                }
                response.set_mount_revision(ToProto(tablet->GetMountRevision()));

                // COMPAT(ponasenko-rs)
                if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                    ETabletReign::AddConflictHorizon)
                {
                    if (tablet->IsPhysicallySorted()) {
                        response.set_conflict_horizon_timestamp(
                            ToProto(tablet->GetPersistentConflictHorizonTimestamp()));
                    }
                }

                // NB: Do not unregister master avenue since it still has pending messages.
                // It will be unregistered later by TReqUnregisterMasterAvenueEndpoint message.

                PostMasterMessage(tablet, response);

                if (tablet->IsPreloadedChunkRetentionRequired() &&
                    tablet->GetSettings().MountConfig->InMemoryMode != EInMemoryMode::None)
                {
                    YT_TLOG_INFO("Preloaded chunk data will be retained after unmount")
                        .With(tablet->GetLoggingTags());
                    const auto& inMemoryManager = Bootstrap_->GetInMemoryManager();
                    for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                        if (!store->IsChunk() || store->IsEmpty()) {
                            continue;
                        }

                        auto chunkStore = store->AsChunk();
                        if (auto chunkData = chunkStore->GetInMemoryChunkData()) {
                            inMemoryManager->FinalizeChunk(
                                chunkStore->GetChunkId(),
                                chunkData);
                        }
                    }
                }

                TabletMap_.Remove(tablet->GetId());

                break;
            }

            case ETabletState::Frozen: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO("Improper tablet state transition requested, ignored (CurrentState %v, RequestedState: %v, %v)",
                        state,
                        requestedState,
                        tablet->GetLoggingTags());
                    return;
                }

                tablet->SetState(ETabletState::Frozen);
                tablet->SetLastStableState(ETabletState::Frozen);
                tablet->ClearDynamicStoreIdPool(/*keepReservations*/ false);

                for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                    if (store->IsChunk()) {
                        ReleaseBackingStore(store->AsChunk());
                    }
                }

                YT_TLOG_INFO("Tablet frozen")
                    .With(tablet->GetLoggingTags());

                tablet->GetStructuredLogger()->OnTabletFrozen();

                if (cancelTransition) {
                    TRspCancelTabletTransition response;
                    ToProto(response.mutable_tablet_id(), tablet->GetId());
                    response.set_mount_revision(ToProto(tablet->GetMountRevision()));
                    response.set_actual_tablet_state(ToProto(NTabletClient::ETabletState::Frozen));
                    PostMasterMessage(tablet, response);
                } else {
                    TRspFreezeTablet response;
                    ToProto(response.mutable_tablet_id(), tablet->GetId());
                    *response.mutable_mount_hint() = tablet->GetMountHint();
                    response.set_mount_revision(ToProto(tablet->GetMountRevision()));

                    // COMPAT(ponasenko-rs)
                    if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                        ETabletReign::AddConflictHorizon)
                    {
                        if (tablet->IsPhysicallySorted()) {
                            response.set_conflict_horizon_timestamp(
                                ToProto(tablet->GetPersistentConflictHorizonTimestamp()));
                        }
                    }

                    PostMasterMessage(tablet, response);
                }

                break;
            }

            case ETabletState::Mounted: {
                YT_VERIFY(cancelTransition);

                tablet->SetState(ETabletState::Mounted);
                tablet->SetLastStableState(ETabletState::Mounted);

                if (!tablet->GetActiveStore()) {
                    const auto& storeManager = tablet->GetStoreManager();
                    storeManager->Rotate(/*createNewStore*/ true, EStoreRotationReason::None);
                }

                UpdateTabletSnapshot(tablet);

                TRspCancelTabletTransition response;
                ToProto(response.mutable_tablet_id(), tablet->GetId());
                response.set_mount_revision(ToProto(tablet->GetMountRevision()));
                response.set_actual_tablet_state(ToProto(NTabletClient::ETabletState::Mounted));
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

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto finallyGuard = Finally([tablet] {
            tablet->ChaosData()->IsTrimInProgress.store(false);
        });

        auto trimmedRowCount = request->trimmed_row_count();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        UpdateTrimmedRowCount(tablet, trimmedRowCount);
    }

    void HydraRotateStore(TReqRotateStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        auto reason = FromProto<EStoreRotationReason>(request->reason());
        auto expectedActiveStoreId = FromProto<TStoreId>(request->expected_active_store_id());
        auto allowEmptyStore = request->allow_empty_store();

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto actualMountRevision = tablet->GetActiveServantMountRevision();
        if (mountRevision != actualMountRevision) {
            YT_TLOG_DEBUG("Mount revision mismatch in store rotation request, ignored")
                .WithFormat("Expected", "%x", mountRevision)
                .WithFormat("Actual", "%x", actualMountRevision);
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (tablet->GetState() != ETabletState::Mounted) {
            YT_TLOG_DEBUG("Rotation request received by a tablet in invalid state, ignored")
                .With(tablet->GetLoggingTags())
                .With("State", tablet->GetState());
            storeManager->UnscheduleRotation();
            return;
        }

        if (tablet->GetActiveStore() &&
            expectedActiveStoreId &&
            tablet->GetActiveStore()->GetId() != expectedActiveStoreId)
        {
            YT_TLOG_DEBUG("Active store id mismatch in store rotation attempt")
                .With("ExpectedActiveStoreId", expectedActiveStoreId)
                .With("ActualActiveStoreId", tablet->GetActiveStore()->GetId())
                .With("Reason", reason)
                .With(tablet->GetLoggingTags());
            storeManager->UnscheduleRotation();
            return;
        }

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead && tablet->GetUnreservedDynamicStoreIdCount() == 0) {
            if (!tablet->GetDynamicStoreIdRequested()) {
                AllocateDynamicStore(tablet);
            }
            // TODO(ifsmirnov): Store flusher will try making unsuccessful mutations if response
            // from master comes late. Maybe should optimize.
            storeManager->UnscheduleRotation();
            return;
        }

        storeManager->Rotate(/*createNewStore*/ true, reason, allowEmptyStore);
        UpdateTabletSnapshot(tablet);

        if (tablet->IsPhysicallyOrdered()) {
            if (AllocateDynamicStoreIfNeeded(tablet)) {
                YT_TLOG_DEBUG("Dynamic store id for ordered tablet allocated after rotation")
                    .With(tablet->GetLoggingTags());

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
            YT_TLOG_DEBUG("Preparing tablet stores update under externalized transaction")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tabletId)
                .With("Reason", reason);
        }

        auto* tablet = GetTabletOrThrow(tabletId);
        const auto& structuredLogger = tablet->GetStructuredLogger();

        ValidatePreparingTransactionIsProperlyExternalized(tablet, transaction, "tablet stores update");

        YT_VERIFY(tablet->IsActiveServant() == !transaction->IsExternalizedToThisCell());

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        // Validate.
        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (tablet->IsActiveServant()) {
            tablet->ValidateMountRevision(mountRevision);
        } else {
            if (tablet->SmoothMovementData().GetSiblingMountRevision() != mountRevision) {
                THROW_ERROR_EXCEPTION("Invalid sibling mount revision, expected %x, got %x",
                    tablet->SmoothMovementData().GetSiblingMountRevision(),
                    mountRevision);
            }
        }

        if (IsInUnmountWorkflow(tablet->GetState()) && updateReason != ETabletStoresUpdateReason::Flush) {
            THROW_ERROR_EXCEPTION("Tablet is in %Qlv state", tablet->GetState())
                .With("update_reason", updateReason);
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
                    .With("store_ref_count", hunkChunk->GetStoreRefCount())
                    .With("prepared_store_ref_count", hunkChunk->GetPreparedStoreRefCount());
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
                    tablet->GetLoggingTags(),
                    movementData.GetRole(),
                    movementData.GetStage(),
                    reason);
            }
        }

        if (tablet->GetStoresUpdatePreparedTransactionId()) {
            THROW_ERROR_EXCEPTION("Cannot prepare stores update since it is already prepared by transaction %v",
                tablet->GetStoresUpdatePreparedTransactionId());
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

            hunkChunk->TryLock(transaction->GetId(), EObjectLockMode::Exclusive)
                .ThrowOnError();

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

                hunkChunk->TryLock(transaction->GetId(), EObjectLockMode::Shared)
                    .ThrowOnError();

                tablet->UpdateDanglingHunkChunks(hunkChunk);

                YT_TLOG_DEBUG("Hunk chunk added")
                    .With(tablet->GetLoggingTags())
                    .With("ChunkId", chunkId);
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
                            hunkChunk->TryLock(transaction->GetId(), EObjectLockMode::Shared)
                                .ThrowOnError();

                            tablet->UpdateDanglingHunkChunks(hunkChunk);
                            existingReferencedHunks.insert(chunkId);
                        }
                    }
                }
            }
        }

        tablet->SetStoresUpdatePreparedTransactionId(transaction->GetId());

        // TODO(ifsmirnov): log preparation errors as well.
        structuredLogger->OnTabletStoresUpdatePrepared(
            storeIdsToAdd,
            storeIdsToRemove,
            updateReason,
            transaction->GetId());

        YT_TLOG_INFO("Tablet stores update prepared")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId())
            .With("StoreIdsToAdd", storeIdsToAdd)
            .With("HunkChunkIdsToAdd", hunkChunkIdsToAdd)
            .With("StoreIdsToRemove", storeIdsToRemove)
            .With("HunkChunkIdsToRemove", hunkChunkIdsToRemove)
            .With("UpdateReason", updateReason)
            .With("ConflictHorizonTimestamp", FromProto<TTimestamp>(request->conflict_horizon_timestamp()))
            .With("UnleashedBackingStoreId", FromProto<TStoreId>(request->unleashed_backing_store_id()));
    }

    void HydraPrepareAndCommitBoggleHunkTabletStoreLock(
        TTransaction* /*transaction*/,
        TReqBoggleHunkTabletStoreLock* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.LatePrepare);

        const auto* context = GetCurrentMutationContext();
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

        const auto& hunkLockManager = tablet->GetHunkLockManager();

        auto hunkStoreId = FromProto<THunkStoreId>(request->store_id());
        auto lock = request->lock();
        if (!lock) {
            auto lockCount = hunkLockManager->GetPersistentLockCount(hunkStoreId);
            if (lockCount > 0) {
                THROW_ERROR_EXCEPTION("Hunk store %v has positive lock count %v",
                    hunkStoreId,
                    lockCount);
            }
        }

        // Set transient flags and create futures once again if we are in recovery, as they were lost.
        hunkLockManager->OnBoggleLockPrepared(hunkStoreId, lock);

        auto hunkCellId = FromProto<TCellId>(request->hunk_cell_id());
        auto hunkTabletId = FromProto<TTabletId>(request->hunk_tablet_id());
        auto hunkMountRevision = FromProto<NHydra::TRevision>(request->mount_revision());

        if (lock) {
            hunkLockManager->RegisterHunkStore(hunkStoreId, hunkCellId, hunkTabletId, hunkMountRevision);
        } else {
            hunkLockManager->UnregisterHunkStore(hunkStoreId);
            CheckIfTabletFullyFlushed(tablet);
        }
    }

    // COMPAT(akozhikhov)
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

    // COMPAT(akozhikhov)
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
        auto hunkMountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
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

    // COMPAT(akozhikhov)
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
            YT_TLOG_DEBUG("Aborting tablet stores update under externalized transaction")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tabletId)
                .With("Reason", reason);
        }

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto actualMountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (actualMountRevision != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto expectedTransactionId = tablet->GetStoresUpdatePreparedTransactionId();

        if (expectedTransactionId == transaction->GetId()) {
            tablet->SetStoresUpdatePreparedTransactionId({});
        } else {
            // This is fine because out-of-order aborts may come for transactions
            // that were not even prepared.
            YT_TLOG_DEBUG("Unexpected stores update transaction aborted, ignored")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transaction->GetId())
                .With("PreparedTransactionId", expectedTransactionId);

            // Continue nevertheless to mimic old behaviour.
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
                    YT_TLOG_DEBUG("Removing dangling uncommitted hunk chunk")
                        .With("HunkChunkId", hunkChunk->GetId());
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

        YT_TLOG_INFO("Tablet stores update aborted")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId());
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
            YT_TLOG_DEBUG("Committing tablet stores update under externalized transaction")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tabletId)
                .With("Reason", reason);
        }

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (FromProto<NHydra::TRevision>(request->mount_revision()) != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto expectedTransactionId = tablet->GetStoresUpdatePreparedTransactionId();
        if (expectedTransactionId != transaction->GetId()) {
            YT_TLOG_ALERT("Unexpected stores update transaction committed")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transaction->GetId())
                .With("PreparedTransactionId", expectedTransactionId);

            // Continue nevertheless to mimic old behaviour.
        }

        tablet->SetStoresUpdatePreparedTransactionId({});

        if (auto discardStoresRevision = tablet->GetLastDiscardStoresRevision()) {
            auto prepareRevision = transaction->GetPrepareRevision();
            if (prepareRevision < discardStoresRevision) {
                YT_TLOG_DEBUG("Tablet stores update commit interrupted by stores discard, ignored")
                    .With(tablet->GetLoggingTags())
                    .With("TransactionId", transaction->GetId())
                    .WithFormat("DiscardStoresRevision", "%x", discardStoresRevision)
                    .WithFormat("PrepareUpdateTabletStoresRevision", "%x", prepareRevision);

                // Validate that all prepared-for-removal stores were indeed discarded.
                for (const auto& descriptor : request->stores_to_remove()) {
                    auto storeId = FromProto<TStoreId>(descriptor.store_id());
                    if (const auto& store = tablet->FindStore(storeId)) {
                        YT_TLOG_ALERT("Store prepared for removal was not discarded while tablet stores update commit was interrupted by the discard")
                            .With(tablet->GetLoggingTags())
                            .With("StoreId", storeId)
                            .With("TransactionId", transaction->GetId())
                            .WithFormat("DiscardStoresRevision", "%x", discardStoresRevision)
                            .WithFormat("PrepareUpdateTabletStoresRevision", "%x", prepareRevision);

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

            if (request->has_unleashed_backing_store_id()) {
                auto backingStoreId = FromProto<TStoreId>(request->unleashed_backing_store_id());
                auto backingStore = tablet->GetStore(backingStoreId);
                registerBackingStore(backingStore);
            }
        }

        std::vector<TChunkId> compressionDictionaryIds;
        THashSet<THunkChunkPtr> addedHunkChunks;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            if (request->create_hunk_chunks_during_prepare()) {
                auto hunkChunk = tablet->FindHunkChunk(chunkId);
                if (!hunkChunk) {
                    YT_TLOG_ALERT("Hunk chunk is missing")
                        .With(tablet->GetLoggingTags())
                        .With("ChunkId", chunkId);
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

                YT_TLOG_DEBUG("Hunk chunk added")
                    .With(tablet->GetLoggingTags())
                    .With("ChunkId", chunkId);
                InsertOrCrash(addedHunkChunks, hunkChunk);
            }

            if (auto miscExt = FindProtoExtension<TMiscExt>(descriptor.chunk_meta().extensions())) {
                if (miscExt->has_dictionary_compression_policy()) {
                    auto policy = FromProto<EDictionaryCompressionPolicy>(miscExt->dictionary_compression_policy());
                    tablet->AttachCompressionDictionary(policy, chunkId);
                    compressionDictionaryIds.push_back(chunkId);
                }
            }
        }

        const auto& rowCache = tablet->GetRowCache();
        bool needResetRowCache = false;

        std::vector<TStoreId> removedStoreIds;
        removedStoreIds.reserve(request->stores_to_remove_size());
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            removedStoreIds.push_back(storeId);

            auto store = tablet->GetStore(storeId);
            if (store->IsDynamic() && store->IsSorted() && rowCache) {
                auto sortedDynamicStore = store->AsSortedDynamic();
                auto storeFlushIndex = sortedDynamicStore->GetFlushIndex();
                auto lastFlushedIndex = rowCache->GetLastFlushedIndex();
                if (lastFlushedIndex < storeFlushIndex) {
                    YT_TLOG_DEBUG("Store has not been flushed to row cache")
                        .With(tablet->GetLoggingTags())
                        .With("StoreId", storeId)
                        .With("LastFlushedIndex", lastFlushedIndex)
                        .With("StoreFlushIndex", storeFlushIndex);

                    needResetRowCache = true;
                }
            }

            storeManager->RemoveStore(store);

            YT_TLOG_DEBUG("Store removed")
                .With(tablet->GetLoggingTags())
                .With("StoreId", storeId)
                .With("DynamicMemoryUsage", store->GetDynamicMemoryUsage());

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, -1);

                    const auto& hunkChunk = ref.HunkChunk;

                    YT_TLOG_DEBUG("Hunk chunk unreferenced")
                        .With(tablet->GetLoggingTags())
                        .With("StoreId", storeId)
                        .With("HunkChunkRef", ref)
                        .With("StoreRefCount", hunkChunk->GetStoreRefCount());
                }
            }
        }

        if (needResetRowCache) {
            YT_TLOG_DEBUG_IF(
                IsLeader() && tablet->IsActiveServant(),
                "Store that was not flushed to row cache is detected at the leading cell peer, row cache will be reset")
                .With(tablet->GetLoggingTags());

            tablet->ResetRowCache(Slot_);
        }

        std::vector<TChunkId> removedHunkChunkIds;
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            removedHunkChunkIds.push_back(chunkId);

            auto hunkChunk = tablet->GetHunkChunk(chunkId);
            tablet->RemoveHunkChunk(hunkChunk);
            hunkChunk->SetState(EHunkChunkState::Removed);
            hunkChunk->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);

            YT_TLOG_DEBUG("Hunk chunk removed")
                .With(tablet->GetLoggingTags())
                .With("ChunkId", chunkId);
        }

        std::vector<IStorePtr> addedStores;
        THashSet<THunkChunkPtr> existingReferencedHunks;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storeManager->AddStore(
                store,
                TAddStoreOptions{
                    .UseInterceptedChunkData = true,
                    .OnFlush = updateReason == ETabletStoresUpdateReason::Flush,
                });
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

            if (store->IsOrdered()) {
                YT_TLOG_DEBUG("Ordered chunk store added")
                    .With(tablet->GetLoggingTags())
                    .With("StoreId", storeId)
                    .With("MaxTimestamp", store->GetMaxTimestamp())
                    .With("StartingRowIndex", store->AsOrdered()->GetStartingRowIndex())
                    .With("BackingStoreId", backingStoreId);
            } else {
                YT_TLOG_DEBUG("Sorted chunk store added")
                    .With(tablet->GetLoggingTags())
                    .With("StoreId", storeId)
                    .With("MaxTimestamp", store->GetMaxTimestamp())
                    .With("BackingStoreId", backingStoreId);
            }

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

                    YT_TLOG_DEBUG("Hunk chunk referenced")
                        .With(tablet->GetLoggingTags())
                        .With("StoreId", storeId)
                        .With("HunkChunkRef", ref)
                        .With("StoreRefCount", hunkChunk->GetStoreRefCount());
                }

                if (store->IsSorted()) {
                    // COMPAT(ponasenko-rs)
                    if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                        ETabletReign::AddConflictHorizon)
                    {
                        auto storeTimestamp = store->GetMaxTimestamp();
                        tablet->AdvancePersistentConflictHorizonTimestamp(storeTimestamp);

                        if (!backingStoreId) {
                            tablet->AdvanceTransientConflictHorizonTimestamp(
                                storeTimestamp,
                                /*expectedMountRevision*/ std::nullopt);
                        }
                    }
                }
            }
        }

        if (request->has_unleashed_backing_store_id()) {
            YT_VERIFY(request->has_conflict_horizon_timestamp());

            YT_VERIFY(tablet->IsPhysicallySorted());

            // COMPAT(ponasenko-rs)
            if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                ETabletReign::AddConflictHorizon)
            {
                tablet->AdvancePersistentConflictHorizonTimestamp(
                    FromProto<TTimestamp>(request->conflict_horizon_timestamp()));
            }

            if (!IsRecovery() &&
                IsBackingStoreRequired(tablet) &&
                tablet->IsActiveServant())
            {
                auto backingStoreId = FromProto<TStoreId>(request->unleashed_backing_store_id());
                const auto& backingStore = GetOrCrash(idToBackingStore, backingStoreId);

                YT_VERIFY(request->conflict_horizon_timestamp() == backingStore->GetMaxTimestamp().Underlying());

                YT_TLOG_DEBUG("Adding unleashed backing store")
                    .With(tablet->GetLoggingTags())
                    .With("BackingStoreId", backingStoreId)
                    .With("MaxTimestamp", backingStore->GetMaxTimestamp());

                // COMPAT(ponasenko-rs)
                if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                    ETabletReign::AddConflictHorizon)
                {
                    AddUnleashedBackingStore(tablet, backingStore->AsSortedDynamic());
                }
            } else {
                // NB: It is important to use conflict_horizon_timestamp instead of backingStore->GetMaxTimestamp()
                // on non-active servant and for other cases it is also safe.

                // COMPAT(ponasenko-rs)
                if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >=
                    ETabletReign::AddConflictHorizon)
                {
                    tablet->AdvanceTransientConflictHorizonTimestamp(
                        FromProto<TTimestamp>(request->conflict_horizon_timestamp()),
                        /*expectedMountRevision*/ std::nullopt);
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
            YT_TLOG_DEBUG("Dynamic store id added to the pool")
                .With(tablet->GetLoggingTags())
                .With("StoreId", storeId);

            allocatedDynamicStoreId = storeId;
        }

        auto& movementData = tablet->SmoothMovementData();
        bool isCommonFlush = updateReason == ETabletStoresUpdateReason::Flush &&
            movementData.CommonDynamicStoreIds().contains(removedStoreIds[0]);

        if (isCommonFlush) {
            movementData.CommonDynamicStoreIds().erase(removedStoreIds[0]);
        }

        YT_TLOG_INFO("Tablet stores update committed")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transaction->GetId())
            .With("AddedStoreIds", MakeFormattableView(addedStores, TStoreIdFormatter()))
            .With("RemovedStoreIds", removedStoreIds)
            .With("AddedHunkChunkIds", MakeFormattableView(addedHunkChunks, THunkChunkIdFormatter()))
            .With("RemovedHunkChunkIds", removedHunkChunkIds)
            .With("AddedCompressionDictionaryIds", compressionDictionaryIds)
            .With("RetainedTimestamp", retainedTimestamp)
            .With("UpdateReason", updateReason);

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
    }

    void HydraSplitPartition(TReqSplitPartition* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetActiveServantMountRevision()) {
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
            YT_TLOG_INFO("Partition split failed")
                .With(tablet->GetLoggingTags())
                .With("PartitionId", partitionId)
                .With("Keys", JoinToString(pivotKeys, TStringBuf(" .. ")));
            return;
        }

        UpdateTabletSnapshot(tablet);

        YT_TLOG_INFO("Partition split")
            .With(tablet->GetLoggingTags())
            .With("OriginalPartitionId", partitionId)
            .With("ResultingPartitionIds", MakeFormattableView(
                TRange(
                    tablet->PartitionList().data() + partitionIndex,
                    tablet->PartitionList().data() + partitionIndex + pivotKeys.size()),
                TPartitionIdFormatter()))
            .With("DataSize", partitionDataSize)
            .With("Keys", JoinToString(pivotKeys, TStringBuf(" .. ")));
    }

    void HydraMergePartitions(TReqMergePartitions* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto firstPartitionId = FromProto<TPartitionId>(request->partition_id());
        auto* firstPartition = tablet->GetPartition(firstPartitionId);

        int firstPartitionIndex = firstPartition->GetIndex();
        int lastPartitionIndex = firstPartitionIndex + request->partition_count() - 1;

        auto originalPartitionIds = Format("%v",
            MakeFormattableView(
                TRange(
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

        YT_TLOG_INFO("Partitions merged")
            .With(tablet->GetLoggingTags())
            .With("OriginalPartitionIds", originalPartitionIds)
            .With("ResultingPartitionId", tablet->PartitionList()[firstPartitionIndex]->GetId())
            .With("DataSize", partitionsDataSize);
    }

    void HydraUpdatePartitionSampleKeys(TReqUpdatePartitionSampleKeys* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetActiveServantMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->FindPartition(partitionId);
        if (!partition) {
            return;
        }

        auto reader = CreateWireProtocolReader(
            TSharedRef::FromString(request->sample_keys()),
            New<TRowBuffer>(
                TSampleKeyListTag(),
                TChunkedMemoryPool::DefaultStartChunkSize,
                Bootstrap_
                    ->GetNodeMemoryUsageTracker()
                    ->WithCategory(EMemoryCategory::TabletFootprint),
                /*allowMemoryOvercommit*/ true));
        auto sampleKeys = reader->ReadUnversionedRowset(true);

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->UpdatePartitionSampleKeys(partition, sampleKeys);

        UpdateTabletSnapshot(tablet);

        YT_TLOG_INFO("Partition sample keys updated")
            .With(tablet->GetLoggingTags())
            .With("PartitionId", partition->GetId())
            .With("SampleKeyCount", sampleKeys.Size());
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
            THROW_ERROR WrapHydraError(TError("Invalid replica mode %Qlv", *mode));
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

        YT_TLOG_INFO("Table replica updated")
            .With(tablet->GetLoggingTags())
            .With("ReplicaId", replicaInfo->GetId())
            .With("Enabled", enabled)
            .With("Mode", mode)
            .With("Atomicity", atomicity)
            .With("PreserveTimestamps", preserveTimestamps);
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

        if (transaction->IsExternalizedToThisCell()) {
            YT_TLOG_DEBUG("Preparing pull rows update under externalized transaction")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tabletId);
        }

        ValidatePreparingTransactionIsProperlyExternalized(tablet, transaction, "pull rows");

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

        if (chaosData->PreparedWritePulledRowsTransactionId.Load()) {
            THROW_ERROR_EXCEPTION("Another pulled rows write is in progress")
                .With("transaction_id", transaction->GetId())
                .With("write_pull_rows_transaction_id", chaosData->PreparedWritePulledRowsTransactionId.Load());
        }

        // COMPAT(savrus)
        int reign = GetCurrentMutationContext()->Request().Reign;
        if (reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare) ||
            (reign < static_cast<int>(ETabletReign::Start_25_2) &&
            reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare_25_1)))
        {
            if (chaosData->PreparedAdvanceReplicationProgressTransactionId.Load()) {
                THROW_ERROR_EXCEPTION("Another replication progress advance is in progress")
                    .With("transaction_id", transaction->GetId())
                    .With("advance_replication_progress_transaction_id", chaosData->PreparedAdvanceReplicationProgressTransactionId.Load());
            }
        }

        auto newProgress = FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress());
        if (newProgress.Segments.empty()) {
            THROW_ERROR_EXCEPTION("Empty progress");
        }
        if (CompareRows(newProgress.Segments.front().LowerKey.Get(), tablet->GetPivotKey()) != 0) {
            THROW_ERROR_EXCEPTION("Replication progress boundaries differ from tablet pivot keys")
                .With("tablet_lower_key", tablet->GetPivotKey())
                .With("progress_lower_key", newProgress.Segments.front().LowerKey.Get());
        }
        if (CompareRows(newProgress.UpperKey.Get(), tablet->GetNextPivotKey()) != 0) {
            THROW_ERROR_EXCEPTION("Replication progress boundaries differ from tablet pivot keys")
                .With("tablet_upper_key", tablet->GetNextPivotKey())
                .With("progress_upper_key", newProgress.UpperKey.Get());
        }

        if (tablet->IsActiveServant()) {
            tablet->ValidateServantIsWritable(GetCellDirectory())
                .ThrowOnError();
        }

        chaosData->PreparedWritePulledRowsTransactionId.Store(transaction->GetId());

        const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
        tabletCellWriteManager->AddPersistentAffectedTablet(transaction, tablet);

        YT_TLOG_DEBUG("Write pulled rows prepared")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId())
            .With("ReplicationRound", round);
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

        if (transaction->IsCoarseSerializationNeeded()) {
            YT_TLOG_DEBUG("Write pull rows committed and is waiting for serialization")
                .With("TabletId", tabletId)
                .With("TransactionId", transaction->GetId())
                .With("ReplicationRound", round);
            return;
        }

        FinalizeWritePulledRows(transaction, request, true);
    }

    void HydraSerializeWritePulledRows(TTransaction* transaction, TReqWritePulledRows* request)
    {
        FinalizeWritePulledRows(transaction, request, false);
    }

    bool HydraNeedExternalizeWritePullRows(TTransaction* /*transaction*/, TReqWritePulledRows* request, TTabletId tabletId)
    {
        return tabletId == FromProto<TTabletId>(request->tablet_id());
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
        if (chaosData->PreparedWritePulledRowsTransactionId.Load() != transaction->GetId()) {
            YT_TLOG_ALERT("Unexpected write pull rows transaction finalized, ignored")
                .With("TransactionId", transaction->GetId())
                .With("ExpectedTransactionId", chaosData->PreparedWritePulledRowsTransactionId.Load())
                .With("TabletId", tablet->GetId());
            return;
        }

        chaosData->PreparedWritePulledRowsTransactionId.Store(NullTransactionId);

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
        if (chaosData->PreparedWritePulledRowsTransactionId.Load() != transaction->GetId()) {
            return;
        }

        chaosData->PreparedWritePulledRowsTransactionId.Store(NullTransactionId);

        YT_TLOG_DEBUG("Write pulled rows aborted")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId());
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

        if (transaction->IsExternalizedToThisCell()) {
            YT_TLOG_DEBUG("Preparing replication progress advance update under externalized transaction")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tabletId);
        }

        ValidatePreparingTransactionIsProperlyExternalized(tablet, transaction, "replication progress advance");

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

        if (chaosData->PreparedAdvanceReplicationProgressTransactionId.Load()) {
            THROW_ERROR_EXCEPTION("Another replication progress advance is in progress")
                .With("transaction_id", transaction->GetId())
                .With("advance_replication_progress_transaction_id", chaosData->PreparedAdvanceReplicationProgressTransactionId.Load());
        }

        // COMPAT(savrus)
        int reign = GetCurrentMutationContext()->Request().Reign;
        if (reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare) ||
            (reign < static_cast<int>(ETabletReign::Start_25_2) &&
            reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare_25_1)))
        {
            if (chaosData->PreparedWritePulledRowsTransactionId.Load()) {
                THROW_ERROR_EXCEPTION("Another pulled rows write is in progress")
                    .With("transaction_id", transaction->GetId())
                    .With("write_pull_rows_transaction_id", chaosData->PreparedWritePulledRowsTransactionId.Load());
            }
        }

        if (tablet->IsActiveServant()) {
            tablet->ValidateServantIsWritable(GetCellDirectory())
                .ThrowOnError();
        }

        chaosData->PreparedAdvanceReplicationProgressTransactionId.Store(transaction->GetId());

        const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
        tabletCellWriteManager->AddPersistentAffectedTablet(transaction, tablet);

        transaction->ForceSerialization(tabletId);

        YT_TLOG_DEBUG("Prepared replication progress advance transaction")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId());
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

        if (round && replicationRound != *round) {
            YT_TLOG_ALERT("Unexpected replication progress advance transaction serialized, ignored")
                .With("TransactionId", transaction->GetId())
                .With("ReplicationRound", *round)
                .With("ExpectedReplicationRound", replicationRound)
                .With("TabletId", tablet->GetId());
            return;
        }

        if (chaosData->PreparedAdvanceReplicationProgressTransactionId.Load() != transaction->GetId()) {
            YT_TLOG_ALERT("Unexpected replication progress advance transaction serialized, ignored")
                .With("TransactionId", transaction->GetId())
                .With("ExpectedTransactionId", chaosData->PreparedAdvanceReplicationProgressTransactionId.Load())
                .With("TabletId", tablet->GetId());
            return;
        }

        chaosData->PreparedAdvanceReplicationProgressTransactionId.Store(NullTransactionId);

        auto progress = New<TRefCountedReplicationProgress>(FromProto<NChaosClient::TReplicationProgress>(request->new_replication_progress()));
        bool validateStrictAdvance = request->validate_strict_advance();

        // NB: It is legitimate for `progress` to be less than `tabletProgress`: tablet progress could have been
        // updated by some recent transaction while `progress` has been constructed even before `transaction` started.
        auto tabletProgress = tablet->RuntimeData()->ReplicationProgress.Acquire();
        bool isStrictlyAdvanced = IsReplicationProgressGreaterOrEqual(*progress, *tabletProgress);

        YT_TLOG_DEBUG("Serializing advance replication progress transaction")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId())
            .With("IsStrictlyAdvanced", isStrictlyAdvanced)
            .With("CurrentProgress", static_cast<NChaosClient::TReplicationProgress>(*tabletProgress))
            .With("NewProgress", static_cast<NChaosClient::TReplicationProgress>(*progress))
            .With("ReplicationRound", round);

        if (isStrictlyAdvanced) {
            tablet->RuntimeData()->ReplicationProgress.Store(progress);

            YT_TLOG_DEBUG("Updated tablet replication progress")
                .With("TabletId", tabletId)
                .With("TransactionId", transaction->GetId())
                .With("ReplicationProgress", static_cast<NChaosClient::TReplicationProgress>(*progress));
        } else if (validateStrictAdvance) {
            YT_TLOG_ALERT("Failed to advance tablet replication progress because current tablet progress is greater")
                .With("TabletId", tabletId)
                .With("TransactionId", transaction->GetId())
                .With("CurrentProgress", static_cast<NChaosClient::TReplicationProgress>(*tabletProgress))
                .With("NewProgress", static_cast<NChaosClient::TReplicationProgress>(*progress));
        }

        // COMPAT(savrus)
        if (round) {
            chaosData->ReplicationRound = *round + 1;
        }

        YT_TLOG_DEBUG("Serialized replication progress advance transaction")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId());
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
        if (chaosData->PreparedAdvanceReplicationProgressTransactionId.Load() != transaction->GetId()) {
            return;
        }

        chaosData->PreparedAdvanceReplicationProgressTransactionId.Store(NullTransactionId);

        YT_TLOG_DEBUG("Replication progress advance aborted")
            .With("TabletId", tabletId)
            .With("TransactionId", transaction->GetId());
    }

    bool HydraNeedExternalizeAdvanceReplicationProgress(
        TTransaction* /*transaction*/,
        TReqAdvanceReplicationProgress* request,
        TTabletId tabletId)
    {
        return tabletId == FromProto<TTabletId>(request->tablet_id());
    }

    void HydraAdvanceReplicationEra(
        TReqAdvanceReplicationEra* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto newReplicationEra = FromProto<TReplicationEra>(request->new_replication_era());

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            YT_TLOG_DEBUG("Tablet is missing during advancement of replication era")
                .With("TabletId", tabletId)
                .With("NewReplicationEra", newReplicationEra);
            return;
        }
        if (auto era = tablet->RuntimeData()->ReplicationEra.load();
            era == InvalidReplicationEra || era < newReplicationEra)
        {
            tablet->RuntimeData()->ReplicationEra.store(newReplicationEra);

            YT_TLOG_DEBUG("Replication era advanced")
                .With("TabletId", tabletId)
                .With("NewReplicationEra", newReplicationEra)
                .With("OldReplicationEra", era);
        } else if (era > newReplicationEra) {
            YT_TLOG_ALERT("Trying to advance to older era")
                .With("TabletId", tabletId)
                .With("CurrentReplicationEra", era)
                .With("NewReplicationEra", newReplicationEra);
        } else {
            // Might happen if TabletPuller hasn't seen the new era yet.
            YT_TLOG_DEBUG("Replication era is already advanced")
                .With("TabletId", tabletId)
                .With("NewReplicationEra", newReplicationEra);
        }
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
                    .With("checkpoint_timestamp", checkpointTimestamp)
                    .With("start_timestamp", transaction->GetStartTimestamp());
            }
        }

        if (auto lastPassedCheckpointTimestamp = tablet->BackupMetadata().GetLastPassedCheckpointTimestamp()) {
            if (transaction->GetStartTimestamp() <= lastPassedCheckpointTimestamp) {
                THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v since tablet %v has passed "
                    "backup checkpoint exceeding transaction start timestamp",
                    replicaId,
                    tabletId)
                    .With("last_passed_checkpoint_timestamp", lastPassedCheckpointTimestamp)
                    .With("start_timestamp", transaction->GetStartTimestamp());
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

        // COMPAT(ponasenko-rs)
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

        YT_TLOG_DEBUG("Async replicated rows prepared")
            .With("TabletId", tabletId)
            .With("ReplicaId", replicaId)
            .With("TransactionId", transaction->GetId())
            .WithFormat("CurrentReplicationRowIndex", "%v -> %v", replicaInfo->GetCurrentReplicationRowIndex(), newReplicationRowIndex)
            .With("TotalRowCount", tablet->GetTotalRowCount())
            .WithFormat("CurrentReplicationTimestamp", "%v -> %v", replicaInfo->GetCurrentReplicationTimestamp(), newReplicationTimestamp);
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
            YT_TLOG_ALERT("Unexpected replication transaction finalized, ignored")
                .With("TransactionId", transaction->GetId())
                .With("ExpectedTransactionId", replicaInfo->GetPreparedReplicationTransactionId())
                .With("TabletId", tablet->GetId());
            return;
        }

        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        BackupManager_->ValidateReplicationTransactionCommit(tablet, transaction);

        // COMPAT(babenko, ponasenko-rs)
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
        auto newCurrentReplicationTimestamp = FromProto<NTransactionClient::TTimestamp>(request->new_replication_timestamp());

        if (newCurrentReplicationRowIndex < prevCurrentReplicationRowIndex) {
            YT_TLOG_ALERT("CurrentReplicationIndex went back")
                .With("TabletId", tabletId)
                .With("ReplicaId", replicaId)
                .With("TransactionId", transaction->GetId())
                .WithFormat("CurrentReplicationRowIndex", "%v -> %v", prevCurrentReplicationRowIndex, newCurrentReplicationRowIndex);
            newCurrentReplicationRowIndex = prevCurrentReplicationRowIndex;
        }
        if (newCurrentReplicationTimestamp < prevCurrentReplicationTimestamp) {
            YT_TLOG_ALERT("CurrentReplicationTimestamp went back")
                .With("TabletId", tabletId)
                .With("ReplicaId", replicaId)
                .With("TransactionId", transaction->GetId())
                .WithFormat("CurrentReplicationTimestamp", "%v -> %v", prevCurrentReplicationTimestamp, newCurrentReplicationTimestamp);
            newCurrentReplicationTimestamp = prevCurrentReplicationTimestamp;
        }

        replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCommittedReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);
        replicaInfo->RecomputeReplicaStatus();

        AdvanceReplicatedTrimmedRowCount(tablet, transaction);

        YT_TLOG_DEBUG("Async replicated rows committed")
            .With("TabletId", tabletId)
            .With("ReplicaId", replicaId)
            .With("TransactionId", transaction->GetId())
            .WithFormat("CurrentReplicationRowIndex", "%v -> %v",
                prevCurrentReplicationRowIndex,
                replicaInfo->GetCurrentReplicationRowIndex())
            .WithFormat("CommittedReplicationRowIndex", "%v -> %v",
                prevCommittedReplicationRowIndex,
                replicaInfo->GetCommittedReplicationRowIndex())
            .WithFormat("CurrentReplicationTimestamp", "%v -> %v",
                prevCurrentReplicationTimestamp,
                replicaInfo->GetCurrentReplicationTimestamp())
            .WithFormat("TrimmedRowCount", "%v -> %v", prevTrimmedRowCount, tablet->GetTrimmedRowCount())
            .With("TotalRowCount", tablet->GetTotalRowCount());

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

        YT_TLOG_DEBUG("Async replicated rows aborted")
            .With("TabletId", tabletId)
            .With("ReplicaId", replicaId)
            .With("TransactionId", transaction->GetId())
            .WithFormat("CurrentReplicationRowIndex", "%v -> %v",
                replicaInfo->GetCurrentReplicationRowIndex(),
                request->new_replication_row_index())
            .With("TotalRowCount", tablet->GetTotalRowCount())
            .WithFormat("CurrentReplicationTimestamp", "%v -> %v",
                replicaInfo->GetCurrentReplicationTimestamp(),
                request->new_replication_timestamp());

        ReplicationTransactionFinished_.Fire(tablet, replicaInfo);
    }

    bool HydraNeedExternalizeReplicateRows(
        TTransaction* /*transaction*/,
        TReqReplicateRows* request,
        TTabletId tabletId)
    {
        return tabletId == FromProto<TTabletId>(request->tablet_id());
    }

    void HydraDecommissionTabletCell(TReqDecommissionTabletCellOnNode* /*request*/)
    {
        YT_TLOG_INFO("Tablet cell is decommissioning");

        CellLifeStage_ = ETabletCellLifeStage::DecommissioningOnNode;
        SetTabletCellSuspend(/*suspend*/ true);

        Slot_->GetTransactionManager()->SetRemoving();
    }

    void HydraSuspendTabletCell(NTabletServer::NProto::TReqSuspendTabletCell* /*request*/)
    {
        YT_VERIFY(HasHydraContext());

        YT_TLOG_INFO("Suspending tablet cell");

        SetTabletCellSuspend(/*suspend*/ true);
        Suspending_ = true;
    }

    void HydraResumeTabletCell(NTabletServer::NProto::TReqResumeTabletCell* /*request*/)
    {
        YT_VERIFY(HasHydraContext());

        YT_TLOG_INFO("Resuming tablet cell");

        SetTabletCellSuspend(/*suspend*/ false);
        Suspending_ = false;

        PostTabletCellSuspensionToggledMessage(/*suspended*/ false);
    }

    void HydraSetCustomRuntimeData(TReqSetCustomRuntimeData* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (request->has_custom_runtime_data()) {
            constexpr int CustomRuntimeDataTruncateLimit = 100;
            YT_TLOG_INFO("Set custom runtime data for tablet")
                .With(tablet->GetLoggingTags())
                .With("CustomRuntimeData", TruncateString(
                        ConvertToYsonString(ConvertToNode(request->custom_runtime_data()), EYsonFormat::Text).ToString(),
                        CustomRuntimeDataTruncateLimit));

            tablet->CustomRuntimeData() = TYsonString(request->custom_runtime_data());
        } else {
            YT_TLOG_INFO("Set empty custom runtime data for tablet")
                .With(tablet->GetLoggingTags());

            tablet->CustomRuntimeData() = TYsonString();
        }
    }

    void HydraUnregisterMasterAvenueEndpoint(TReqUnregisterMasterAvenueEndpoint* request)
    {
        auto masterEndpointId = FromProto<TAvenueEndpointId>(request->master_avenue_endpoint_id());

        const auto& hiveManager = Slot_->GetHiveManager();
        if (!hiveManager->FindMailbox(masterEndpointId)) {
            YT_TLOG_ALERT("Requested to unregister unexisting master avenue, ignored")
                .With("MasterEndpointId", masterEndpointId);
            return;
        }

        YT_TLOG_DEBUG("Master avenue endpoint unregistered")
            .With("MasterEndpointId", masterEndpointId);

        UnregisterMasterAvenue(masterEndpointId);
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
        auto mailbox = Slot_->GetMasterMailbox();
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

        YT_TLOG_INFO("Checking if tablet cell is decommissioned")
            .With("LifeStage", CellLifeStage_)
            .With("TabletMapEmpty", TabletMap_.empty())
            .With("TransactionManagerDecommissioned", transactionManagerDecommissioned)
            .With("TransactionSupervisorDecommissioned", transactionSupervisorDecommissioned)
            .With("LeaseManagerDecommissioned", leaseManagerDecommissioned);

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

        YT_TLOG_INFO("Tablet cell decommissioned");

        CellLifeStage_ = ETabletCellLifeStage::Decommissioned;

        const auto& hiveManager = Slot_->GetHiveManager();
        auto mailbox = Slot_->GetMasterMailbox();
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

        YT_TLOG_INFO("Checking if tablet cell is suspended")
            .With("TransactionManagerDecommissioned", transactionManagerDecommissioned)
            .With("TransactionSupervisorDecommissioned", transactionSupervisorDecommissioned)
            .With("LeaseManagerDecommissioned", leaseManagerDecommissioned);

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

        YT_TLOG_INFO("Tablet cell is suspended")
            .With("Suspending", Suspending_)
            .With("TransactionManagerDecommissioned", Slot_->GetTransactionManager()->IsDecommissioned())
            .With("TransactionSupervisorDecommissioned", Slot_->GetTransactionSupervisor()->IsDecommissioned());

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
        req.set_mount_revision(ToProto(tablet->GetMountRevision()));
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
            YT_TLOG_DEBUG("Dynamic store id sent to a tablet in a wrong state, ignored")
                .With(tablet->GetLoggingTags())
                .With("State", state);
            return;
        }

        auto dynamicStoreId = FromProto<TDynamicStoreId>(request->dynamic_store_id());
        tablet->PushDynamicStoreIdToPool(dynamicStoreId);
        tablet->SetDynamicStoreIdRequested(false);
        UpdateTabletSnapshot(tablet);

        YT_TLOG_DEBUG("Dynamic store allocated for a tablet")
            .With(tablet->GetLoggingTags())
            .With("DynamicStoreId", dynamicStoreId);
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
            YT_TLOG_INFO("Dynamic memory store is orphaned and will be kept")
                .With("StoreId", store->GetId())
                .With("TabletId", tablet->GetId())
                .With("LockCount", lockCount);
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
            YT_TLOG_INFO("Store unlocked and will be dropped")
                .With("StoreId", store->GetId());
            YT_VERIFY(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }

    const ILeaseManagerPtr& GetLeaseManager() const final
    {
        return Slot_->GetLeaseManager();
    }

    TFuture<void> IssueLeases(const std::vector<TLeaseId>& leaseIds) override
    {
        const auto& leaseManager = Slot_->GetLeaseManager();
        const auto& connection = Bootstrap_->GetConnection();
        const auto& hiveManager = Slot_->GetHiveManager();

        return IssueLeasesForCell(
            leaseIds,
            leaseManager,
            hiveManager,
            GetCellId(),
            /*synWithAllLeaseTransactionCoordinators*/ false,
            BIND([connection] (TCellTag cellTag) {
                return connection->GetMasterCellId(cellTag);
            }),
            BIND([connection] (TCellTag cellTag) {
                return connection->FindMasterChannel(EMasterChannelKind::Leader, cellTag);
            }));
    }

    void SetTabletOrphaned(std::unique_ptr<TTablet> tabletHolder)
    {
        auto id = tabletHolder->GetId();
        tabletHolder->SetState(ETabletState::Orphaned);
        YT_TLOG_DEBUG("Tablet is orphaned and will be kept")
            .With("TabletId", id)
            .With("LockCount", tabletHolder->GetTotalTabletLockCount());
        YT_VERIFY(OrphanedTablets_.emplace(id, std::move(tabletHolder)).second);
    }

    void OnTabletUnlocked(TTablet* tablet)
    {
        CheckIfTabletFullyUnlocked(tablet);
        if (tablet->GetState() == ETabletState::Orphaned && tablet->GetTotalTabletLockCount() == 0) {
            auto id = tablet->GetId();
            YT_TLOG_INFO("Tablet unlocked and will be dropped")
                .With("TabletId", id);
            YT_VERIFY(OrphanedTablets_.erase(id) == 1);
        }
    }

    void OnTabletRowUnlocked(TTablet* tablet) override
    {
        CheckIfTabletFullyUnlocked(tablet);
    }

    ISimpleHydraManagerPtr GetHydraManager() const final
    {
        return Slot_->GetSimpleHydraManager();
    }

    void AbortAllTransactions(TTablet* tablet) override
    {
        if (!IsLeader()) {
            return;
        }

        const auto& tabletWriteManager = tablet->GetTabletWriteManager();
        const auto& transactionManager = GetTransactionManager();
        const auto& transactionSupervisor = Slot_->GetTransactionSupervisor();

        for (auto transactionId : tabletWriteManager->GetAffectingTransactionIds()) {
            auto* transaction = transactionManager->FindTransaction(transactionId);
            if (!transaction) {
                continue;
            }

            // Fast path: transaction abort has  already been requested.
            if (transaction->GetTransientState() == ETransactionState::TransientAbortPrepared) {
                continue;
            }

            // Fast path: transaction cannot be aborted, do not issue useless mutations.
            auto persistentState = transaction->GetPersistentState();
            if (persistentState == ETransactionState::PersistentCommitPrepared ||
                persistentState == ETransactionState::CommitPending ||
                persistentState == ETransactionState::Committed ||
                persistentState == ETransactionState::Serialized)
            {
                continue;
            }

            YT_TLOG_DEBUG("Aborting transaction by out-of-order tablet request")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transactionId)
                .With("PersistentState", transaction->GetPersistentState())
                .With("TransientState", transaction->GetTransientState());

            transactionSupervisor->AbortTransaction(transactionId)
                // TODO(ifsmirnov): remove subscription with excessive logging
                // after some testing.
                .Subscribe(BIND([transactionId, Logger = Logger] (const TError& error) {
                    if (error.IsOK()) {
                        YT_TLOG_DEBUG("Transaction aborted by out-of-order tablet request")
                            .With("TransactionId", transactionId);
                    } else {
                        YT_TLOG_DEBUG("Error aborting transaction by out-of-order tablet request")
                            .With("TransactionId", transactionId)
                            .With(error);
                    }
                }));
        }
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

        YT_TLOG_INFO("All tablet locks released")
            .With(tablet->GetLoggingTags())
            .With("NewState", newTransientState);

        {
            TReqSetTabletState request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(ToProto(tablet->GetMountRevision()));
            request.set_state(ToProto(newPersistentState));
            Slot_->CommitTabletMutation(request);
        }
    }

    void CheckIfTabletFullyFlushed(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        if (auto storeId = tablet->GetProvisionallyFlushingStoreId();
            storeId && !tablet->FindStore(storeId))
        {
            tablet->SetProvisionallyFlushingStoreId(NullStoreId);
            TReqReportTabletProvisionallyFlushed request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            Slot_->CommitTabletMutation(request);
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

        YT_TLOG_INFO("All tablet stores flushed")
            .With(tablet->GetLoggingTags())
            .With("NewState", newTransientState);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(ToProto(tablet->GetMountRevision()));
        request.set_state(ToProto(newPersistentState));
        Slot_->CommitTabletMutation(request);
    }

    void PostMasterMessage(
        TTablet* tablet,
        const ::google::protobuf::MessageLite& message,
        bool forceCellMailbox = false) override
    {
        // Used in tests only. NB: Synchronous sleep is required since we don't expect
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
            auto mailbox = hiveManager->GetMailbox(avenueEndpointId);
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

        YT_TLOG_DEBUG("Started precaching chunk replicas")
            .With("StoreChunkCount", storeChunkIds.size())
            .With("HunkChunkCount", hunkChunkIds.size());

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
                YT_TLOG_DEBUG("Finished precaching chunk replicas");
            }));
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
                Bootstrap_->GetOutThrottler(EWorkloadCategory::SystemTabletReplication),
                Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TableReplication),
                Bootstrap_->GetErrorManager());
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
            Bootstrap_->GetReplicatorClientCache()->GetLocalClient(),
            Bootstrap_->GetReplicationCardUpdatesBatcher()));
        tablet->SetTablePuller(CreateTablePuller(
            Config_,
            tablet,
            Bootstrap_->GetReplicatorClientCache(),
            Slot_,
            Bootstrap_->GetTabletSnapshotStore(),
            CreateSerializedInvoker(Bootstrap_->GetTableReplicatorPoolInvoker()),
            Bootstrap_->GetInThrottler(EWorkloadCategory::SystemTabletReplication),
            Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::ChaosReplicationIncoming),
            Bootstrap_->GetErrorManager()));
    }

    void RemoveChaosAgent(TTablet* tablet)
    {
        tablet->SetChaosAgent(nullptr);
        tablet->SetTablePuller(nullptr);
    }

    void StartChaosReplicaEpoch(TTablet* tablet, TReplicationCardId replicationCardId)
    {
        if (!IsLeader()) {
            return;
        }

        AddChaosAgent(tablet, replicationCardId);
        tablet->GetChaosAgent()->Enable();
        tablet->GetTablePuller()->Enable();
        tablet->ChaosData()->PullerReplicaCache.Store(CreatePullerReplicaCache(tablet, replicationCardId));
    }

    void StopChaosReplicaEpoch(TTablet* tablet)
    {
        if (tablet->GetTablePuller()) {
            tablet->GetTablePuller()->Disable();
        }

        if (tablet->GetChaosAgent()) {
            tablet->GetChaosAgent()->Disable();
        }

        tablet->ChaosData()->PullerReplicaCache.Store(GetDisabledPullerReplicaCache());
    }

    void SetBackingStore(TTablet* tablet, const IChunkStorePtr& store, const IDynamicStorePtr& backingStore)
    {
        store->SetBackingStore(backingStore);
        YT_TLOG_DEBUG("Backing store set")
            .With(tablet->GetLoggingTags())
            .With("StoreId", store->GetId())
            .With("BackingStoreId", backingStore->GetId())
            .With("BackingDynamicMemoryUsage", backingStore->GetDynamicMemoryUsage());
        tablet->GetStructuredLogger()->OnBackingStoreSet(store, backingStore);

        TDelayedExecutor::Submit(
            // NB: Submit the callback via the regular automaton invoker, not the epoch one since
            // we need the store to be released even if the epoch ends.
            BIND(
                &TTabletManager::ReleaseBackingStoreWeak,
                MakeWeak(this),
                MakeWeak(store),
                tablet->GetMountRevision())
                .Via(Slot_->GetAutomatonInvoker()),
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime);
    }

    void AddUnleashedBackingStore(TTablet* tablet, const TSortedDynamicStorePtr& backingStore)
    {
        tablet->GetStoreManager()->AsSorted()->AddUnleashedBackingStore(backingStore);

        TDelayedExecutor::Submit(
            // NB: Submit the callback via the epoch automaton invoker which is a different approach from the one in SetBackingStore.
            // TSortedStoreManager::UnleashedBackingStores_ will be cleared on epoch end.
            BIND(
                &TTabletManager::ReleaseUnleashedBackingStoreWeak,
                MakeWeak(this),
                tablet->GetId(),
                backingStore->GetId(),
                tablet->GetMountRevision())
                .Via(Slot_->GetEpochAutomatonInvoker()),
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime);
    }

    void ReleaseBackingStoreWeak(
        const TWeakPtr<IChunkStore>& storeWeak,
        TRevision expectedMountRevision)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (auto store = storeWeak.Lock()) {
            ReleaseBackingStore(store);

            if (auto* tablet = FindTablet(store->GetTabletId())) {
                tablet->AdvanceTransientConflictHorizonTimestamp(store->GetMaxTimestamp(), expectedMountRevision);
            }
        }
    }

    void ReleaseUnleashedBackingStoreWeak(
        TTabletId tabletId,
        TDynamicStoreId backingStoreToRemoveId,
        TRevision expectedMountRevision)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        // It is possible that at this point this tablet is a recreated c++ object after unmount and mount.
        // Sorted store manager is ready for this.
        // TODO(ponasenko-rs): Use TTablet::CancelableContext_ to simplify interface.
        tablet->GetStoreManager()->AsSorted()->ReleaseUnleashedBackingStore(backingStoreToRemoveId, expectedMountRevision);
    }

    void ValidateMemoryLimit(const std::optional<std::string>& poolTag) override
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

        if (HasMutationContext() && extraMountConfigAttributes) {
            for (auto key : TBuiltinTableMountConfig::NonDynamicallyModifiableFields) {
                if (extraMountConfigAttributes->RemoveChild(key)) {
                    YT_TLOG_DEBUG("Removed invalid builtin key from extra mount config")
                        .With("TabletId", tabletId)
                        .With("Key", key);
                }
            }
        }

        // COMPAT(navasardianna): EMasterReign::SendTableTabletBalancerConfigToTablet.
        auto tabletBalancerConfig = tableSettings.has_tablet_balancer_config()
            ? ConvertTo<IMapNodePtr>(TYsonString(tableSettings.tablet_balancer_config()))
            : GetEphemeralNodeFactory()->CreateMap();

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
                    TYsonString(tableSettings.hunk_writer_options()), tabletId),
                .TabletBalancerConfig = tabletBalancerConfig,
            },
            // COMPAT(ifsmirnov)
            .GlobalPatch = tableSettings.has_global_patch()
                ? ConvertTo<TTableConfigPatchPtr>(TYsonString(tableSettings.global_patch()))
                : New<TTableConfigPatch>(),
        };

        // COMPAT(ifsmirnov)
        if (tableSettings.has_experiments()) {
            settings.Experiments = ConvertTo<std::map<std::string, TTableConfigExperimentPtr>>(
                TYsonString(tableSettings.experiments()));
        }

        return settings;
    }

    void SerializeTableSettings(NProto::TTableSettings* request, const TRawTableSettings& settings)
    {
        const auto& provided = settings.Provided;

        ToProto(request->mutable_mount_config(), ConvertToYsonString(provided.MountConfigNode));
        if (provided.ExtraMountConfig) {
            ToProto(request->mutable_extra_mount_config_attributes(), ConvertToYsonString(provided.ExtraMountConfig));
        }
        ToProto(request->mutable_store_reader_config(), ConvertToYsonString(provided.StoreReaderConfig));
        ToProto(request->mutable_hunk_reader_config(), ConvertToYsonString(provided.HunkReaderConfig));
        ToProto(request->mutable_store_writer_config(), ConvertToYsonString(provided.StoreWriterConfig));
        ToProto(request->mutable_store_writer_options(), ConvertToYsonString(provided.StoreWriterOptions));
        ToProto(request->mutable_hunk_writer_config(), ConvertToYsonString(provided.HunkWriterConfig));
        ToProto(request->mutable_hunk_writer_options(), ConvertToYsonString(provided.HunkWriterOptions));

        // COMPAT(navasardianna)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) >= ETabletReign::SendTableTabletBalancerConfigToTablet) {
            ToProto(request->mutable_tablet_balancer_config(), ConvertToYsonString(provided.TabletBalancerConfig));
        }

        ToProto(request->mutable_global_patch(), ConvertToYsonString(settings.GlobalPatch));
        ToProto(request->mutable_experiments(), ConvertToYsonString(settings.Experiments));
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
                YT_TLOG_ERROR("Error deserializing tablet mount config with extra attributes patch")
                    .With("TabletId", tabletId)
                    .With(ex);
                return ConvertTo<TTableMountConfigPtr>(mountConfigMap);
            }
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing tablet mount config")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTableMountConfig>();
        }
    }

    TTabletStoreReaderConfigPtr DeserializeTabletStoreReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing store reader config")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTabletStoreReaderConfig>();
        }
    }

    TTabletHunkReaderConfigPtr DeserializeTabletHunkReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing hunk reader config")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTabletHunkReaderConfig>();
        }
    }

    TTabletStoreWriterConfigPtr DeserializeTabletStoreWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing store writer config")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTabletStoreWriterConfig>();
        }
    }

    TTabletStoreWriterOptionsPtr DeserializeTabletStoreWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing store writer options")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTabletStoreWriterOptions>();
        }
    }

    TTabletHunkWriterConfigPtr DeserializeTabletHunkWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing hunk writer config")
                .With("TabletId", tabletId)
                .With(ex);
            return New<TTabletHunkWriterConfig>();
        }
    }

    TTabletHunkWriterOptionsPtr DeserializeTabletHunkWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error deserializing hunk writer options")
                .With("TabletId", tabletId)
                .With(ex);
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
        store->SetMemoryTracker(Bootstrap_->GetNodeMemoryUsageTracker());
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
                auto finalTimestamp = NullTimestamp;
                auto maxClipTimestamp = NullTimestamp;

                if (descriptor) {
                    if (descriptor->has_chunk_view_descriptor()) {
                        const auto& chunkViewDescriptor = descriptor->chunk_view_descriptor();
                        if (chunkViewDescriptor.has_read_range()) {
                            readRange = FromProto<NChunkClient::TLegacyReadRange>(chunkViewDescriptor.read_range());
                        }
                        if (chunkViewDescriptor.has_override_timestamp()) {
                            finalTimestamp = static_cast<TTimestamp>(chunkViewDescriptor.override_timestamp());
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
                    storeId,
                    chunkId,
                    readRange,
                    finalTimestamp,
                    maxClipTimestamp,
                    tablet,
                    descriptor,
                    StoreContext_,
                    CreateBackendChunkReadersHolder(
                        Bootstrap_,
                        Bootstrap_->GetClient(),
                        Bootstrap_->GetLocalDescriptor(),
                        Bootstrap_->GetChunkRegistry(),
                        tablet->GetSettings().StoreReaderConfig));
            }

            case EStoreType::SortedDynamic:
                return NewWithOffloadedDtor<TSortedDynamicStore>(
                    NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                    storeId,
                    tablet,
                    StoreContext_);

            case EStoreType::OrderedChunk: {
                if (!IsRecovery()) {
                    YT_VERIFY(descriptor);
                    YT_VERIFY(!descriptor->has_chunk_view_descriptor());
                }

                return New<TOrderedChunkStore>(
                    storeId,
                    tablet,
                    descriptor,
                    StoreContext_,
                    CreateBackendChunkReadersHolder(
                        Bootstrap_,
                        Bootstrap_->GetClient(),
                        Bootstrap_->GetLocalDescriptor(),
                        Bootstrap_->GetChunkRegistry(),
                        tablet->GetSettings().StoreReaderConfig));
            }

            case EStoreType::OrderedDynamic:
                return NewWithOffloadedDtor<TOrderedDynamicStore>(
                    NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                    storeId,
                    tablet,
                    StoreContext_);

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
            YT_TLOG_WARNING("Requested to add an already existing table replica")
                .With("TabletId", tablet->GetId())
                .With("ReplicaId", replicaId);
            return nullptr;
        }

        auto [replicaIt, replicaInserted] = replicas.emplace(replicaId, TTableReplicaInfo(tablet, replicaId));
        YT_VERIFY(replicaInserted);
        auto& replicaInfo = replicaIt->second;

        replicaInfo.SetClusterName(descriptor.cluster_name());
        replicaInfo.SetReplicaPath(descriptor.replica_path());
        replicaInfo.SetStartReplicationTimestamp(FromProto<NTransactionClient::TTimestamp>(descriptor.start_replication_timestamp()));
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

        YT_TLOG_INFO("Table replica added")
            .With(tablet->GetLoggingTags())
            .With("ReplicaId", replicaId)
            .With("ClusterName", replicaInfo.GetClusterName())
            .With("ReplicaPath", replicaInfo.GetReplicaPath())
            .With("Mode", replicaInfo.GetMode())
            .With("StartReplicationTimestamp", replicaInfo.GetStartReplicationTimestamp())
            .With("CurrentReplicationRowIndex", replicaInfo.GetCurrentReplicationRowIndex())
            .With("CurrentReplicationTimestamp", replicaInfo.GetCurrentReplicationTimestamp());

        return &replicaInfo;
    }

    void RemoveTableReplica(TTablet* tablet, TTableReplicaId replicaId)
    {
        auto& replicas = tablet->Replicas();
        auto it = replicas.find(replicaId);
        if (it == replicas.end()) {
            YT_TLOG_WARNING("Requested to remove a non-existing table replica")
                .With("TabletId", tablet->GetId())
                .With("ReplicaId", replicaId);
            return;
        }

        auto& replicaInfo = it->second;

        if (!IsRecovery()) {
            StopTableReplicaEpoch(&replicaInfo);
        }

        replicas.erase(it);

        AdvanceReplicatedTrimmedRowCount(tablet, nullptr);
        UpdateTabletSnapshot(tablet);

        YT_TLOG_INFO("Table replica removed")
            .With(tablet->GetLoggingTags())
            .With("ReplicaId", replicaId);
    }


    void EnableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_TLOG_INFO("Table replica enabled")
            .With(tablet->GetLoggingTags())
            .With("ReplicaId", replicaInfo->GetId());

        replicaInfo->SetState(ETableReplicaState::Enabled);

        if (IsLeader()) {
            replicaInfo->GetReplicator()->Enable();
        }

        {
            TRspEnableTableReplica response;
            ToProto(response.mutable_tablet_id(), tablet->GetId());
            ToProto(response.mutable_replica_id(), replicaInfo->GetId());
            response.set_mount_revision(ToProto(tablet->GetMountRevision()));
            PostMasterMessage(tablet, response);
        }
    }

    void DisableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_TLOG_INFO("Table replica disabled")
            .With(tablet->GetLoggingTags())
            .With("ReplicaId", replicaInfo->GetId())
            .With("CurrentReplicationRowIndex", replicaInfo->GetCurrentReplicationRowIndex())
            .With("CurrentReplicationTimestamp", replicaInfo->GetCurrentReplicationTimestamp());

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
            response.set_mount_revision(ToProto(tablet->GetMountRevision()));
            PostMasterMessage(tablet, response);
        }
    }

    void PostTableReplicaStatistics(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        TReqUpdateTableReplicaStatistics request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_replica_id(), replicaInfo.GetId());
        request.set_mount_revision(ToProto(tablet->GetMountRevision()));
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
            masterRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
            masterRequest.set_trimmed_row_count(trimmedRowCount);
            PostMasterMessage(tablet, masterRequest);
        }

        YT_TLOG_DEBUG("Rows trimmed")
            .With("TabletId", tablet->GetId())
            .WithFormat("TrimmedRowCount", "%v -> %v", prevTrimmedRowCount, trimmedRowCount);
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
            trimmedRowCount = lastStore->GetStartingRowIndex() + lastStore->GetRowCount();
            if (trimmedRowCount != minReplicationRowIndex) {
                YT_TLOG_ALERT("Invalid min replication row index; skipping full trim")
                    .With(tablet->GetLoggingTags())
                    .With("MinReplicationRowIndex", minReplicationRowIndex)
                    .With("LastStoreId", lastStore->GetId())
                    .With("LastStoreStartingRowIndex", lastStore->GetStartingRowIndex())
                    .With("LastStoreRowCount", lastStore->GetRowCount());
                return;
            }
        } else {
            trimmedRowCount = it->second->GetStartingRowIndex();
        }

        YT_VERIFY(tablet->GetTrimmedRowCount() <= trimmedRowCount);
        UpdateTrimmedRowCount(tablet, trimmedRowCount);
    }

    const IBackupManagerPtr& GetBackupManager() const final
    {
        return BackupManager_;
    }


    TFuture<void> OnStoresUpdateCommitSemaphoreAcquired(
        TTablet* tablet,
        const ITransactionPtr& transaction,
        TAsyncSemaphoreGuard&&)
    {
        try {
            YT_TLOG_DEBUG("Started committing tablet stores update transaction")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transaction->GetId());

            ExternalizeTransactionIfNeeded(tablet, transaction, "tablet stores update");

            NApi::TTransactionCommitOptions commitOptions{
                .GeneratePrepareTimestamp = false,
            };
            WaitFor(transaction->Commit(commitOptions))
                .ThrowOnError();

            YT_TLOG_DEBUG("Tablet stores update transaction committed")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transaction->GetId());

            return OKFuture;
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
    }

    TCellId GetCellId() const final
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
        YT_TLOG_ALERT_UNLESS(lockAllowed, "Tablet was locked in unexpected state")
            .With("TabletId", tablet->GetId())
            .With("TabletState", state)
            .With("LockType", lockType)
            .With("LockCount", tablet->GetTotalTabletLockCount());

        return tablet->Lock(lockType);
    }

    i64 UnlockTablet(TTablet* tablet, ETabletLockType lockType) override
    {
        auto lockCount = tablet->Unlock(lockType);
        OnTabletUnlocked(tablet);
        return lockCount;
    }

    TTabletNodeDynamicConfigPtr GetDynamicConfig() const override final
    {
        return Bootstrap_->GetTabletNodeDynamicConfig();
    }

    void OnTableDynamicConfigChanged(const TClusterTableConfigPatchSetPtr& /*oldConfig*/)
    {
        YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        Slot_->GetGuardedAutomatonInvoker()->Invoke(BIND(
            &TTabletManager::DoTableDynamicConfigChanged,
            MakeWeak(this),
            Bootstrap_->GetTableDynamicConfigManager()->GetConfig()));
    }

    void OnDynamicConfigChanged(
        const TTabletNodeDynamicConfigPtr& oldConfig,
        const TTabletNodeDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        Slot_->GetGuardedAutomatonInvoker()->Invoke(BIND(
            &TTabletManager::DoDynamicConfigChanged,
            MakeWeak(this),
            oldConfig,
            newConfig));
    }

    void DoTableDynamicConfigChanged(const TClusterTableConfigPatchSetPtr& patch)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (!IsLeader()) {
            return;
        }

        auto globalPatch = static_cast<TTableConfigPatchPtr>(patch);

        YT_TLOG_DEBUG("Observing new table dynamic config")
            .With("ExperimentNames", MakeFormattableView(
                    patch->TableConfigExperiments,
                    [] (auto* builder, const auto& experiment) {
                        FormatValue(builder, experiment.first, /*format*/ TStringBuf{});
                    }));

        auto globalPatchYson = ConvertToYsonString(globalPatch);
        auto experimentsYson = ConvertToYsonString(patch->TableConfigExperiments).ToString();

        for (const auto& [id, tablet] : Tablets()) {
            ScheduleTabletConfigUpdate(tablet, patch, globalPatchYson, experimentsYson);
        }
    }

    void DoDynamicConfigChanged(
        const TTabletNodeDynamicConfigPtr& oldConfig,
        const TTabletNodeDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (auto& [_, tablet] : Tablets()) {
            tablet->OnDynamicConfigChanged(Slot_, oldConfig, newConfig);
        }

        const auto& storeCompactorConfig = newConfig->StoreCompactor;
        for (auto [storeKind, partitionKind] : NLsm::StoreCompactionHintKinds) {
            CompactionHintFetchers_[storeKind]->Reconfigure(
                storeCompactorConfig->CompactionHintFetchers[storeKind]);
        }
    }

    void ScheduleTabletConfigUpdate(
        TTablet* tablet,
        const TClusterTableConfigPatchSetPtr& patch,
        const TYsonString& globalPatchYson,
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
            req.set_mount_revision(ToProto(tablet->GetMountRevision()));
            ToProto(req.mutable_global_patch(), globalPatchYson);
            req.set_experiments(experimentsYson);
            Slot_->CommitTabletMutation(req);
        };

        if (!tablet->IsActiveServant()) {
            return;
        }

        const auto& currentSettings = tablet->RawSettings();

        // Check for global config changes.
        if (!static_cast<TTableConfigPatchPtr>(patch)->IsEqual(currentSettings.GlobalPatch)) {
            return scheduleUpdate();
        }

        // Check for changes in experiments.

        // NB: Fixed-order container is crucial for simultaneous traversal.
        static_assert(std::is_same_v<
            decltype(currentSettings.Experiments),
            std::map<std::string, TTableConfigExperimentPtr>>);

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
            .With("tablet_id", tablet->GetId())
            .With(configErrors);
        tablet->RuntimeData()->Errors.ConfigError.Store(error);
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

    static void ValidateTrimmedRowCountPrecedesReplication(const TTablet* tablet, i64 trimmedRowCount)
    {
        const auto& storeRowIndexMap = tablet->StoreRowIndexMap();
        // Fast path: skip replicationTimestamp calculation for empty tablet.
        if (storeRowIndexMap.empty()) {
            // No stores.
            return;
        }

        auto replicationTimestamp = tablet->GetOrderedChaosReplicationMinTimestamp();
        ValidateTrimmedRowCountPrecedesTimestamp(tablet, trimmedRowCount, replicationTimestamp);
    }

    void ValidatePreparingTransactionIsProperlyExternalized(
        TTablet* tablet,
        TTransaction* transaction,
        TStringBuf actionKind) const
    {
        if (!tablet->IsActiveServant() && !transaction->IsExternalizedToThisCell()) {
            THROW_ERROR_EXCEPTION("Cannot prepare %v at the non-active servant "
                "with non-externalized transaction %v, transaction may be stale",
                actionKind,
                transaction->GetId())
                .With("tablet_id", tablet->GetId());
        }

        if (tablet->IsActiveServant() &&
            tablet->SmoothMovementData().ShouldForwardMutation() &&
            !transaction->IsExternalizedFromThisCell())
        {
            THROW_ERROR_EXCEPTION("Cannot prepare %v at the active servant because "
                "transaction %v is not externalized but must be forwarded, "
                "transaction may be stale",
                actionKind,
                transaction->GetId())
                .With("tablet_id", tablet->GetId());
        }
    }

    THashMap<std::string, TClusterReplicationStatus> GetPerClusterReplicationStatus() const
    {
        THashMap<std::string, TClusterReplicationStatus> replicationClusters;

        for (const auto& [_, tablet] : Tablets()) {
            for (auto& [_, replicaInfo] : tablet->Replicas()) {
                auto replicaClusterName = replicaInfo.GetClusterName();
                auto& replicaReplicationStatus = replicationClusters[replicaClusterName];

                if (replicaInfo.GetPreparedReplicationTransactionId()) {
                    replicaReplicationStatus.PreparedReplicatorTransactionCount++;
                }

                if (auto state = replicaInfo.GetState(); state == ETableReplicaState::Enabled || state == ETableReplicaState::Enabling) {
                    switch (replicaInfo.GetMode()) {
                        case ETableReplicaMode::Sync: {
                            ++replicaReplicationStatus.SyncReplicaCount;
                            break;
                        }
                        case ETableReplicaMode::AsyncToSync: {
                            ++replicaReplicationStatus.AsyncToSyncReplicaCount;
                            break;
                        }
                        case ETableReplicaMode::SyncToAsync: {
                            ++replicaReplicationStatus.SyncToAsyncReplicaCount;
                            break;
                        }
                        default:
                            break;
                    }
                }

                auto replicator = replicaInfo.GetReplicator();
                if (replicator && replicator->HasActiveReplicationIteration()) {
                    replicaReplicationStatus.ActiveReplicatorIterationCount++;
                }
            }
        }

        return replicationClusters;
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
