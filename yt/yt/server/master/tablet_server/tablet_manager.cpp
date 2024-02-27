#include "tablet_manager.h"

#include "backup_manager.h"
#include "balancing_helpers.h"
#include "chaos_helpers.h"
#include "config.h"
#include "helpers.h"
#include "hunk_storage_node.h"
#include "hunk_tablet.h"
#include "hunk_tablet_type_handler.h"
#include "private.h"
#include "table_replica.h"
#include "table_replica_type_handler.h"
#include "table_settings.h"
#include "tablet.h"
#include "tablet_action.h"
#include "tablet_action_manager.h"
#include "tablet_action_type_handler.h"
#include "tablet_balancer.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_type_handler.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_cell_type_handler.h"
#include "tablet_chunk_manager.h"
#include "tablet_node_tracker.h"
#include "tablet_resources.h"
#include "tablet_service.h"
#include "tablet_type_handler.h"
#include "replicated_table_tracker.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_view.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/dynamic_store.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/lib/hive/avenue_directory.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>
#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/subject.h>

#include <yt/yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>
#include <yt/yt/server/master/table_server/replicated_table_node.h>
#include <yt/yt/server/master/table_server/table_collocation.h>
#include <yt/yt/server/master/table_server/table_manager.h>
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>
#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/backup.h>
#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/tree_builder.h>

#include <algorithm>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NCellarClient;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NChaosClient;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer::NProto;
using namespace NNodeTrackerServer;
using namespace NObjectClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NSecurityServer;
using namespace NTableClient::NProto;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletNodeTrackerClient::NProto;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NTabletNode::EStoreType;
using NTabletNode::TBuiltinTableMountConfigPtr;
using NTabletNode::TCustomTableMountConfigPtr;
using NTabletNode::TTableMountConfigPtr;
using NTabletNode::TTableConfigPatchPtr;
using NTabletNode::DynamicStoreIdPoolSize;

using NTransactionServer::TTransaction;

using NSecurityServer::ConvertToClusterResources;

using NYT::FromProto;
using NYT::ToProto;

using TTabletResources = NTabletServer::TTabletResources;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TabletManager)
        , TabletService_(New<TTabletService>(Bootstrap_))
        , TabletBalancer_(New<TTabletBalancer>(Bootstrap_))
        , TabletCellDecommissioner_(New<TTabletCellDecommissioner>(Bootstrap_))
        , TabletActionManager_(New<TTabletActionManager>(Bootstrap_))
        , TabletChunkManager_(CreateTabletChunkManager(Bootstrap_))
        , TabletMap_(TEntityMapTypeTraits<TTabletBase>(Bootstrap_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        DefaultTabletCellBundleId_ = MakeWellKnownId(EObjectType::TabletCellBundle, primaryCellTag, 0xffffffffffffffff);
        SequoiaTabletCellBundleId_ = MakeWellKnownId(EObjectType::TabletCellBundle, primaryCellTag, 0xfffffffffffffffe);

        RegisterMethod(BIND(&TImpl::HydraOnTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnmounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletFrozen, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnfrozen, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTableReplicaStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTableReplicaEnabled, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTableReplicaDisabled, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletTrimmedRowCount, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletLocked, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCreateTabletAction, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDestroyTabletActions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraKickOrphanedTabletActions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTabletCellStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateUpstreamTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAllocateDynamicStore, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTabletCellBundleResourceUsage, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletCellBundleResourceUsage, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnHunkTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnHunkTabletUnmounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSwitchServant, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDeallocateServant, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraReportSmoothMovementProgress, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraReportSmoothMovementAborted, Unretained(this)));

        const auto& tabletNodeTracker = Bootstrap_->GetTabletNodeTracker();
        tabletNodeTracker->SubscribeHeartbeat(BIND(&TImpl::OnTabletNodeHeartbeat, MakeWeak(this)));
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateTabletCellBundleTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateTabletCellTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateTabletTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateHunkTabletTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateTableReplicaTypeHandler(Bootstrap_, &TableReplicaMap_));
        objectManager->RegisterHandler(CreateTabletActionTypeHandler(Bootstrap_, &TabletActionMap_));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(&TImpl::OnTransactionAborted, MakeWeak(this)));

        transactionManager->RegisterTransactionActionHandlers<NProto::TReqUpdateTabletStores>({
            .Prepare = BIND_NO_PROPAGATE(&TImpl::HydraPrepareUpdateTabletStores, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TImpl::HydraCommitUpdateTabletStores, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TImpl::HydraAbortUpdateTabletStores, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<NProto::TReqUpdateHunkTabletStores>({
            .Prepare = BIND_NO_PROPAGATE(&TImpl::HydraPrepareUpdateHunkTabletStores, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TImpl::HydraCommitUpdateHunkTabletStores, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TImpl::HydraAbortUpdateHunkTabletStores, Unretained(this)),
        });

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeAfterSnapshotLoaded(BIND_NO_PROPAGATE(&TImpl::OnAfterCellManagerSnapshotLoaded, MakeWeak(this)));
        cellManager->SubscribeCellBundleDestroyed(BIND_NO_PROPAGATE(&TImpl::OnTabletCellBundleDestroyed, MakeWeak(this)));
        cellManager->SubscribeCellDecommissionStarted(BIND_NO_PROPAGATE(&TImpl::OnTabletCellDecommissionStarted, MakeWeak(this)));

        TabletService_->Initialize();
    }

    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromMethod(&TImpl::BuildOrchidYson, MakeWeak(this))
            ->Via(Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::TabletManager));
    }

    const ITabletChunkManagerPtr& GetTabletChunkManager() const
    {
        return TabletChunkManager_;
    }

    void OnTabletCellBundleDestroyed(TCellBundle* cellBundle)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (cellBundle->GetType() != EObjectType::TabletCellBundle) {
            return;
        }

        auto* tabletCellBundle = cellBundle->As<TTabletCellBundle>();

        // Unbind tablet actions associated with the bundle.
        for (auto* action : tabletCellBundle->TabletActions()) {
            action->SetTabletCellBundle(nullptr);
        }

        BundleIdToProfilingCounters_.erase(tabletCellBundle->GetId());
    }

    TTabletBase* GetTabletOrThrow(TTabletId id)
    {
        auto* tablet = FindTablet(id);
        if (!IsObjectAlive(tablet)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No tablet %v",
                id);
        }
        return tablet;
    }

    TTabletBase* CreateTablet(TTabletOwnerBase* table, EObjectType type)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());
        YT_VERIFY(IsTabletType(type));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(type);

        std::unique_ptr<TTabletBase> tabletHolder;
        switch (type) {
            case EObjectType::Tablet:
                tabletHolder = TPoolAllocator::New<TTablet>(id);
                break;
            case EObjectType::HunkTablet:
                tabletHolder = TPoolAllocator::New<THunkTablet>(id);
                break;
            default:
                YT_ABORT();
        }

        tabletHolder->SetOwner(table);

        auto* tablet = TabletMap_.Insert(id, std::move(tabletHolder));
        objectManager->RefObject(tablet);

        YT_LOG_DEBUG(
            "Tablet created (TableId: %v, TabletId: %v, Type: %lv, Account: %v)",
            table->GetId(),
            tablet->GetId(),
            type,
            table->Account()->GetName());

        return tablet;
    }

    void DestroyTablet(TTabletBase* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // XXX(savrus): this is a workaround for YTINCIDENTS-42
        if (auto* cell = tablet->GetCell()) {
            YT_LOG_ALERT("Destroying tablet with non-null tablet cell (TabletId: %v, CellId: %v)",
                tablet->GetId(),
                cell->GetId());

            // Replicated table not supported.
            if (tablet->GetType() == EObjectType::Tablet) {
                YT_VERIFY(tablet->As<TTablet>()->Replicas().empty());
            }

            if (cell->Tablets().erase(tablet) > 0) {
                YT_LOG_ALERT("Unbinding tablet from tablet cell since tablet is destroyed (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    cell->GetId());
            }

            if (tablet->GetState() == ETabletState::Mounted) {
                YT_LOG_ALERT("Sending force unmount request to node since tablet is destroyed (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    cell->GetId());

                TReqUnmountTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                request.set_force(true);

                const auto& hiveManager = Bootstrap_->GetHiveManager();
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }

        YT_VERIFY(!tablet->GetOwner());

        if (auto* action = tablet->GetAction()) {
            OnTabletActionTabletsTouched(
                action,
                THashSet<TTabletBase*>{tablet},
                TError("Tablet %v has been removed", tablet->GetId()));
        }

        if (tablet->GetType() == EObjectType::Tablet) {
            auto dynamicStoreCount = tablet->As<TTablet>()->DynamicStores().size();
            if (dynamicStoreCount > 0) {
                YT_LOG_ALERT(
                    "Tablet has dynamic stores upon destruction "
                    "(TabletId: %v, StoreCount: %v)",
                    tablet->GetId(),
                    dynamicStoreCount);
            }
        }

        Y_UNUSED(TabletMap_.Release(tablet->GetId()).release());
    }

    TTableReplica* CreateTableReplica(
        TReplicatedTableNode* table,
        const TString& clusterName,
        const TYPath& replicaPath,
        ETableReplicaMode mode,
        bool preserveTimestamps,
        EAtomicity atomicity,
        bool enabled,
        TTimestamp startReplicationTimestamp,
        const std::optional<std::vector<i64>>& startReplicationRowIndexes,
        bool enableReplicatedTableTracker)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto* replica : GetValuesSortedByKey(table->Replicas())) {
            if (replica->GetClusterName() == clusterName &&
                replica->GetReplicaPath() == replicaPath)
            {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::TableReplicaAlreadyExists,
                    "Replica table %v at cluster %Qv already exists",
                    replicaPath,
                    clusterName);
            }
        }

        if (!preserveTimestamps && atomicity == EAtomicity::None) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::InvalidTabletState,
                "Cannot create replica table: incompatible atomicity and preserve_timestamps")
                << TErrorAttribute("\"atomicity\"", atomicity)
                << TErrorAttribute("\"preserve_timestamps\"", preserveTimestamps);
        }

        YT_VERIFY(!startReplicationRowIndexes || startReplicationRowIndexes->size() == table->Tablets().size());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TableReplica);
        auto replicaHolder = TPoolAllocator::New<TTableReplica>(id);
        replicaHolder->SetTable(table);
        replicaHolder->SetClusterName(clusterName);
        replicaHolder->SetReplicaPath(replicaPath);
        replicaHolder->SetMode(mode);
        replicaHolder->SetPreserveTimestamps(preserveTimestamps);
        replicaHolder->SetAtomicity(atomicity);
        replicaHolder->SetStartReplicationTimestamp(startReplicationTimestamp);
        auto state = enabled
            ? ETableReplicaState::Enabled
            : ETableReplicaState::Disabled;
        replicaHolder->SetState(state);
        replicaHolder->SetEnableReplicatedTableTracker(enableReplicatedTableTracker);

        auto* replica = TableReplicaMap_.Insert(id, std::move(replicaHolder));
        objectManager->RefObject(replica);

        InsertOrCrash(table->Replicas(), replica);

        YT_LOG_DEBUG(
            "Table replica created (TableId: %v, ReplicaId: %v, Mode: %v, StartReplicationTimestamp: %v)",
            table->GetId(),
            replica->GetId(),
            mode,
            startReplicationTimestamp);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (int tabletIndex = 0; tabletIndex < std::ssize(table->Tablets()); ++tabletIndex) {
            auto* tablet = table->Tablets()[tabletIndex]->As<TTablet>();
            auto pair = tablet->Replicas().emplace(replica, TTableReplicaInfo());
            YT_VERIFY(pair.second);
            auto& replicaInfo = pair.first->second;

            if (startReplicationRowIndexes) {
                replicaInfo.SetCommittedReplicationRowIndex((*startReplicationRowIndexes)[tabletIndex]);
            }

            if (!tablet->IsActive()) {
                replicaInfo.SetState(ETableReplicaState::None);
                continue;
            }

            replicaInfo.SetState(ETableReplicaState::Disabled);

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            TReqAddTableReplica req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            PopulateTableReplicaDescriptor(req.mutable_replica(), replica, replicaInfo);
            hiveManager->PostMessage(mailbox, req);
        }

        ReplicaCreated_.Fire(TReplicaData{
            .TableId = table->GetId(),
            .Id = id,
            .Mode = mode,
            .Enabled = state == ETableReplicaState::Enabled,
            .ClusterName = clusterName,
            .TablePath = replicaPath,
            .TrackingEnabled = replica->GetEnableReplicatedTableTracker(),
            .ContentType = ETableReplicaContentType::Data,
        });

        return replica;
    }

    void ZombifyTableReplica(TTableReplica* replica)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto* table = replica->GetTable()) {
            EraseOrCrash(table->Replicas(), replica);

            const auto& hiveManager = Bootstrap_->GetHiveManager();
            for (auto* tablet : table->Tablets()) {
                EraseOrCrash(tablet->As<TTablet>()->Replicas(), replica);

                if (!tablet->IsActive()) {
                    continue;
                }

                auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
                TReqRemoveTableReplica req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                ToProto(req.mutable_replica_id(), replica->GetId());
                hiveManager->PostMessage(mailbox, req);
            }

            replica->SetTable(nullptr);
        }

        replica->TransitioningTablets().clear();

        ReplicaDestroyed_.Fire(replica->GetId());
    }

    void AlterTableReplica(
        TTableReplica* replica,
        std::optional<bool> enabled,
        std::optional<ETableReplicaMode> mode,
        std::optional<EAtomicity> atomicity,
        std::optional<bool> preserveTimestamps,
        std::optional<bool> enableReplicatedTableTracker)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
        }

        auto* table = replica->GetTable();
        auto state = replica->GetState();

        table->ValidateNotBackup("Cannot alter replica of a backup table");

        if (table->GetAggregatedTabletBackupState() != ETabletBackupState::None) {
            THROW_ERROR_EXCEPTION("Canont alter replica since its table is being backed up")
                << TErrorAttribute("table_id", table->GetId())
                << TErrorAttribute("tablet_backup_state", table->GetAggregatedTabletBackupState());
        }

        if (enabled) {
            if (*enabled) {
                switch (state) {
                    case ETableReplicaState::Enabled:
                    case ETableReplicaState::Enabling:
                        enabled = std::nullopt;
                        break;
                    case ETableReplicaState::Disabled:
                        break;
                    default:
                        replica->ThrowInvalidState();
                        break;
                }
            } else {
                switch (state) {
                    case ETableReplicaState::Disabled:
                    case ETableReplicaState::Disabling:
                        enabled = std::nullopt;
                        break;
                    case ETableReplicaState::Enabled:
                        break;
                    default:
                        replica->ThrowInvalidState();
                        break;
                }
            }

            for (auto* tablet : table->Tablets()) {
                if (tablet->GetState() == ETabletState::Unmounting) {
                    THROW_ERROR_EXCEPTION("Cannot alter \"enabled\" replica flag since tablet %v is in %Qlv state",
                        tablet->GetId(),
                        tablet->GetState());
                }
            }
        }

        if (!preserveTimestamps && atomicity == EAtomicity::None) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::InvalidTabletState,
                "Cannot set atomicity %v with preserveTimestamps %v",
                atomicity,
                preserveTimestamps);
        }

        if (mode && replica->GetMode() == *mode) {
            mode = std::nullopt;
        }

        if (atomicity && replica->GetAtomicity() == *atomicity) {
            atomicity = std::nullopt;
        }

        if (preserveTimestamps && replica->GetPreserveTimestamps() == *preserveTimestamps) {
            preserveTimestamps = std::nullopt;
        }

        YT_LOG_DEBUG("Table replica updated "
            "(TableId: %v, ReplicaId: %v, Enabled: %v, Mode: %v, "
            "Atomicity: %v, PreserveTimestamps: %v, EnableReplicatedTableTracker: %v)",
            table->GetId(),
            replica->GetId(),
            enabled,
            mode,
            atomicity,
            preserveTimestamps,
            enableReplicatedTableTracker);

        if (mode) {
            replica->SetMode(*mode);
            FireUponTableReplicaUpdate(replica);
        }

        if (atomicity) {
            replica->SetAtomicity(*atomicity);
        }

        if (preserveTimestamps) {
            replica->SetPreserveTimestamps(*preserveTimestamps);
        }

        if (enableReplicatedTableTracker) {
            replica->SetEnableReplicatedTableTracker(*enableReplicatedTableTracker);
        }

        if (enabled) {
            if (*enabled) {
                YT_LOG_DEBUG("Enabling table replica (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabling);
            } else {
                YT_LOG_DEBUG("Disabling table replica (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Disabling);
            }
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (auto* tabletBase : table->Tablets()) {
            if (!tabletBase->IsActive()) {
                continue;
            }

            auto* tablet = tabletBase->As<TTablet>();
            auto* replicaInfo = tablet->GetReplicaInfo(replica);

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            TReqAlterTableReplica req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            ToProto(req.mutable_replica_id(), replica->GetId());

            if (enabled) {
                std::optional<ETableReplicaState> newState;
                if (*enabled && replicaInfo->GetState() != ETableReplicaState::Enabled) {
                    newState = ETableReplicaState::Enabling;
                }
                if (!*enabled && replicaInfo->GetState() != ETableReplicaState::Disabled) {
                    newState = ETableReplicaState::Disabling;
                }
                if (newState) {
                    req.set_enabled(*newState == ETableReplicaState::Enabling);
                    StartReplicaTransition(tablet, replica, replicaInfo, *newState);
                }
            }

            if (mode) {
                req.set_mode(ToProto<int>(*mode));
            }
            if (atomicity) {
                req.set_atomicity(ToProto<int>(*atomicity));
            }
            if (preserveTimestamps) {
                req.set_preserve_timestamps(*preserveTimestamps);
            }

            hiveManager->PostMessage(mailbox, req);
        }

        if (enabled) {
            CheckTransitioningReplicaTablets(replica);
        }
    }


    TTabletAction* CreateTabletAction(
        TObjectId hintId,
        ETabletActionKind kind,
        const std::vector<TTabletBase*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime,
        std::optional<TDuration> expirationTimeout)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablets.empty()) {
            THROW_ERROR_EXCEPTION("Invalid number of tablets: expected more than zero");
        }

        if (tablets[0]->GetType() != EObjectType::Tablet) {
            THROW_ERROR_EXCEPTION("Tablet actions are not supported for tablets of type %Qlv",
                tablets[0]->GetType());
        }

        auto* table = tablets[0]->As<TTablet>()->GetTable();
        if (!IsObjectAlive(table)) {
            THROW_ERROR_EXCEPTION("Table is destroyed");
        }

        // Validate that table is not in process of mount/unmount/etc.
        table->ValidateNoCurrentMountTransaction("Cannot create tablet action");

        for (const auto* tablet : tablets) {
            if (tablet->GetOwner() != table) {
                THROW_ERROR_EXCEPTION("Tablets %v and %v belong to different tables",
                    tablets[0]->GetId(),
                    tablet->GetId());
            }
            if (auto* action = tablet->GetAction()) {
                THROW_ERROR_EXCEPTION("Tablet %v already participating in action %v",
                    tablet->GetId(),
                    action->GetId());
            }
            if (tablet->GetState() != ETabletState::Mounted && tablet->GetState() != ETabletState::Frozen) {
                THROW_ERROR_EXCEPTION("Tablet %v is in state %Qlv",
                    tablet->GetId(),
                    tablet->GetState());
            }
            if (tablet->GetTabletwiseAvenueEndpointId()) {
                THROW_ERROR_EXCEPTION("Tablet %v is recovering from smooth movement",
                    tablet->GetId());
            }
        }

        bool freeze;
        {
            auto state = tablets[0]->GetState();
            for (const auto* tablet : tablets) {
                if (tablet->GetState() != state) {
                    THROW_ERROR_EXCEPTION("Tablets are in mixed state");
                }
            }
            freeze = state == ETabletState::Frozen;
        }

        const auto& bundle = table->TabletCellBundle();

        for (auto* cell : cells) {
            if (!IsCellActive(cell)) {
                THROW_ERROR_EXCEPTION("Tablet cell %v is not active", cell->GetId());
            }

            if (cell->CellBundle() != bundle) {
                THROW_ERROR_EXCEPTION("%v %v and tablet cell %v belong to different bundles",
                    table->GetCapitalizedObjectName(),
                    table->GetId(),
                    cell->GetId());
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(bundle.Get(), EPermission::Use);

        switch (kind) {
            case ETabletActionKind::Move:
            case ETabletActionKind::SmoothMove:
                if (!cells.empty() && cells.size() != tablets.size()) {
                    THROW_ERROR_EXCEPTION("Number of destination cells and tablets mismatch: %v tablets, %v cells",
                        cells.size());
                }
                if (!pivotKeys.empty()) {
                    THROW_ERROR_EXCEPTION("Invalid number of pivot keys: expected 0, actual %v",
                        pivotKeys.size());
                }
                if (tabletCount) {
                    THROW_ERROR_EXCEPTION("Invalid number of tablets: expected std::nullopt, actual %v",
                        *tabletCount);
                }
                break;

            case ETabletActionKind::Reshard:
                if (pivotKeys.empty() && (!tabletCount || *tabletCount < 1)) {
                    THROW_ERROR_EXCEPTION("Invalid number of new tablets: expected pivot keys or tablet count greater than 1");
                }

                if (!cells.empty()) {
                    if (pivotKeys.empty()) {
                        if (ssize(cells) != *tabletCount) {
                            THROW_ERROR_EXCEPTION("Number of destination cells and tablet count mismatch: "
                                "tablet count %v, cells %v",
                                *tabletCount,
                                cells.size());
                        }
                    } else {
                        if (ssize(cells) != ssize(pivotKeys)) {
                            THROW_ERROR_EXCEPTION("Number of destination cells and pivot keys mismatch: pivot keys %v, cells %",
                                pivotKeys.size(),
                                cells.size());

                        }
                    }
                }

                for (int index = 1; index < std::ssize(tablets); ++index) {
                    const auto& cur = tablets[index];
                    const auto& prev = tablets[index - 1];
                    if (cur->GetIndex() != prev->GetIndex() + 1) {
                        THROW_ERROR_EXCEPTION("Tablets %v and %v are not consequent",
                            prev->GetId(),
                            cur->GetId());
                    }
                }
                break;

            default:
                YT_ABORT();
        }

        auto tableSettings = GetTableSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        ValidateTableMountConfig(table, tableSettings.EffectiveMountConfig, GetDynamicConfig());

        if (kind == ETabletActionKind::SmoothMove) {
            if (tablets.size() != 1) {
                THROW_ERROR_EXCEPTION("Only one tablet can be moved at a time");
            }

            if (cells.size() != 1) {
                THROW_ERROR_EXCEPTION("Destination cell must be specified");
            }

            const auto* tablet = tablets[0];

            if (tablet->GetCell() == cells[0]) {
                THROW_ERROR_EXCEPTION("Tablet already belongs to cell %v",
                    tablet->GetCell()->GetId());
            }

            if (tablet->GetState() != ETabletState::Mounted) {
                THROW_ERROR_EXCEPTION("Only mounted tablet can be moved");
            }

            if (!tablet->IsMountedWithAvenue()) {
                THROW_ERROR_EXCEPTION("Tablet must be mounted with avenues");
            }

            const auto* table = tablet->GetOwner()->As<TTableNode>();

            if (table->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Replicated table tablet cannot be moved");
            }

            if (table->GetReplicationCardId()) {
                THROW_ERROR_EXCEPTION("Chaos table tablet cannot be moved");
            }

            if (!table->IsPhysicallySorted()) {
                THROW_ERROR_EXCEPTION("Ordered table tablet cannot be moved");
            }

            if (IsDynamicStoreReadEnabled(table, GetDynamicConfig())) {
                THROW_ERROR_EXCEPTION("Cannot move table with enabled dynamic store read");
            }

            if (table->GetAtomicity() != EAtomicity::Full) {
                THROW_ERROR_EXCEPTION("Tablet with atomicity %Qlv cannot be moved",
                    table->GetAtomicity());
            }

            const auto& schema = table->GetSchema()->AsTableSchema();
            if (schema->HasHunkColumns()) {
                THROW_ERROR_EXCEPTION("Cannot move table with hunk columns");
            }
        }

        auto* action = DoCreateTabletAction(
            hintId,
            kind,
            ETabletActionState::Preparing,
            tablets,
            cells,
            pivotKeys,
            tabletCount,
            freeze,
            skipFreezing,
            correlationId,
            expirationTime,
            expirationTimeout);

        OnTabletActionStateChanged(action);
        return action;
    }

    void DestroyTabletAction(TTabletAction* action)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        UnbindTabletAction(action);
        if (auto* bundle = action->GetTabletCellBundle()) {
            bundle->TabletActions().erase(action);
            if (!action->IsFinished()) {
                bundle->DecreaseActiveTabletActionCount();
            }
        }

        YT_LOG_DEBUG("Tablet action destroyed (ActionId: %v, TabletBalancerCorrelationId: %v)",
            action->GetId(),
            action->GetCorrelationId());
    }

    void PrepareMount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateMount();

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        auto validateCellBundle = [table] (const TTabletCell* cell) {
            if (cell->CellBundle() != table->TabletCellBundle()) {
                THROW_ERROR_EXCEPTION("Cannot mount tablets into cell %v since it belongs to bundle %Qv while the %v "
                    "is configured to use bundle %Qv",
                    cell->GetId(),
                    cell->CellBundle()->GetName(),
                    table->GetLowercaseObjectName(),
                    table->TabletCellBundle()->GetName());
            }
        };

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (hintCellId || !targetCellIds.empty()) {
            if (hintCellId && !targetCellIds.empty()) {
                THROW_ERROR_EXCEPTION("At most one of \"cell_id\" and \"target_cell_ids\" must be specified");
            }

            if (hintCellId) {
                auto hintCell = GetTabletCellOrThrow(hintCellId);
                validateCellBundle(hintCell);
            } else {
                int tabletCount = lastTabletIndex - firstTabletIndex + 1;
                if (!targetCellIds.empty() && std::ssize(targetCellIds) != tabletCount) {
                    THROW_ERROR_EXCEPTION("\"target_cell_ids\" must either be empty or contain exactly "
                        "\"last_tablet_index\" - \"first_tablet_index\" + 1 entries (%v != %v - %v + 1)",
                        targetCellIds.size(),
                        lastTabletIndex,
                        firstTabletIndex);
                }

                for (auto cellId : targetCellIds) {
                    auto targetCell = GetTabletCellOrThrow(cellId);
                    if (!IsCellActive(targetCell)) {
                        THROW_ERROR_EXCEPTION("Cannot mount tablet into cell %v since it is not active",
                            cellId);
                    }
                    validateCellBundle(targetCell);
                }
            }
        } else {
            ValidateHasHealthyCells(table->TabletCellBundle().Get()); // may throw
        }

        const auto& allTablets = table->Tablets();

        auto maxChunkCount = GetDynamicConfig()->MaxChunksPerMountedTablet;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tabletBase = allTablets[index];
            tabletBase->ValidateMount(freeze);

            if (tabletBase->GetType() == EObjectType::Tablet) {
                auto* tablet = tabletBase->As<TTablet>();
                auto* chunkList = tablet->GetChunkList();
                auto chunkCount = chunkList->Statistics().ChunkCount;

                if (chunkCount > maxChunkCount) {
                    THROW_ERROR_EXCEPTION("Cannot mount tablet %v since it has too many chunks",
                        tablet->GetId())
                        << TErrorAttribute("chunk_count", chunkCount)
                        << TErrorAttribute("max_chunks_per_mounted_tablet", maxChunkCount);
                }
            }
        }

        if (IsTableType(table->GetType())) {
            PrepareMountTable(table->As<TTableNode>());
        } else if (table->GetType() == EObjectType::HunkStorage) {
            PrepareMountHunkStorage(table->As<THunkStorageNode>());
        }

        ValidateTabletStaticMemoryUpdate(
            table,
            firstTabletIndex,
            lastTabletIndex);

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "mount_table");
    }

    void PrepareMountTable(TTableNode* table)
    {
        auto tableSettings = GetTableSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        ValidateTableMountConfig(table, tableSettings.EffectiveMountConfig, GetDynamicConfig());

        if (table->GetReplicationCardId() && !table->IsSorted()) {
            if (table->GetCommitOrdering() != ECommitOrdering::Strong) {
                THROW_ERROR_EXCEPTION("Ordered dynamic table bound for chaos replication should have %Qlv commit ordering",
                    ECommitOrdering::Strong);
            }

            if (!table->GetSchema()->AsTableSchema()->FindColumn(TimestampColumnName)) {
                THROW_ERROR_EXCEPTION("Ordered dynamic table bound for chaos replication should have %Qlv column",
                    TimestampColumnName);
            }
        }

        for (const auto& column : table->GetSchema()->AsTableSchema()->Columns()) {
            if (column.GetWireType() == EValueType::Null) {
                THROW_ERROR_EXCEPTION("Cannot mount table since it has column %Qv with value type %Qlv",
                    column.Name(),
                    EValueType::Null);
            }
        }

        if (auto backupState = table->GetAggregatedTabletBackupState();
            backupState != ETabletBackupState::None)
        {
            THROW_ERROR_EXCEPTION("Cannot mount table since it has invalid backup state %Qlv",
                backupState);
        }
    }

    void PrepareMountHunkStorage(THunkStorageNode* hunkStorage)
    {
        GetHunkStorageSettings(
            hunkStorage,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
    }

    void Mount(
        TTabletOwnerBase* table,
        const TString& path,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze,
        TTimestamp mountTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        TTabletCell* hintCell = nullptr;
        if (hintCellId) {
            hintCell = FindTabletCell(hintCellId);
        }

        table->SetMountPath(path);

        const auto& allTablets = table->Tablets();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;

        if (!targetCellIds.empty()) {
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = allTablets[index];
                if (!tablet->GetCell()) {
                    auto* cell = FindTabletCell(targetCellIds[index - firstTabletIndex]);
                    assignment.emplace_back(tablet, cell);
                }
            }
        } else {
            std::vector<TTabletBase*> tabletsToMount;
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = allTablets[index];
                if (!tablet->GetCell()) {
                    tabletsToMount.push_back(tablet);
                }
            }

            assignment = ComputeTabletAssignment(
                table,
                hintCell,
                std::move(tabletsToMount));
        }

        auto tableSettings = GetTabletOwnerSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        auto serializedTableSettings = SerializeTabletOwnerSettings(tableSettings);

        DoMountTablets(
            table,
            serializedTableSettings,
            assignment,
            freeze,
            mountTimestamp);

        UpdateTabletState(table);
    }

    void PrepareUnmount(
        TTabletOwnerBase* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateUnmount();

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (!force) {
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = table->Tablets()[index];
                tablet->ValidateUnmount();
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "unmount_table");
    }

    void Unmount(
        TTabletOwnerBase* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        DoUnmount(table, force, firstTabletIndex, lastTabletIndex, /*onDestroy*/ false);
        UpdateTabletState(table);
    }

    void PrepareRemount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateRemount();

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (IsTableType(table->GetType())) {
            PrepareRemountTable(table->As<TTableNode>(), firstTabletIndex, lastTabletIndex);
        }
    }

    void PrepareRemountTable(
        TTableNode* table,
        int /*firstTabletIndex*/,
        int lastTabletIndex)
    {
        if (!table->IsSorted() && table->GetReplicationCardId() && lastTabletIndex != std::ssize(table->Tablets()) - 1) {
            THROW_ERROR_EXCEPTION("Invalid last tablet index: expected %v, got %v",
                std::ssize(table->Tablets()) - 1,
                lastTabletIndex);
        }

        auto tableSettings = GetTableSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        ValidateTableMountConfig(table, tableSettings.EffectiveMountConfig, GetDynamicConfig());
    }

    void Remount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        auto resourceUsageBefore = table->GetTabletResourceUsage();

        DoRemount(table, firstTabletIndex, lastTabletIndex);

        if (resourceUsageBefore != table->GetTabletResourceUsage()) {
            YT_LOG_ALERT(
                "Tablet resource usage changed during table remount "
                "(TableId: %v, UsageBefore: %v, UsageAfter: %v)",
                table->GetId(),
                resourceUsageBefore,
                table->GetTabletResourceUsage());
        }
        UpdateResourceUsage(table, table->GetTabletResourceUsage() - resourceUsageBefore);
    }

    void PrepareFreeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateFreeze();

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            tablet->ValidateFreeze();
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "freeze_table");
    }

    void Freeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            DoFreezeTablet(tablet);
        }

        UpdateTabletState(table);
    }

    void PrepareUnfreeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateUnfreeze();

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            tablet->ValidateUnfreeze();
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "unfreeze_table");
    }

    void Unfreeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            DoUnfreezeTablet(tablet);
        }

        UpdateTabletState(table);
    }

    void PrepareReshard(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts,
        bool create = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        table->ValidateReshard(
            Bootstrap_,
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            pivotKeys,
            trimmedRowCounts);

        if (!create && !table->IsForeign()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (create) {
            int oldTabletCount = table->IsExternal() ? 0 : 1;
            ValidateResourceUsageIncrease(
                table,
                TTabletResources().SetTabletCount(newTabletCount - oldTabletCount));
        }

        if (table->IsExternal()) {
            return;
        }

        // Now check against tablets.
        // This is a job of secondary master in a two-phase commit.
        // Should not throw when table is created.

        const auto& tablets = table->Tablets();
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            if (auto* chunkList = table->GetChunkList(contentType)) {
                YT_VERIFY(tablets.size() == chunkList->Children().size());
            }
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;

        if (tablets.size() - oldTabletCount + newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
                MaxTabletCount);
        }

        ValidateResourceUsageIncrease(
            table,
            TTabletResources().SetTabletCount(newTabletCount - oldTabletCount));

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            tablet->ValidateReshard();
        }

        if (newTabletCount < oldTabletCount) {
            for (int index = firstTabletIndex + newTabletCount; index < firstTabletIndex + oldTabletCount; ++index) {
                auto* tablet = tablets[index];
                tablet->ValidateReshardRemove();
            }
        }

        // Do after all validations.
        if (IsTableType(table->GetType())) {
            TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "reshard_table");
        }
    }

    void Reshard(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts)
    {
        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        DoReshard(
            table,
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            pivotKeys,
            trimmedRowCounts);

        UpdateTabletState(table);
    }

    void DestroyTabletOwner(TTabletOwnerBase* table)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        if (!table->Tablets().empty()) {
            int firstTabletIndex = 0;
            int lastTabletIndex = static_cast<int>(table->Tablets().size()) - 1;

            TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "remove");

            DoUnmount(
                table,
                /*force*/ true,
                firstTabletIndex,
                lastTabletIndex,
                /*onDestroy*/ true);

            for (auto* tablet : table->Tablets()) {
                tablet->SetOwner(nullptr);
                YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);
                objectManager->UnrefObject(tablet);
            }

            YT_VERIFY(!table->IsExternal());

            const auto& bundle = table->TabletCellBundle();
            bundle->UpdateResourceUsage(-table->GetTabletResourceUsage());

            table->MutableTablets().clear();

            // NB: security manager has already been informed when node's account was reset.
        }

        if (table->GetType() == EObjectType::ReplicatedTable) {
            auto* replicatedTable = table->As<TReplicatedTableNode>();
            for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                objectManager->UnrefObject(replica);
            }
            YT_VERIFY(replicatedTable->Replicas().empty());
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        if (IsTableType(table->GetType())) {
            auto* typedTable = table->As<TTableNode>();
            for (auto [transactionId, lock] : typedTable->DynamicTableLocks()) {
                auto* transaction = transactionManager->FindTransaction(transactionId);
                if (!IsObjectAlive(transaction)) {
                    continue;
                }

                transaction->LockedDynamicTables().erase(typedTable);
            }

            if (auto* replicationCollocation = typedTable->GetReplicationCollocation()) {
                YT_VERIFY(table->GetType() == EObjectType::ReplicatedTable);
                const auto& tableManager = Bootstrap_->GetTableManager();
                tableManager->RemoveTableFromCollocation(typedTable, replicationCollocation);
            }
        }
    }

    void MergeTable(TTableNode* originatingNode, TTableNode* branchedNode)
    {
        YT_VERIFY(originatingNode->IsTrunk());

        auto updateMode = branchedNode->GetUpdateMode();
        if (updateMode == EUpdateMode::Append) {
            TabletChunkManager_->CopyChunkListsIfShared(originatingNode, 0, originatingNode->Tablets().size() - 1);
        }

        TChunkLists originatingChunkLists;
        TChunkLists branchedChunkLists;
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            originatingChunkLists[contentType] = originatingNode->GetChunkList(contentType);
            branchedChunkLists[contentType] = branchedNode->GetChunkList(contentType);
        }

        auto* transaction = branchedNode->GetTransaction();

        YT_VERIFY(originatingNode->IsPhysicallySorted());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        transaction->LockedDynamicTables().erase(originatingNode);

        i64 totalMemorySizeDelta = 0;

        // Deaccumulate old tablet statistics.
        for (int index = 0; index < std::ssize(originatingNode->Tablets()); ++index) {
            auto* tablet = originatingNode->Tablets()[index];

            auto tabletStatistics = tablet->GetTabletStatistics();
            originatingNode->DiscountTabletStatistics(tabletStatistics);

            if (tablet->GetState() != ETabletState::Unmounted) {
                totalMemorySizeDelta -= tablet->GetTabletStaticMemorySize();

                auto* cell = tablet->GetCell();
                cell->GossipStatistics().Local() -= tabletStatistics;
            }
        }

        // Replace root chunk list.
        if (updateMode == EUpdateMode::Overwrite) {
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                originatingChunkLists[contentType]->RemoveOwningNode(originatingNode);
                branchedChunkLists[contentType]->AddOwningNode(originatingNode);
                originatingNode->SetChunkList(contentType, branchedChunkLists[contentType]);
            }
        }

        // Merge tablet chunk lists and accumulate new tablet statistics.
        for (int index = 0; index < std::ssize(branchedChunkLists[EChunkListContentType::Main]->Children()); ++index) {
            auto* tablet = originatingNode->Tablets()[index]->As<TTablet>();
            if (updateMode == EUpdateMode::Overwrite) {
                AbandonDynamicStores(tablet);
            }

            std::vector<TChunkTree*> stores;
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                if (updateMode == EUpdateMode::Append && contentType == EChunkListContentType::Hunk) {
                    continue;
                }

                auto* appendChunkList = branchedChunkLists[contentType]->Children()[index]->AsChunkList();
                auto* tabletChunkList = originatingChunkLists[contentType]->Children()[index]->AsChunkList();

                if (updateMode == EUpdateMode::Overwrite && contentType == EChunkListContentType::Main) {
                    YT_VERIFY(appendChunkList->GetKind() == EChunkListKind::SortedDynamicTablet);
                    appendChunkList->SetPivotKey(tabletChunkList->GetPivotKey());
                }

                if (updateMode == EUpdateMode::Append) {
                    if (!appendChunkList->Children().empty()) {
                        chunkManager->AttachToChunkList(tabletChunkList, {appendChunkList});
                    }
                }

                if (originatingNode->GetInMemoryMode() != EInMemoryMode::None &&
                    tablet->GetState() != ETabletState::Unmounted &&
                    contentType == EChunkListContentType::Main)
                {
                    auto& nodeStatistics = tablet->NodeStatistics();
                    nodeStatistics.set_preload_pending_store_count(
                        nodeStatistics.preload_pending_store_count() +
                        ssize(appendChunkList->Children()));
                }

                if (tablet->GetState() != ETabletState::Unmounted &&
                    contentType == EChunkListContentType::Main)
                {
                    EnumerateStoresInChunkTree(appendChunkList, &stores);
                }
            }

            auto newStatistics = tablet->GetTabletStatistics();
            originatingNode->AccountTabletStatistics(newStatistics);

            if (tablet->GetState() == ETabletState::Unmounted) {
                continue;
            }

            auto newMemorySize = tablet->GetTabletStaticMemorySize();

            totalMemorySizeDelta += newMemorySize;

            if (updateMode == EUpdateMode::Overwrite) {
                tablet->SetStoresUpdatePreparedTransaction(nullptr);
            }

            auto* cell = tablet->GetCell();
            cell->GossipStatistics().Local() += newStatistics;

            TReqUnlockTablet req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            ToProto(req.mutable_transaction_id(), transaction->GetId());
            req.set_commit_timestamp(static_cast<i64>(
                transactionManager->GetTimestampHolderTimestamp(transaction->GetId())));
            req.set_update_mode(ToProto<int>(updateMode));

            i64 startingRowIndex = 0;
            for (const auto* store : stores) {
                auto* descriptor = req.add_stores_to_add();
                FillStoreDescriptor(originatingNode, store, descriptor, &startingRowIndex);
            }

            if (updateMode == EUpdateMode::Overwrite &&
                tablet->GetState() == ETabletState::Mounted &&
                IsDynamicStoreReadEnabled(originatingNode, GetDynamicConfig()))
            {
                CreateAndAttachDynamicStores(tablet, &req);
            }

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            hiveManager->PostMessage(mailbox, req);
        }

        // The rest of TChunkOwner::DoMerge later unconditionally replaces statistics of
        // originating node with the ones of branched node. Since dynamic stores are already
        // attached, we have to account them this way.
        branchedNode->SnapshotStatistics() = {};
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            branchedNode->SnapshotStatistics() += originatingNode->GetChunkList(contentType)->Statistics().ToDataStatistics();
        }

        auto resourceUsageDelta = TTabletResources()
            .SetTabletStaticMemory(totalMemorySizeDelta);

        UpdateResourceUsage(originatingNode, resourceUsageDelta);

        originatingNode->RemoveDynamicTableLock(transaction->GetId());

        if (updateMode == EUpdateMode::Append) {
            chunkManager->ClearChunkList(branchedChunkLists[EChunkListContentType::Main]);
        }
    }

    TGuid GenerateTabletBalancerCorrelationId() const
    {
        auto* mutationContext = GetCurrentMutationContext();
        const auto& generator = mutationContext->RandomGenerator();
        ui64 lo = generator->Generate<ui64>();
        ui64 hi = generator->Generate<ui64>();
        return TGuid(lo, hi);
    }

    TTabletActionId SpawnTabletAction(const TReshardDescriptor& descriptor)
    {
        std::vector<TTabletId> tabletIds;
        for (const auto& tablet : descriptor.Tablets) {
            tabletIds.push_back(tablet->GetId());
        }

        auto* tablet = descriptor.Tablets[0];
        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        auto* table = descriptor.Tablets[0]->As<TTablet>()->GetTable();

        auto correlationId = GenerateTabletBalancerCorrelationId();

        YT_LOG_DEBUG("Automatically resharding tablets "
            "(TableId: %v, TabletIds: %v, NewTabletCount: %v, TotalSize: %v, Bundle: %v, "
            "TabletBalancerCorrelationId: %v, Sync: true)",
            table->GetId(),
            tabletIds,
            descriptor.TabletCount,
            descriptor.DataSize,
            table->TabletCellBundle()->GetName(),
            correlationId);

        const auto& tablets = descriptor.Tablets;
        try {
            auto* action = CreateTabletAction(
                TObjectId{},
                ETabletActionKind::Reshard,
                std::vector<TTabletBase*>(tablets.begin(), tablets.end()),
                /*cells*/ {},
                /*pivotKeys*/ {},
                descriptor.TabletCount,
                /*skipFreezing*/ false,
                correlationId,
                TInstant::Zero(),
                /*expirationTimeout*/ std::nullopt);
            return action->GetId();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to create tablet action during sync reshard (TabletBalancerCorrelationId: %v)",
                correlationId);
            return NullObjectId;
        }
    }

    TTabletActionId SpawnTabletAction(const TTabletMoveDescriptor& descriptor)
    {
        auto* tablet = descriptor.Tablet;
        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        auto* table = tablet->GetTable();

        auto correlationId = GenerateTabletBalancerCorrelationId();

        YT_LOG_DEBUG("Moving tablet during cell balancing "
            "(TableId: %v, InMemoryMode: %v, TabletId: %v, SrcCellId: %v, DstCellId: %v, "
            "Bundle: %v, TabletBalancerCorrelationId: %v, Sync: true)",
            table->GetId(),
            table->GetInMemoryMode(),
            descriptor.Tablet->GetId(),
            descriptor.Tablet->GetCell()->GetId(),
            descriptor.TabletCellId,
            table->TabletCellBundle()->GetName(),
            correlationId);

        try {
            auto* action = CreateTabletAction(
                TObjectId{},
                ETabletActionKind::Move,
                {tablet},
                {GetTabletCellOrThrow(descriptor.TabletCellId)},
                /*pivotKeys*/ {},
                /*tabletCount*/ std::nullopt,
                /*skipFreezing*/ false,
                correlationId,
                TInstant::Zero(),
                /*expirationTimeout*/ std::nullopt);
            return action->GetId();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG("Failed to create tablet action during sync cells balancing (TabletBalancerCorrelationId: %v)",
                correlationId);
            return NullObjectId;
        }
    }

    std::vector<TTabletActionId> SyncBalanceCells(
        TTabletCellBundle* bundle,
        const std::optional<std::vector<TTableNode*>>& tables,
        bool keepActions)
    {
        if (bundle->GetActiveTabletActionCount() > 0) {
            THROW_ERROR_EXCEPTION("Bundle is already being balanced, try again later");
        }

        std::optional<THashSet<const TTableNode*>> tablesSet;
        if (tables) {
            tablesSet = THashSet<const TTableNode*>(tables->begin(), tables->end());
        }

        auto descriptors = ReassignInMemoryTablets(
            bundle,
            tablesSet,
            /*ignoreConfig*/ true);

        std::vector<TTabletActionId> actionIds;
        actionIds.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (auto actionId = SpawnTabletAction(descriptor)) {
                actionIds.push_back(actionId);
            }
        }

        if (keepActions) {
            SetSyncTabletActionsKeepalive(actionIds);
        }

        return actionIds;
    }

    std::vector<TTabletActionId> SyncBalanceTablets(
        TTableNode* table,
        bool keepActions)
    {
        ValidateSyncBalanceTablets(table);

        for (const auto& tablet : table->Tablets()) {
            if (tablet->GetAction()) {
                THROW_ERROR_EXCEPTION("Table is already being balanced, try again later")
                    << TErrorAttribute("tablet_id", tablet->GetId());
            }
        }

        std::vector<TTablet*> tablets;
        tablets.reserve(table->Tablets().size());
        for (auto* tablet : table->Tablets()) {
            tablets.push_back(tablet->As<TTablet>());
        }

        TTabletBalancerContext context;
        auto descriptors = MergeSplitTabletsOfTable(
            tablets,
            &context);

        std::vector<TTabletActionId> actionIds;
        actionIds.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (auto actionId = SpawnTabletAction(descriptor)) {
                actionIds.push_back(actionId);
            }
        }

        if (keepActions) {
            SetSyncTabletActionsKeepalive(actionIds);
        }

        return actionIds;
    }

    void ValidateSyncBalanceTablets(TTableNode* table)
    {
        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        if (table->IsPhysicallyLog()) {
            THROW_ERROR_EXCEPTION("Cannot automatically reshard table of type %Qlv",
                table->GetType());
        }
    }

    void ValidateCloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        ENodeCloneMode mode,
        TAccount* account)
    {
        if (sourceNode->IsForeign()) {
            return;
        }

        auto* trunkSourceNode = sourceNode->GetTrunkNode();

        ValidateNodeCloneMode(trunkSourceNode, mode);

        if (const auto& cellBundle = trunkSourceNode->TabletCellBundle()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle.Get());
        }

        ValidateResourceUsageIncrease(
            trunkSourceNode,
            TTabletResources().SetTabletCount(
                trunkSourceNode->GetTabletResourceUsage().TabletCount),
            account);
    }

    void ValidateBeginCopyTabletOwner(
        TTabletOwnerBase* sourceNode,
        ENodeCloneMode mode)
    {
        YT_VERIFY(sourceNode->IsNative());

        auto* trunkSourceNode = sourceNode->GetTrunkNode();
        if (const auto& cellBundle = trunkSourceNode->TabletCellBundle()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle.Get());
        }

        if (IsTableType(sourceNode->GetType())) {
            ValidateBeginCopyTable(sourceNode->As<TTableNode>(), mode);
        } else {
            YT_ABORT();
        }
    }

    void ValidateBeginCopyTable(
        TTableNode* sourceTable,
        ENodeCloneMode mode)
    {
        auto* trunkSourceTable = sourceTable->GetTrunkNode();
        ValidateNodeCloneMode(trunkSourceTable, mode);
    }

    void CloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        TTabletOwnerBase* clonedNode,
        ENodeCloneMode mode)
    {
        if (IsTableType(sourceNode->GetType())) {
            CloneTable(
                sourceNode->As<TTableNode>(),
                clonedNode->As<TTableNode>(),
                mode);
        } else {
            YT_ABORT();
        }
    }

    void CloneTable(
        TTableNode* sourceTable,
        TTableNode* clonedTable,
        ENodeCloneMode mode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(sourceTable->IsExternal() == clonedTable->IsExternal());

        auto* trunkSourceTable = sourceTable->GetTrunkNode();
        auto* trunkClonedTable = clonedTable; // sic!
        bool isBackupAction = mode == ENodeCloneMode::Backup || mode == ENodeCloneMode::Restore;

        SetTabletCellBundle(clonedTable, trunkSourceTable->TabletCellBundle().Get());

        if (!sourceTable->IsDynamic()) {
            return;
        }

        if (mode == ENodeCloneMode::Backup) {
            trunkClonedTable->SetBackupState(ETableBackupState::BackupCompleted);
        } else if (mode == ENodeCloneMode::Restore) {
            trunkClonedTable->SetBackupState(ETableBackupState::None);
        } else {
            trunkClonedTable->SetBackupState(trunkSourceTable->GetTrunkNode()->GetBackupState());
        }

        if (sourceTable->IsExternal()) {
            return;
        }

        YT_VERIFY(!trunkSourceTable->Tablets().empty());
        YT_VERIFY(trunkClonedTable->Tablets().empty());

        try {
            switch (mode) {
                case ENodeCloneMode::Copy:
                    sourceTable->ValidateAllTabletsFrozenOrUnmounted("Cannot copy dynamic table");
                    break;

                case ENodeCloneMode::Move:
                    sourceTable->ValidateAllTabletsUnmounted("Cannot move dynamic table");
                    break;

                case ENodeCloneMode::Backup:
                case ENodeCloneMode::Restore:
                    break;

                default:
                    YT_ABORT();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Error cloning table (TableId: %v, %v)",
                sourceTable->GetId(),
                NRpc::GetCurrentAuthenticationIdentity());
        }

        // Undo the harm done in TChunkOwnerTypeHandler::DoClone.
        auto fakeClonedRootChunkLists = trunkClonedTable->GetChunkLists();
        for (auto* fakeClonedRootChunkList : fakeClonedRootChunkLists) {
            fakeClonedRootChunkList->RemoveOwningNode(trunkClonedTable);
        }

        const auto& sourceTablets = trunkSourceTable->Tablets();
        YT_VERIFY(!sourceTablets.empty());
        auto& clonedTablets = trunkClonedTable->MutableTablets();
        YT_VERIFY(clonedTablets.empty());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        TChunkLists clonedRootChunkLists;
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            auto* fakeClonedRootChunkList = fakeClonedRootChunkLists[contentType];
            auto* clonedRootChunkList = chunkManager->CreateChunkList(fakeClonedRootChunkList->GetKind());
            clonedRootChunkLists[contentType] = clonedRootChunkList;

            trunkClonedTable->SetChunkList(contentType, clonedRootChunkList);
            clonedRootChunkList->AddOwningNode(trunkClonedTable);
        }

        const auto& backupManager = Bootstrap_->GetBackupManager();

        clonedTablets.reserve(sourceTablets.size());

        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            auto* sourceRootChunkList = trunkSourceTable->GetChunkList(contentType);
            YT_VERIFY(sourceRootChunkList->Children().size() == sourceTablets.size());
        }

        for (int index = 0; index < std::ssize(sourceTablets); ++index) {
            auto* sourceTablet = sourceTablets[index];

            auto* clonedTablet = CreateTablet(trunkClonedTable, EObjectType::Tablet)->As<TTablet>();
            clonedTablet->CopyFrom(*sourceTablet);

            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                auto* sourceRootChunkList = trunkSourceTable->GetChunkList(contentType);
                auto* tabletChunkList = sourceRootChunkList->Children()[index];
                chunkManager->AttachToChunkList(clonedRootChunkLists[contentType], {tabletChunkList});
            }

            clonedTablets.push_back(clonedTablet);
            trunkClonedTable->AccountTabletStatistics(clonedTablet->GetTabletStatistics());

            backupManager->SetClonedTabletBackupState(
                clonedTablet,
                sourceTablet->As<TTablet>(),
                mode);
        }
        trunkClonedTable->RecomputeTabletMasterMemoryUsage();

        if (mode == ENodeCloneMode::Backup) {
            trunkClonedTable->SetBackupCheckpointTimestamp(
                trunkSourceTable->GetBackupCheckpointTimestamp());
            trunkSourceTable->SetBackupCheckpointTimestamp(NullTimestamp);

            trunkClonedTable->SetBackupMode(trunkSourceTable->GetBackupMode());
            trunkSourceTable->SetBackupMode(EBackupMode::None);
        } else if (mode != ENodeCloneMode::Restore) {
            trunkClonedTable->SetBackupCheckpointTimestamp(
                trunkSourceTable->GetBackupCheckpointTimestamp());
        }

        if (sourceTable->IsReplicated()) {
            auto* trunkReplicatedSourceTable = trunkSourceTable->As<TReplicatedTableNode>();
            auto* replicatedClonedTable = clonedTable->As<TReplicatedTableNode>();

            YT_VERIFY(mode != ENodeCloneMode::Move);

            for (const auto* replica : GetValuesSortedByKey(trunkReplicatedSourceTable->Replicas())) {
                const TTableReplicaBackupDescriptor* replicaBackupDescriptor = nullptr;

                if (isBackupAction) {
                    const auto& backupDescriptors = trunkReplicatedSourceTable->ReplicaBackupDescriptors();
                    for (const auto& descriptor : backupDescriptors) {
                        if (descriptor.ReplicaId == replica->GetId()) {
                            replicaBackupDescriptor = &descriptor;
                            break;
                        }
                    }

                    if (!replicaBackupDescriptor) {
                        YT_LOG_DEBUG(
                            "Will not clone table replica since it does not participate in backup "
                            "(TableId: %v, ReplicaId: %v)",
                            trunkReplicatedSourceTable->GetId(),
                            replica->GetId());
                        continue;
                    }
                }

                std::vector<i64> committedReplicationRowIndexes;
                committedReplicationRowIndexes.reserve(sourceTablets.size());

                for (int tabletIndex = 0; tabletIndex < std::ssize(sourceTablets); ++tabletIndex) {
                    auto* tablet = trunkReplicatedSourceTable->Tablets()[tabletIndex]->As<TTablet>();
                    YT_VERIFY(tablet == sourceTablets[tabletIndex]);

                    TTableReplicaInfo* replicaInfo;
                    if (mode == ENodeCloneMode::Backup && tablet->GetCell()) {
                        if (tablet->BackedUpReplicaInfos().contains(replica->GetId())) {
                            replicaInfo = &GetOrCrash(tablet->BackedUpReplicaInfos(), replica->GetId());
                        } else {
                            YT_LOG_ALERT(
                                "Tablet does not contain replica info during backup (TabletId: %v, "
                                "TableId: %v, ReplicaId: %v)",
                                tablet->GetId(),
                                trunkReplicatedSourceTable->GetId(),
                                replica->GetId());
                            replicaInfo = tablet->GetReplicaInfo(replica);
                        }
                    } else {
                        replicaInfo = tablet->GetReplicaInfo(replica);
                    }

                    auto replicationRowIndex = replicaInfo->GetCommittedReplicationRowIndex();
                    committedReplicationRowIndexes.push_back(replicationRowIndex);
                }

                TString newReplicaPath = isBackupAction
                    ? replicaBackupDescriptor->ReplicaPath
                    : replica->GetReplicaPath();

                auto* clonedReplica = CreateTableReplica(
                    replicatedClonedTable,
                    replica->GetClusterName(),
                    newReplicaPath,
                    replica->GetMode(),
                    replica->GetPreserveTimestamps(),
                    replica->GetAtomicity(),
                    /*enabled*/ false,
                    replica->GetStartReplicationTimestamp(),
                    committedReplicationRowIndexes,
                    replica->GetEnableReplicatedTableTracker());

                YT_LOG_DEBUG(
                    "Table replica cloned (OriginalReplicaId: %v, ClonedReplicaId: %v, "
                    "OriginalTableId: %v, ClonedTableId: %v, OriginalReplicaPath: %v, ClonedReplicaPath: %v)",
                    replica->GetId(),
                    clonedReplica->GetId(),
                    sourceTable->GetId(),
                    clonedTable->GetId(),
                    replica->GetReplicaPath(),
                    clonedReplica->GetReplicaPath());
            }

            if (isBackupAction) {
                trunkSourceTable->MutableReplicaBackupDescriptors().clear();
                trunkClonedTable->MutableReplicaBackupDescriptors().clear();

                for (auto& tablet : sourceTablets) {
                    tablet->As<TTablet>()->BackedUpReplicaInfos().clear();
                }
            }
        }

        if (mode == ENodeCloneMode::Backup) {
            backupManager->ReleaseBackupCheckpoint(
                trunkSourceTable,
                sourceTable->GetTransaction());
        }

        UpdateResourceUsage(
            trunkClonedTable,
            trunkClonedTable->GetTabletResourceUsage(),
            /*scheduleTableDataStatisticsUpdate*/ false);

        backupManager->UpdateAggregatedBackupState(trunkClonedTable);
    }


    void ValidateMakeTableDynamic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsDynamic()) {
            return;
        }

        ValidateResourceUsageIncrease(table, TTabletResources().SetTabletCount(1));
    }

    void MakeTableDynamic(TTableNode* table, i64 trimmedRowCount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsDynamic()) {
            return;
        }

        table->SetDynamic(true);

        if (table->IsExternal()) {
            return;
        }

        auto* tablet = CreateTablet(table, EObjectType::Tablet)->As<TTablet>();
        tablet->SetIndex(0);
        if (table->IsSorted()) {
            tablet->SetPivotKey(EmptyKey());
        }
        tablet->SetTrimmedRowCount(trimmedRowCount);
        table->MutableTablets() = {tablet};
        table->RecomputeTabletMasterMemoryUsage();

        TabletChunkManager_->MakeTableDynamic(table);

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
        UpdateResourceUsage(
            table,
            table->GetTabletResourceUsage(),
            /*scheduleTableDataStatisticsUpdate*/ false);

        table->AccountTabletStatistics(tablet->GetTabletStatistics());

        YT_LOG_DEBUG("Table is switched to dynamic mode (TableId: %v)",
            table->GetId());
    }

    void ValidateMakeTableStatic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            return;
        }

        table->ValidateNotBackup("Cannot switch backup table to static mode");

        if (table->IsPhysicallyLog()) {
            THROW_ERROR_EXCEPTION("Cannot switch mode from dynamic to static for table type %Qlv",
                table->GetType());
        }

        if (table->IsSorted()) {
            THROW_ERROR_EXCEPTION("Cannot switch mode from dynamic to static: table is sorted");
        }

        table->ValidateAllTabletsUnmounted("Cannot switch mode from dynamic to static");
    }

    void MakeTableStatic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            return;
        }

        table->SetDynamic(false);

        if (table->IsExternal()) {
            return;
        }

        for (auto* tablet : table->Tablets()) {
            table->DiscountTabletStatistics(tablet->GetTabletStatistics());
        }

        auto tabletResourceUsage = table->GetTabletResourceUsage();

        TabletChunkManager_->MakeTableStatic(table);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* tablet : table->Tablets()) {
            tablet->SetOwner(nullptr);
            objectManager->UnrefObject(tablet);
        }
        table->MutableTablets().clear();
        table->RecomputeTabletMasterMemoryUsage();

        table->SetLastCommitTimestamp(NullTimestamp);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
        UpdateResourceUsage(
            table,
            -tabletResourceUsage,
            /*scheduleTableDataStatisticsUpdate*/ false);

        YT_LOG_DEBUG("Table is switched to static mode (TableId: %v)",
            table->GetId());
    }

    void LockDynamicTable(
        TTableNode* table,
        TTransaction* transaction,
        TTimestamp timestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!GetDynamicConfig()->EnableBulkInsert) {
            THROW_ERROR_EXCEPTION("Bulk insert is disabled");
        }

        if (table->DynamicTableLocks().contains(transaction->GetId())) {
            THROW_ERROR_EXCEPTION("Dynamic table is already locked by this transaction")
                << TErrorAttribute("transaction_id", transaction->GetId());
        }

        table->ValidateNotBackup("Bulk insert into backup tables is not supported");

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        int pendingTabletCount = 0;

        for (auto* tablet : table->Tablets()) {
            if (tablet->GetState() == ETabletState::Unmounted) {
                continue;
            }

            ++pendingTabletCount;
            YT_VERIFY(tablet->As<TTablet>()->UnconfirmedDynamicTableLocks().emplace(transaction->GetId()).second);

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            TReqLockTablet req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            ToProto(req.mutable_lock()->mutable_transaction_id(), transaction->GetId());
            req.mutable_lock()->set_timestamp(static_cast<i64>(timestamp));
            hiveManager->PostMessage(mailbox, req);
        }

        transaction->LockedDynamicTables().insert(table);
        table->AddDynamicTableLock(transaction->GetId(), timestamp, pendingTabletCount);

        YT_LOG_DEBUG("Waiting for tablet lock confirmation (TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
            table->GetId(),
            transaction->GetId(),
            pendingTabletCount);
    }

    void CheckDynamicTableLock(
        TTableNode* table,
        TTransaction* transaction,
        NTableClient::NProto::TRspCheckDynamicTableLock* response)
    {
        Bootstrap_->VerifyPersistentStateRead();

        YT_VERIFY(table->IsTrunk());

        auto it = table->DynamicTableLocks().find(transaction->GetId());
        response->set_confirmed(it && it->second.PendingTabletCount == 0);
    }

    void RecomputeTableTabletStatistics(TTabletOwnerBase* table)
    {
        table->ResetTabletStatistics();
        for (const auto* tablet : table->Tablets()) {
            table->AccountTabletStatistics(tablet->GetTabletStatistics());
        }
    }

    void OnNodeStorageParametersUpdated(TChunkOwnerBase* node)
    {
        if (!IsTabletOwnerType(node->GetType())) {
            return;
        }
        if (IsTableType(node->GetType()) && !node->As<TTableNode>()->IsDynamic()) {
            return;
        }

        auto* tabletOwner = node->As<TTabletOwnerBase>();

        YT_LOG_DEBUG("Tablet owner replication changed, will recompute tablet statistics "
            "(NodeId: %v)",
            tabletOwner->GetId());
        RecomputeTableTabletStatistics(tabletOwner);
    }

    TTabletCell* GetTabletCellOrThrow(TTabletCellId id)
    {
        auto* cell = FindTabletCell(id);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell %v",
                id);
        }
        return cell;
    }

    void ZombifyTabletCell(TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto actions = cell->Actions();
        for (auto* action : actions) {
            // NB: If destination cell disappears, don't drop action - let it continue with some other cells.
            UnbindTabletActionFromCells(action);
            OnTabletActionDisturbed(action, TError("Tablet cell %v has been removed", cell->GetId()));
        }
        YT_VERIFY(cell->Actions().empty());
    }

    TNode* FindTabletLeaderNode(const TTabletBase* tablet) const
    {
        if (!tablet) {
            return nullptr;
        }

        auto* cell = tablet->GetCell();
        if (!cell) {
            return nullptr;
        }

        int leadingPeerId = cell->GetLeadingPeerId();
        if (leadingPeerId == InvalidPeerId) {
            return nullptr;
        }

        YT_VERIFY(leadingPeerId < std::ssize(cell->Peers()));

        return cell->Peers()[leadingPeerId].Node;
    }

    TTabletCellBundle* FindTabletCellBundle(TTabletCellBundleId id)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cellBundle = cellManager->FindCellBundle(id);
        if (!cellBundle) {
            return nullptr;
        }
        return cellBundle->GetType() == EObjectType::TabletCellBundle
            ? cellBundle->As<TTabletCellBundle>()
            : nullptr;
    }

    TTabletCellBundle* GetTabletCellBundleOrThrow(TTabletCellBundleId id)
    {
        auto* cellBundle = FindTabletCellBundle(id);
        if (!cellBundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell bundle %v",
                id);
        }
        return cellBundle;
    }

    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly)
    {
        auto* cellBundle = DoFindTabletCellBundleByName(name);
        if (!cellBundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell bundle %Qv",
                name);
        }

        if (activeLifeStageOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle);
        }

        return cellBundle;
    }

    TTabletCellBundle* GetDefaultTabletCellBundle()
    {
        return GetBuiltin(DefaultTabletCellBundle_);
    }

    void SetTabletCellBundle(TTabletOwnerBase* table, TTabletCellBundle* newBundle)
    {
        YT_VERIFY(table->IsTrunk());

        auto* oldBundle = table->TabletCellBundle().Get();
        if (oldBundle == newBundle) {
            return;
        }

        if (IsTableType(table->GetType())) {
            DoSetTabletCellBundle(
                table->As<TTableNode>(),
                oldBundle,
                newBundle);
        } else if (table->GetType() == EObjectType::HunkStorage) {
            DoSetTabletCellBundle(
                table->As<THunkStorageNode>(),
                oldBundle,
                newBundle);
        } else {
            YT_ABORT();
        }

        table->TabletCellBundle().Assign(newBundle);
    }

    void DoSetTabletCellBundle(
        TTableNode* table,
        TTabletCellBundle* oldBundle,
        TTabletCellBundle* newBundle)
    {
        YT_VERIFY(table->IsTrunk());

        if (table->IsDynamic()) {
            DoSetTabletCellBundleImpl(table, oldBundle, newBundle);
        }
    }

    void DoSetTabletCellBundle(
        THunkStorageNode* hunkStorage,
        TTabletCellBundle* oldBundle,
        TTabletCellBundle* newBundle)
    {
        YT_VERIFY(hunkStorage->IsTrunk());

        DoSetTabletCellBundleImpl(hunkStorage, oldBundle, newBundle);
    }

    void DoSetTabletCellBundleImpl(
        TTabletOwnerBase* table,
        TTabletCellBundle* oldBundle,
        TTabletCellBundle* newBundle)
    {
        YT_VERIFY(table->IsTrunk());

        auto resourceUsageDelta = table->GetTabletResourceUsage();

        if (table->IsNative()) {
            table->ValidateAllTabletsUnmounted("Cannot change tablet cell bundle");
            if (newBundle && GetDynamicConfig()->EnableTabletResourceValidation) {
                newBundle->ValidateResourceUsageIncrease(resourceUsageDelta);
            }
        }

        if (!table->IsExternal()) {
            if (oldBundle) {
                oldBundle->UpdateResourceUsage(-resourceUsageDelta);
            }
            if (newBundle) {
                newBundle->UpdateResourceUsage(resourceUsageDelta);
            }
        }
    }

    void RecomputeTabletCellStatistics(TCellBase* cellBase)
    {
        if (!IsObjectAlive(cellBase) || cellBase->GetType() != EObjectType::TabletCell) {
            return;
        }

        auto* cell = cellBase->As<TTabletCell>();
        cell->RecomputeClusterStatistics();
    }

    void RecomputeAllTabletCellStatistics()
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto* cellBase : cellManager->Cells(ECellarType::Tablet)) {
            YT_VERIFY(cellBase->GetType() == EObjectType::TabletCell);
            auto* cell = cellBase->As<TTabletCell>();
            cell->GossipStatistics().Local() = NTabletServer::TTabletCellStatistics();
            for (const auto* tablet : cell->Tablets()) {
                if (tablet->Servant().GetCell() == cell) {
                    cell->GossipStatistics().Local() += tablet->GetTabletStatistics();
                } else if (tablet->AuxiliaryServant().GetCell() == cell) {
                    cell->GossipStatistics().Local() += tablet->GetTabletStatistics(/*fromAuxiliaryCell*/ true);
                } else {
                    YT_LOG_ALERT("Tablet belongs to a cell by neither of its servants "
                        "(CellId: %v, TabletId: %v, TableId: %v)",
                        cell->GetId(),
                        tablet->GetId(),
                        tablet->GetOwner()->GetId());
                }
            }
        }
    }

    void OnHunkJournalChunkSealed(TChunk* chunk)
    {
        YT_VERIFY(chunk->IsSealed());

        auto owningNodes = GetOwningNodes(chunk);
        std::vector<TTableNode*> tableNodes;

        for (auto* node : owningNodes) {
            if (!IsTableType(node->GetType())) {
                continue;
            }

            auto* tableNode = node->As<TTableNode>();
            if (!tableNode->IsDynamic()) {
                continue;
            }

            tableNodes.push_back(tableNode);
        }

        for (auto* table : tableNodes) {
            for (auto* tablet : table->Tablets()) {
                auto tabletStatistics = tablet->GetTabletStatistics();
                table->DiscountTabletStatistics(tabletStatistics);
            }
        }

        auto statistics = chunk->GetStatistics();
        AccumulateAncestorsStatistics(chunk, statistics);

        const auto& parents = chunk->Parents();
        for (auto* table : tableNodes) {
            RecomputeTableSnapshotStatistics(table);

            for (auto* tablet : table->Tablets()) {
                auto* tabletHunkChunkList = tablet->GetHunkChunkList();
                // TODO(aleksandra-zh): remove linear search if someday chunks will have a lot of parents.
                auto it = std::find_if(parents.begin(), parents.end(), [tabletHunkChunkList] (auto parent) {
                    return parent.first->GetId() == tabletHunkChunkList->GetId();
                });

                if (it != parents.end()) {
                    TabletChunkManager_->CopyChunkListIfShared(
                        table,
                        EChunkListContentType::Hunk,
                        tablet->GetIndex(),
                        tablet->GetIndex());
                }

                auto tabletStatistics = tablet->GetTabletStatistics();
                table->AccountTabletStatistics(tabletStatistics);
            }
        }
    }

    void AttachDynamicStoreToTablet(TTablet* tablet, TDynamicStore* dynamicStore)
    {
        auto* table = tablet->GetTable();

        TabletChunkManager_->CopyChunkListsIfShared(table, tablet->GetIndex(), tablet->GetIndex());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->AttachToChunkList(tablet->GetChunkList(), {dynamicStore});
        YT_LOG_DEBUG("Dynamic store attached to tablet (TabletId: %v, DynamicStoreId: %v)",
            tablet->GetId(),
            dynamicStore->GetId());

        RecomputeTableSnapshotStatistics(table);

        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            table,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false);

        // TODO(ifsmirnov): YT-17383
        YT_VERIFY(!tablet->AuxiliaryServant());

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = 1;
        tablet->GetCell()->GossipStatistics().Local() += statisticsDelta;
        tablet->GetTable()->AccountTabletStatisticsDelta(statisticsDelta);
    }

    void FireUponTableReplicaUpdate(TTableReplica* replica)
    {
        ReplicaCreated_.Fire(TReplicaData{
            .TableId = replica->GetTable()->GetId(),
            .Id = replica->GetId(),
            .Mode = replica->GetMode(),
            .Enabled = replica->GetState() == ETableReplicaState::Enabled,
            .ClusterName = replica->GetClusterName(),
            .TablePath = replica->GetReplicaPath(),
            .TrackingEnabled = replica->GetEnableReplicatedTableTracker(),
            .ContentType = ETableReplicaContentType::Data,
        });
    }

    void UpdateExtraMountConfigKeys(std::vector<TString> keys)
    {
        for (auto&& key : keys) {
            auto [it, inserted] = MountConfigKeysFromNodes_.insert(std::move(key));
            if (inserted) {
                YT_LOG_DEBUG(
                    "Registered new mount config key (Key: %v)",
                    *it);
            }
        }
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTabletBase);
    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

    DEFINE_SIGNAL_WITH_ACCESSOR(void(TReplicatedTableData), ReplicatedTableCreated);
    DEFINE_SIGNAL_WITH_ACCESSOR(void(TTableId), ReplicatedTableDestroyed);
    DEFINE_SIGNAL(void(TReplicaData), ReplicaCreated);
    DEFINE_SIGNAL(void(TTableReplicaId), ReplicaDestroyed);

private:
    template <class T>
    class TEntityMapTypeTraits
    {
    public:
        explicit TEntityMapTypeTraits(TBootstrap* bootstrap)
            : Bootstrap_(bootstrap)
        { }

        std::unique_ptr<T> Create(TObjectId id) const
        {
            auto type = TypeFromId(id);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& handler = objectManager->FindHandler(type);
            auto objectHolder = handler->InstantiateObject(id);
            return std::unique_ptr<T>(objectHolder.release()->As<T>());
        }

    private:
        TBootstrap* const Bootstrap_;
    };

    const TTabletServicePtr TabletService_;
    const TTabletBalancerPtr TabletBalancer_;
    const TTabletCellDecommissionerPtr TabletCellDecommissioner_;
    const TTabletActionManagerPtr TabletActionManager_;
    const ITabletChunkManagerPtr TabletChunkManager_;

    TEntityMap<TTabletBase, TEntityMapTypeTraits<TTabletBase>> TabletMap_;
    TEntityMap<TTableReplica> TableReplicaMap_;
    TEntityMap<TTabletAction> TabletActionMap_;

    TPeriodicExecutorPtr TabletCellStatisticsGossipExecutor_;
    TPeriodicExecutorPtr BundleResourceUsageGossipExecutor_;
    TPeriodicExecutorPtr ProfilingExecutor_;

    TTimeCounter TabletNodeHeartbeatCounter_ = TabletServerProfiler.TimeCounter("/tablet_node_heartbeat");

    // Mount config keys received from nodes. Persisted.
    THashSet<TString> MountConfigKeysFromNodes_;
    // Mount config keys known to the binary (by the moment of most recent reign change). Persisted.
    THashSet<TString> LocalMountConfigKeys_;

    INodePtr BuildOrchidYson() const
    {
        std::vector<TString> extraMountConfigKeys;
        for (const auto& key : MountConfigKeysFromNodes_) {
            if (!LocalMountConfigKeys_.contains(key)) {
                extraMountConfigKeys.push_back(key);
            }
        }

        // NB: Orchid node is materialized explicitly because |opaque| is not applied
        // if BuildYsonFluently(consumer) is used, and we want to save some screen space.
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("extra_mount_config_keys").Value(extraMountConfigKeys)
                .Item("local_mount_config_keys")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(LocalMountConfigKeys_)
                .Item("mount_config_keys_from_nodes")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(MountConfigKeysFromNodes_)
                .Item("non_avenue_tablet_count").Value(NonAvenueTabletCount_)
            .EndMap();
    }

    THashMap<TTabletCellBundleId, TTabletCellBundleProfilingCounters>
        BundleIdToProfilingCounters_;

    TTabletCellBundleId DefaultTabletCellBundleId_;
    TTabletCellBundle* DefaultTabletCellBundle_ = nullptr;

    TTabletCellBundleId SequoiaTabletCellBundleId_;
    TTabletCellBundle* SequoiaTabletCellBundle_ = nullptr;

    bool EnableUpdateStatisticsOnHeartbeat_ = true;

    // Not a compat, actually.
    bool FillMountConfigKeys_ = false;

    //! Hash parts of the avenue ids generated in current mutation.
    THashSet<ui32> GeneratedAvenueIdHashes_;

    // COMPAT(ifsmirnov)
    bool RecomputeAggregateTabletStatistics_ = false;
    // COMPAT(ifsmirnov)
    bool RecomputeHunkResourceUsage_ = false;

    // COMPAT(ifsmirnov)
    int NonAvenueTabletCount_ = 0;

    // COMPAT(alexelexa)
    bool NeedResetErrorCountOfUnmountedTablets_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    TTabletCellBundleProfilingCounters GetOrCreateBundleProfilingCounters(
        TTabletCellBundle* bundle)
    {
        auto it = BundleIdToProfilingCounters_.find(bundle->GetId());
        if (it != BundleIdToProfilingCounters_.end()) {
            if (it->second.BundleName == bundle->GetName()) {
                return it->second;
            } else {
                BundleIdToProfilingCounters_.erase(it);
            }
        }

        TTabletCellBundleProfilingCounters counters(bundle->GetName());
        BundleIdToProfilingCounters_.emplace(bundle->GetId(), counters);
        return counters;
    }

    void OnTabletCellDecommissionStarted(TCellBase* cellBase)
    {
        if (cellBase->GetType() != EObjectType::TabletCell) {
            return;
        }

        auto* cell = cellBase->As<TTabletCell>();
        auto actions = cell->Actions();
        for (auto* action : actions) {
            // NB: If destination cell disappears, don't drop action - let it continue with some other cells.
            UnbindTabletActionFromCells(action);
            OnTabletActionDisturbed(action, TError("Tablet cell %v has been decommissioned", cell->GetId()));
        }

        CheckIfFullyUnmounted(cell);
    }

    void CheckIfFullyUnmounted(TTabletCell* tabletCell)
    {
        if (!tabletCell->IsDecommissionStarted()) {
            return;
        }
        if (tabletCell->GossipStatistics().Local().TabletCount == 0) {
            tabletCell->GossipStatus().Local().Decommissioned = true;
        }
    }

    TTabletCellBundle* DoFindTabletCellBundleByName(const TString& name)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* bundle = cellManager->FindCellBundleByName(name, ECellarType::Tablet, false);
        if (!bundle) {
            return nullptr;
        }
        YT_VERIFY(bundle->GetType() == EObjectType::TabletCellBundle);
        return bundle->As<TTabletCellBundle>();
    }

    TTabletCellBundle* FindTabletCellBundleByName(const TString& name, bool activeLifeStageOnly)
    {
        auto* bundle = DoFindTabletCellBundleByName(name);
        if (!bundle) {
            return bundle;
        }

        if (activeLifeStageOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            return objectManager->IsObjectLifeStageValid(bundle)
                ? bundle
                : nullptr;
        } else {
            return bundle;
        }
    }

    TTabletAction* DoCreateTabletAction(
        TObjectId hintId,
        ETabletActionKind kind,
        ETabletActionState state,
        const std::vector<TTabletBase*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        std::optional<int> tabletCount,
        bool freeze,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime,
        std::optional<TDuration> expirationTimeout)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(state == ETabletActionState::Preparing || state == ETabletActionState::Orphaned);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletAction, hintId);
        auto actionHolder = TPoolAllocator::New<TTabletAction>(id);
        auto* action = TabletActionMap_.Insert(id, std::move(actionHolder));
        objectManager->RefObject(action);

        for (auto* tablet : tablets) {
            YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

            tablet->SetAction(action);

            if (state == ETabletActionState::Orphaned) {
                // Orphaned action can be created during mount if tablet cells are not available.
                // User can't create orphaned action directly because primary master need to know about mount.
                YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);
                tablet->SetExpectedState(freeze
                    ? ETabletState::Frozen
                    : ETabletState::Mounted);
            }
        }
        for (auto* cell : cells) {
            cell->Actions().insert(action);
        }

        action->SetKind(kind);
        action->SetState(state);
        action->Tablets() = std::move(tablets);
        action->TabletCells() = std::move(cells);
        action->PivotKeys() = std::move(pivotKeys);
        action->SetTabletCount(tabletCount);
        action->SetSkipFreezing(skipFreezing);
        action->SetFreeze(freeze);
        action->SetCorrelationId(correlationId);
        action->SetExpirationTime(expirationTime);
        action->SetExpirationTimeout(expirationTimeout);
        const auto& bundle = action->Tablets()[0]->GetOwner()->TabletCellBundle();
        action->SetTabletCellBundle(bundle.Get());
        bundle->TabletActions().insert(action);
        bundle->IncreaseActiveTabletActionCount();

        YT_LOG_DEBUG("Tablet action created (%v)",
            *action);

        return action;
    }

    void UnbindTabletActionFromCells(TTabletAction* action)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (auto* cell : action->TabletCells()) {
            cell->Actions().erase(action);
        }

        action->TabletCells().clear();
    }

    void UnbindTabletActionFromTablets(TTabletAction* action)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (auto* tablet : action->Tablets()) {
            YT_VERIFY(tablet->GetAction() == action);
            tablet->SetAction(nullptr);
        }

        action->SaveTabletIds();
        action->Tablets().clear();
    }

    void UnbindTabletAction(TTabletAction* action)
    {
        UnbindTabletActionFromTablets(action);
        UnbindTabletActionFromCells(action);
    }

    std::vector<TLegacyOwningKey> CalculatePivotKeys(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount)
    {
        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex);

        if (newTabletCount <= 0) {
            THROW_ERROR_EXCEPTION("Tablet count must be positive");
        }

        struct TEntry
        {
            TLegacyOwningKey MinKey;
            TLegacyOwningKey MaxKey;
            i64 Size;

            bool operator<(const TEntry& other) const
            {
                return MinKey < other.MinKey;
            }
        };

        std::vector<TEntry> entries;
        i64 totalSize = 0;

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index]->As<TTablet>();
            THashSet<TStoreId> edenStoreIds(
                tablet->EdenStoreIds().begin(),
                tablet->EdenStoreIds().end());

            auto chunksOrViews = EnumerateStoresInChunkTree(tablet->GetChunkList());
            for (const auto* chunkOrView : chunksOrViews) {
                const auto* chunk = chunkOrView->GetType() == EObjectType::ChunkView
                    ? chunkOrView->AsChunkView()->GetUnderlyingTree()->AsChunk()
                    : chunkOrView->AsChunk();
                if (chunk->GetChunkType() != EChunkType::Table) {
                    continue;
                }
                auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();
                auto eden = miscExt && miscExt->eden();
                if (eden || edenStoreIds.contains(chunkOrView->GetId())) {
                    continue;
                }

                auto size = chunk->GetUncompressedDataSize();
                entries.push_back({
                    GetMinKeyOrThrow(chunkOrView),
                    GetUpperBoundKeyOrThrow(chunkOrView),
                    size
                });
                totalSize += size;
            }
        }

        std::sort(entries.begin(), entries.end());

        i64 desired = DivCeil<i64>(totalSize, newTabletCount);
        auto* tablet = table->Tablets()[firstTabletIndex]->As<TTablet>();
        std::vector<TLegacyOwningKey> pivotKeys{tablet->GetPivotKey()};
        TLegacyOwningKey lastKey;
        i64 current = 0;

        for (const auto& entry : entries) {
            if (lastKey && lastKey <= entry.MinKey) {
                if (current >= desired) {
                    current = 0;
                    pivotKeys.push_back(entry.MinKey);
                    lastKey = entry.MaxKey;
                    if (ssize(pivotKeys) == newTabletCount) {
                        break;
                    }
                }
            } else if (entry.MaxKey > lastKey) {
                lastKey = entry.MaxKey;
            }
            current += entry.Size;
        }

        return pivotKeys;
    }

    void MountMissedInActionTablets(TTabletAction* action)
    {
        for (auto* tablet : action->Tablets()) {
            try {
                if (!IsObjectAlive(tablet)) {
                    continue;
                }

                if (!IsObjectAlive(tablet->GetOwner())) {
                    continue;
                }

                switch (tablet->GetState()) {
                    case ETabletState::Mounted:
                        break;

                    case ETabletState::Unmounted:
                        MountTablet(tablet, nullptr, action->GetFreeze());
                        break;

                    case ETabletState::Frozen:
                        if (!action->GetFreeze()) {
                            DoUnfreezeTablet(tablet);
                        }
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Tablet %v is in unrecognized state %Qv",
                            tablet->GetId(),
                            tablet->GetState());
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error mounting missed in action tablet "
                    "(TabletId: %v, TableId: %v, Bundle: %v, ActionId: %v, TabletBalancerCorrelationId: %v)",
                    tablet->GetId(),
                    tablet->GetOwner()->GetId(),
                    action->GetTabletCellBundle()->GetName(),
                    action->GetId(),
                    action->GetCorrelationId());
            }
        }
    }

    void OnTabletActionTabletsTouched(
        TTabletAction* action,
        const THashSet<TTabletBase*>& touchedTablets,
        const TError& error)
    {
        bool touched = false;
        for (auto* tablet : action->Tablets()) {
            if (touchedTablets.find(tablet) != touchedTablets.end()) {
                YT_VERIFY(tablet->GetAction() == action);
                tablet->SetAction(nullptr);
                // Restore expected state YT-17492.
                tablet->SetState(tablet->GetState());
                touched = true;
            }
        }

        if (!touched) {
            return;
        }

        action->SaveTabletIds();

        // Smooth move actions will deal with their tables later.
        if (action->GetKind() != ETabletActionKind::SmoothMove) {
            auto& tablets = action->Tablets();
            tablets.erase(
                std::remove_if(
                    tablets.begin(),
                    tablets.end(),
                    [&] (auto* tablet) {
                        return touchedTablets.find(tablet) != touchedTablets.end();
                    }),
                tablets.end());
        }

        UnbindTabletActionFromCells(action);
        OnTabletActionDisturbed(action, error);
    }

    void TouchAffectedTabletActions(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const TString& request)
    {
        YT_VERIFY(firstTabletIndex >= 0 && firstTabletIndex <= lastTabletIndex && lastTabletIndex < std::ssize(table->Tablets()));

        auto error = TError("User request %Qv interfered with the action", request);
        THashSet<TTabletBase*> touchedTablets;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            touchedTablets.insert(table->Tablets()[index]);
        }
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            if (auto* action = table->Tablets()[index]->GetAction()) {
                OnTabletActionTabletsTouched(action, touchedTablets, error);
            }
        }
    }

    void ChangeTabletActionState(TTabletAction* action, ETabletActionState state, bool recursive = true)
    {
        action->SetState(state);
        if (action->IsFinished() && action->GetExpirationTimeout()) {
            action->SetExpirationTime(GetCurrentMutationContext()->GetTimestamp() + *action->GetExpirationTimeout());
        }

        auto tableId = action->Tablets().empty()
            ? TTableId{}
            : action->Tablets()[0]->GetOwner()->GetId();
        YT_LOG_DEBUG("Change tablet action state (ActionId: %v, State: %v, "
            "TableId: %v, Bundle: %v, TabletBalancerCorrelationId: %v),",
            action->GetId(),
            state,
            tableId,
            action->GetTabletCellBundle()->GetName(),
            action->GetCorrelationId());
        if (recursive) {
            OnTabletActionStateChanged(action);
        }
    }

    void OnTabletActionDisturbed(TTabletAction* action, const TError& error)
    {
        // Take care of a rare case when tablet action has been already removed (cf. YT-9754).
        if (!IsObjectAlive(action)) {
            return;
        }

        if (action->Tablets().empty()) {
            action->Error() = error;
            ChangeTabletActionState(action, ETabletActionState::Failed);
            return;
        }

        switch (action->GetState()) {
            case ETabletActionState::Unmounting:
            case ETabletActionState::Freezing:
                // Wait until tablets are unmounted, then mount them.
                action->Error() = error;
                break;

            case ETabletActionState::Mounting:
                // Nothing can be done here.
                action->Error() = error;
                ChangeTabletActionState(action, ETabletActionState::Failed);
                break;

            case ETabletActionState::MountingAuxiliary:
            case ETabletActionState::WaitingForSmoothMove:
                action->Error() = error;
                ChangeTabletActionState(action, ETabletActionState::AbortingSmoothMove);
                break;

            case ETabletActionState::Completed:
            case ETabletActionState::Failed:
                // All tablets have been already taken care of. Do nothing.
                break;

            case ETabletActionState::Mounted:
            case ETabletActionState::Frozen:
            case ETabletActionState::Unmounted:
            case ETabletActionState::Preparing:
            case ETabletActionState::Failing:
                // Transient states inside mutation. Nothing wrong should happen here.
                YT_ABORT();

            default:
                YT_ABORT();
        }
    }

    void OnTabletActionStateChanged(TTabletAction* action)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!action) {
            return;
        }

        bool repeat;
        do {
            repeat = false;
            try {
                DoTabletActionStateChanged(action);
            } catch (const std::exception& ex) {
                YT_VERIFY(action->GetState() != ETabletActionState::Failing);
                action->Error() = TError(ex);
                if (action->GetState() != ETabletActionState::Unmounting) {
                    ChangeTabletActionState(action, ETabletActionState::Failing, false);
                }
                repeat = true;
            }
        } while (repeat);
    }

    void DoTabletActionStateChanged(TTabletAction* action)
    {
        switch (action->GetState()) {
            case ETabletActionState::Preparing: {
                if (action->GetKind() == ETabletActionKind::SmoothMove) {
                    YT_VERIFY(action->Tablets().size() == 1);
                    YT_VERIFY(action->TabletCells().size() == 1);

                    auto* cell = action->TabletCells()[0];
                    YT_VERIFY(action->Tablets()[0]->GetType() == EObjectType::Tablet);

                    auto* tablet = action->Tablets()[0]->As<TTablet>();

                    TTableSettings tableSettings;
                    try {
                        tableSettings = GetTableSettings(
                            tablet->GetTable(),
                            Bootstrap_->GetObjectManager(),
                            Bootstrap_->GetChunkManager(),
                            GetDynamicConfig());
                    } catch (const std::exception& ex) {
                        YT_LOG_DEBUG(ex, "Failed to mount auxiliary servant "
                            "(TableId: %v, TabletId: %v, AuxiliaryCellId: %v)",
                            tablet->GetTable()->GetId(),
                            tablet->GetId(),
                            cell->GetId());
                        throw;
                    }

                    GeneratedAvenueIdHashes_.clear();

                    // TODO(ifsmirnov): YT-20959 - send updated settings to sibling
                    // and unban remount.
                    auto serializedTableSettings = SerializeTableSettings(tableSettings);
                    AllocateAuxiliaryServant(tablet, cell, serializedTableSettings);
                    ChangeTabletActionState(
                        action,
                        ETabletActionState::MountingAuxiliary,
                        /*recursive*/ false);

                    break;
                }

                if (action->GetSkipFreezing()) {
                    ChangeTabletActionState(action, ETabletActionState::Frozen);
                    break;
                }

                for (auto* tablet : action->Tablets()) {
                    DoFreezeTablet(tablet);
                }

                ChangeTabletActionState(action, ETabletActionState::Freezing);
                break;
            }

            case ETabletActionState::Freezing: {
                int freezingCount = 0;
                for (const auto* tablet : action->Tablets()) {
                    YT_VERIFY(IsObjectAlive(tablet));
                    if (tablet->GetState() == ETabletState::Freezing) {
                        ++freezingCount;
                    }
                }
                if (freezingCount == 0) {
                    auto state = action->Error().IsOK()
                        ? ETabletActionState::Frozen
                        : ETabletActionState::Failing;
                    ChangeTabletActionState(action, state);
                }
                break;
            }

            case ETabletActionState::Frozen: {
                for (auto* tablet : action->Tablets()) {
                    YT_VERIFY(IsObjectAlive(tablet));
                    UnmountTablet(tablet, /*force*/ false, /*onDestroy*/ false);
                }

                ChangeTabletActionState(action, ETabletActionState::Unmounting);
                break;
            }

            case ETabletActionState::Unmounting: {
                int unmountingCount = 0;
                for (const auto* tablet : action->Tablets()) {
                    YT_VERIFY(IsObjectAlive(tablet));
                    if (tablet->GetState() == ETabletState::Unmounting) {
                        ++unmountingCount;
                    }
                }
                if (unmountingCount == 0) {
                    auto state = action->Error().IsOK()
                        ? ETabletActionState::Unmounted
                        : ETabletActionState::Failing;
                    ChangeTabletActionState(action, state);
                }
                break;
            }

            case ETabletActionState::Unmounted: {
                YT_VERIFY(!action->Tablets().empty());
                auto* table = action->Tablets().front()->GetOwner();
                if (!IsObjectAlive(table)) {
                    THROW_ERROR_EXCEPTION("Table is not alive");
                }

                switch (action->GetKind()) {
                    case ETabletActionKind::Move:
                        break;

                    case ETabletActionKind::Reshard: {
                        int firstTabletIndex = action->Tablets().front()->GetIndex();
                        int lastTabletIndex = action->Tablets().back()->GetIndex();

                        auto expectedState = action->GetFreeze() ? ETabletState::Frozen : ETabletState::Mounted;

                        std::vector<TTabletBase*> oldTablets;
                        oldTablets.swap(action->Tablets());
                        for (auto* tablet : oldTablets) {
                            tablet->SetAction(nullptr);
                        }
                        for (auto* tablet : oldTablets) {
                            if (tablet->GetExpectedState() != expectedState) {
                                YT_LOG_ALERT_IF(tablet->GetExpectedState() != expectedState,
                                    "Unexpected tablet expected state, try fixing with unmount plus mount "
                                    "(TableId: %v, TabletId: %v, ActionId: %v, ActionExpected: %v, TabletExpected: %v)",
                                    tablet->As<TTablet>()->GetTable()->GetId(),
                                    tablet->GetId(),
                                    action->GetId(),
                                    expectedState,
                                    tablet->GetExpectedState());
                                THROW_ERROR_EXCEPTION("Tablet action canceled due to a bug");
                            }
                        }

                        int newTabletCount = action->GetTabletCount()
                            ? *action->GetTabletCount()
                            : action->PivotKeys().size();

                        try {
                            // TODO(ifsmirnov) Use custom locking to allow reshard when locked by operation and upload has not been started yet.
                            const auto& cypressManager = Bootstrap_->GetCypressManager();
                            cypressManager->LockNode(table, nullptr, ELockMode::Exclusive);

                            PrepareReshard(
                                table,
                                firstTabletIndex,
                                lastTabletIndex,
                                newTabletCount,
                                action->PivotKeys(),
                                /*trimmedRowCounts*/ {});
                            newTabletCount = DoReshard(
                                table,
                                firstTabletIndex,
                                lastTabletIndex,
                                newTabletCount,
                                action->PivotKeys(),
                                /*trimmedRowCounts*/ {});
                        } catch (const std::exception& ex) {
                            for (auto* tablet : oldTablets) {
                                YT_VERIFY(IsObjectAlive(tablet));
                                tablet->SetAction(action);
                            }
                            action->Tablets() = std::move(oldTablets);
                            throw;
                        }

                        action->Tablets() = std::vector<TTabletBase*>(
                            table->Tablets().begin() + firstTabletIndex,
                            table->Tablets().begin() + firstTabletIndex + newTabletCount);
                        for (auto* tablet : action->Tablets()) {
                            tablet->SetAction(action);
                            tablet->SetExpectedState(expectedState);
                        }

                        break;
                    }

                    default:
                        YT_ABORT();
                }

                TTableSettings tableSettings;
                try {
                    tableSettings = GetTableSettings(
                        table->As<TTableNode>(),
                        Bootstrap_->GetObjectManager(),
                        Bootstrap_->GetChunkManager(),
                        GetDynamicConfig());

                    ValidateTableMountConfig(
                        table->As<TTableNode>(),
                        tableSettings.EffectiveMountConfig,
                        GetDynamicConfig());
                } catch (const std::exception& ex) {
                    YT_LOG_ALERT(ex, "Tablet action failed to mount tablets because "
                        "of table mount settings validation error (ActionId: %v, TableId: %v)",
                        action->GetId(),
                        table->GetId());

                    THROW_ERROR_EXCEPTION("Failed to validate table mount settings")
                        << TErrorAttribute("table_id", table->GetId())
                        << ex;
                }
                auto serializedTableSettings = SerializeTableSettings(tableSettings);

                std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;
                if (action->TabletCells().empty()) {
                    if (!CheckHasHealthyCells(table->TabletCellBundle().Get())) {
                        ChangeTabletActionState(action, ETabletActionState::Orphaned, false);
                        break;
                    }

                    assignment = ComputeTabletAssignment(
                        table,
                        nullptr,
                        action->Tablets());
                } else {
                    YT_VERIFY(action->TabletCells().size() >= action->Tablets().size());
                    for (int index = 0; index < std::ssize(action->Tablets()); ++index) {
                        assignment.emplace_back(
                            action->Tablets()[index],
                            action->TabletCells()[index]);
                    }
                }

                DoMountTablets(table, serializedTableSettings, assignment, action->GetFreeze());

                ChangeTabletActionState(action, ETabletActionState::Mounting);
                break;
            }

            case ETabletActionState::Mounting: {
                int mountedCount = 0;
                for (const auto* tablet : action->Tablets()) {
                    YT_VERIFY(IsObjectAlive(tablet));
                    if (tablet->GetState() == ETabletState::Mounted ||
                        tablet->GetState() == ETabletState::Frozen)
                    {
                        ++mountedCount;
                    }
                }

                if (mountedCount == std::ssize(action->Tablets())) {
                    ChangeTabletActionState(action, ETabletActionState::Mounted);
                }
                break;
            }

            case ETabletActionState::Mounted: {
                ChangeTabletActionState(action, ETabletActionState::Completed);
                break;
            }

            case ETabletActionState::Failing: {
                YT_LOG_DEBUG(action->Error(), "Tablet action failed (ActionId: %v, TabletBalancerCorrelationId: %v)",
                    action->GetId(),
                    action->GetCorrelationId());

                MountMissedInActionTablets(action);
                UnbindTabletAction(action);
                ChangeTabletActionState(action, ETabletActionState::Failed);
                break;
            }

            case ETabletActionState::Completed:
                if (!action->Error().IsOK()) {
                    ChangeTabletActionState(action, ETabletActionState::Failed, false);
                }
                // No break intentionally.
            case ETabletActionState::Failed: {
                UnbindTabletAction(action);
                const auto now = GetCurrentMutationContext()->GetTimestamp();
                if (action->GetExpirationTime() <= now) {
                    const auto& objectManager = Bootstrap_->GetObjectManager();
                    objectManager->UnrefObject(action);
                }
                if (auto* bundle = action->GetTabletCellBundle()) {
                    bundle->DecreaseActiveTabletActionCount();
                }
                break;
            }

            case ETabletActionState::MountingAuxiliary: {
                auto* tablet = action->Tablets()[0];
                if (tablet->AuxiliaryServant().GetState() != ETabletState::Mounted) {
                    break;
                }

                auto* cell = action->TabletCells()[0];
                YT_VERIFY(tablet->AuxiliaryServant().GetCell() == cell);

                TReqStartSmoothMovement req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                ToProto(req.mutable_target_cell_id(), cell->GetId());
                req.set_source_mount_revision(tablet->Servant().GetMountRevision());
                req.set_target_mount_revision(tablet->AuxiliaryServant().GetMountRevision());
                ToProto(
                    req.mutable_source_avenue_endpoint_id(),
                    tablet->GetTabletwiseAvenueEndpointId());

                const auto& hiveManager = Bootstrap_->GetHiveManager();
                auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
                hiveManager->PostMessage(mailbox, req);

                ChangeTabletActionState(action, ETabletActionState::WaitingForSmoothMove);
                break;
            }

            case ETabletActionState::WaitingForSmoothMove: {
                auto* cell = action->TabletCells()[0];
                auto* tablet = action->Tablets()[0];
                if (tablet->AuxiliaryServant()) {
                    break;
                }
                if (tablet->Servant().GetCell() != cell) {
                    break;
                }
                ChangeTabletActionState(action, ETabletActionState::Completed);
                break;
            }

            case ETabletActionState::AbortingSmoothMove: {
                auto* tablet = action->Tablets()[0];
                YT_VERIFY(tablet->Servant());

                if (tablet->AuxiliaryServant()) {
                    if (auto state = tablet->AuxiliaryServant().GetState();
                        state == ETabletState::Mounting || state == ETabletState::FrozenMounting)
                    {
                        // Auxiliary servant has not yet been mounted. Main servant does not participate
                        // in movement yet.
                        DeallocateAuxiliaryServant(tablet);
                    } else {
                        // Main smooth movement phase is in progress, two-phase abort is required.
                        TReqAbortSmoothMovement req;
                        ToProto(req.mutable_tablet_id(), tablet->GetId());

                        const auto& hiveManager = Bootstrap_->GetHiveManager();
                        auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
                        hiveManager->PostMessage(mailbox, req);
                    }
                }

                tablet->SetAction(nullptr);
                action->Tablets().clear();

                YT_LOG_DEBUG(action->Error(), "Smooth movement aborted (ActionId: %v, TabletId: %v)",
                    action->GetId(),
                    tablet->GetId());

                ChangeTabletActionState(action, ETabletActionState::Failed);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void HydraKickOrphanedTabletActions(NProto::TReqKickOrphanedTabletActions* request)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        THashSet<TCellBundle*> healthyBundles;
        for (auto* bundle : cellManager->CellBundles(ECellarType::Tablet)) {
            if (!IsObjectAlive(bundle)) {
                continue;
            }

            for (auto* cellBase : bundle->Cells()) {
                YT_VERIFY(cellBase->GetType() == EObjectType::TabletCell);
                auto* cell = cellBase->As<TTabletCell>();
                if (IsCellActive(cell)) {
                    healthyBundles.insert(cell->CellBundle().Get());
                    continue;
                }
            }
        }

        auto orphanedActionIds = FromProto<std::vector<TTabletActionId>>(request->tablet_action_ids());
        for (auto actionId : orphanedActionIds) {
            auto* action = FindTabletAction(actionId);
            if (IsObjectAlive(action) && action->GetState() == ETabletActionState::Orphaned) {
                const auto& bundle = action->Tablets().front()->GetOwner()->TabletCellBundle();
                if (healthyBundles.contains(bundle.Get())) {
                    ChangeTabletActionState(action, ETabletActionState::Unmounted);
                }
            }
        }
    }

    void MountTablet(
        TTabletBase* tablet,
        TTabletCell* cell,
        bool freeze)
    {
        auto* table = tablet->GetOwner();
        auto tableSettings = GetTabletOwnerSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        auto serializedTableSettings = SerializeTabletOwnerSettings(tableSettings);
        auto assignment = ComputeTabletAssignment(
            table,
            cell,
            std::vector<TTabletBase*>{tablet});

        DoMountTablets(table, serializedTableSettings, assignment, freeze);
    }

    TReqMountTablet PrepareMountRequestStem(
        TTablet* tablet,
        const TSerializedTableSettings& serializedTableSettings)
    {
        const auto* table = tablet->GetTable();

        TReqMountTablet req;
        auto& reqEssential = *req.mutable_essential_content();

        reqEssential.set_path(table->GetMountPath());
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        ToProto(req.mutable_table_id(), table->GetId());

        ToProto(reqEssential.mutable_schema_id(), table->GetSchema()->GetId());
        ToProto(reqEssential.mutable_schema(), *table->GetSchema()->AsTableSchema());

        FillTableSettings(req.mutable_replicatable_content(), serializedTableSettings);

        const auto& allTablets = table->Tablets();
        int tabletIndex = tablet->GetIndex();
        if (table->IsSorted() && !table->IsReplicated()) {
            ToProto(reqEssential.mutable_pivot_key(), tablet->GetPivotKey());
            ToProto(reqEssential.mutable_next_pivot_key(), tablet->GetIndex() + 1 == std::ssize(allTablets)
                ? MaxKey()
                : allTablets[tabletIndex + 1]->As<TTablet>()->GetPivotKey());
        } else if (!table->IsSorted()) {
            auto lower = tabletIndex == 0
                ? EmptyKey()
                : MakeUnversionedOwningRow(tablet->GetIndex());
            auto upper = tabletIndex + 1 == std::ssize(allTablets)
                ? MaxKey()
                : MakeUnversionedOwningRow(tablet->GetIndex() + 1);
            ToProto(reqEssential.mutable_pivot_key(), lower);
            ToProto(reqEssential.mutable_next_pivot_key(), upper);
        }

        reqEssential.set_atomicity(ToProto<int>(table->GetAtomicity()));
        reqEssential.set_commit_ordering(ToProto<int>(table->GetCommitOrdering()));
        ToProto(reqEssential.mutable_upstream_replica_id(), table->GetUpstreamReplicaId());

        return req;
    }

    void DoMountTablets(
        TTabletOwnerBase* table,
        const TSerializedTabletOwnerSettings& serializedTableSettings,
        const std::vector<std::pair<TTabletBase*, TTabletCell*>>& assignment,
        bool freeze,
        TTimestamp mountTimestamp = NullTimestamp)
    {
        if (IsTableType(table->GetType())) {
            auto* typedTable = table->As<TTableNode>();
            typedTable->SetMountedWithEnabledDynamicStoreRead(
                IsDynamicStoreReadEnabled(typedTable, GetDynamicConfig()));
        }

        GeneratedAvenueIdHashes_.clear();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        TTabletResources resourceUsageDelta;
        const auto& allTablets = table->Tablets();
        for (auto [tablet, cell] : assignment) {
            YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);

            if (!IsCellActive(cell) && tablet->GetType() == EObjectType::Tablet) {
                DoCreateTabletAction(
                    TObjectId(),
                    ETabletActionKind::Move,
                    ETabletActionState::Orphaned,
                    std::vector<TTabletBase*>{tablet},
                    std::vector<TTabletCell*>{},
                    std::vector<NTableClient::TLegacyOwningKey>{},
                    /*tabletCount*/ std::nullopt,
                    freeze,
                    /*skipFreezing*/ false,
                    /*correlationId*/ {},
                    /*expirationTime*/ TInstant::Zero(),
                    /*expirationTimeout*/ std::nullopt);
                continue;
            }

            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                if (auto* chunkList = table->GetChunkList(contentType)) {
                    const auto& chunkLists = chunkList->Children();
                    YT_VERIFY(allTablets.size() == chunkLists.size());
                }
            }

            tablet->Servant().SetCell(cell);
            InsertOrCrash(cell->Tablets(), tablet);
            objectManager->RefObject(cell);

            table->DiscountTabletStatistics(tablet->GetTabletStatistics());

            {
                auto newState = freeze ? ETabletState::FrozenMounting : ETabletState::Mounting;
                tablet->Servant().SetState(newState);
                tablet->SetState(newState);
            }

            tablet->SetInMemoryMode(table->GetInMemoryMode());
            resourceUsageDelta.TabletStaticMemory += tablet->GetTabletStaticMemorySize();

            cell->GossipStatistics().Local() += tablet->GetTabletStatistics();
            table->AccountTabletStatistics(tablet->GetTabletStatistics());

            const auto* context = GetCurrentMutationContext();
            tablet->Servant().SetMountRevision(context->GetVersion().ToRevision());
            tablet->SetSettingsRevision(context->GetVersion().ToRevision());
            tablet->SetWasForcefullyUnmounted(false);
            tablet->Servant().SetMountTime(context->GetTimestamp());

            switch (tablet->GetType()) {
                case EObjectType::Tablet:
                    DoMountTableTablet(
                        tablet->As<TTablet>(),
                        table->As<TTableNode>(),
                        std::get<TSerializedTableSettings>(serializedTableSettings),
                        cell,
                        freeze,
                        mountTimestamp);
                    break;

                case EObjectType::HunkTablet:
                    DoMountHunkTablet(
                        tablet->As<THunkTablet>(),
                        std::get<TSerializedHunkStorageSettings>(serializedTableSettings),
                        cell);
                    break;

                default:
                    YT_ABORT();
            }
        }

        UpdateResourceUsage(table, resourceUsageDelta);
    }

    TAvenueEndpointId GenerateAvenueEndpointId(TTabletBase* tablet)
    {
        ui32 hash = HashFromId(tablet->GetId());

        // Try to keep as many lower bits as possible.
        {
            ui32 salt = 0;
            ui32 upperBitMask = 0;
            ui32 saltOffset = 32;

            while (GeneratedAvenueIdHashes_.contains(hash)) {
                if (salt == upperBitMask) {
                    --saltOffset;
                    upperBitMask ^= 1u << saltOffset;
                }
                ++salt;
                hash = (hash & ~upperBitMask) ^ (salt << saltOffset);
            }
        }

        GeneratedAvenueIdHashes_.insert(hash);

        auto* mutationContext = GetCurrentMutationContext();

        auto result = MakeRegularId(
            EObjectType::AliceAvenueEndpoint,
            CellTagFromId(tablet->GetId()),
            mutationContext->GetVersion(),
            hash);

        mutationContext->CombineStateHash(result);

        return result;
    }

    template <class TReq>
    void MaybeSetTabletAvenueEndpointId(TTabletBase* tablet, TCellId cellId, TReq* mountReq)
    {
        // COMPAT(ifsmirnov): remove "Maybe" from method name when avenues are adopted.
        if (!GetDynamicConfig()->UseAvenues) {
            return;
        }

        auto masterEndpointId = GenerateAvenueEndpointId(tablet);
        auto nodeEndpointId = GetSiblingAvenueEndpointId(masterEndpointId);

        tablet->SetNodeAvenueEndpointId(nodeEndpointId);
        ToProto(mountReq->mutable_master_avenue_endpoint_id(), masterEndpointId);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->RegisterAvenueEndpoint(masterEndpointId, /*cookie*/ {});

        hiveManager->GetOrCreateCellMailbox(cellId);
        Bootstrap_->GetAvenueDirectory()->UpdateEndpoint(nodeEndpointId, cellId);
    }

    void DoMountTableTablet(
        TTablet* tablet,
        TTableNode* table,
        const TSerializedTableSettings& serializedTableSettings,
        TTabletCell* cell,
        bool freeze,
        TTimestamp mountTimestamp)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());

        auto tabletIndex = tablet->GetIndex();
        const auto& allTablets = table->Tablets();

        int preloadPendingStoreCount = 0;

        {
            auto req = PrepareMountRequestStem(tablet, serializedTableSettings);
            auto& reqReplicatable = *req.mutable_replicatable_content();
            // NB: essential content must be filled in PrepareMountRequestStem.

            MaybeSetTabletAvenueEndpointId(tablet, cell->GetId(), &req);

            reqReplicatable.set_retained_timestamp(tablet->GetRetainedTimestamp());
            if (!table->IsPhysicallySorted()) {
                reqReplicatable.set_trimmed_row_count(tablet->GetTrimmedRowCount());
            }

            req.set_mount_revision(tablet->Servant().GetMountRevision());
            req.set_freeze(freeze);

            if (table->IsReplicated()) {
                auto* replicatedTable = table->As<TReplicatedTableNode>();
                for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                    const auto* replicaInfo = tablet->GetReplicaInfo(replica);
                    PopulateTableReplicaDescriptor(req.add_replicas(), replica, *replicaInfo);
                }
            }

            if (table->GetReplicationCardId()) {
                if (tablet->ReplicationProgress().Segments.empty()) {
                    if (table->IsSorted()) {
                        tablet->ReplicationProgress().Segments.push_back({tablet->GetPivotKey(), MinTimestamp});
                        tablet->ReplicationProgress().UpperKey = tabletIndex + 1 == std::ssize(allTablets)
                            ? MaxKey()
                            : allTablets[tabletIndex + 1]->As<TTablet>()->GetPivotKey();
                    } else {
                        auto lower = tabletIndex == 0
                            ? EmptyKey()
                            : MakeUnversionedOwningRow(tablet->GetIndex());
                        auto upper = tabletIndex + 1 == std::ssize(allTablets)
                            ? MaxKey()
                            : MakeUnversionedOwningRow(tablet->GetIndex() + 1);
                        tablet->ReplicationProgress().Segments.push_back({std::move(lower), MinTimestamp});
                        tablet->ReplicationProgress().UpperKey = std::move(upper);
                    }
                }

                ToProto(req.mutable_replication_progress(), tablet->ReplicationProgress());
            }

            auto* chunkList = tablet->GetChunkList();
            const auto& chunkListStatistics = chunkList->Statistics();
            i64 startingRowIndex = chunkListStatistics.LogicalRowCount - chunkListStatistics.RowCount;

            std::vector<TChunkTree*> chunksOrViews;
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                auto* chunkList = tablet->GetChunkList(contentType);
                EnumerateStoresInChunkTree(chunkList, &chunksOrViews);
            }
            for (const auto* chunkOrView : chunksOrViews) {
                if (IsHunkChunk(tablet, chunkOrView)) {
                    FillHunkChunkDescriptor(chunkOrView->AsChunk(), reqReplicatable.add_hunk_chunks());
                } else {
                    FillStoreDescriptor(table, chunkOrView, reqReplicatable.add_stores(), &startingRowIndex);
                }
            }

            for (auto [transactionId, lock] : table->DynamicTableLocks()) {
                auto* protoLock = reqReplicatable.add_locks();
                ToProto(protoLock->mutable_transaction_id(), transactionId);
                protoLock->set_timestamp(lock.Timestamp);
            }

            if (!freeze && IsDynamicStoreReadEnabled(table, GetDynamicConfig())) {
                CreateAndAttachDynamicStores(tablet, &req);
            }

            if (table->GetInMemoryMode() != EInMemoryMode::None) {
                preloadPendingStoreCount = chunksOrViews.size();
            }

            auto* mountHint = req.mutable_mount_hint();
            ToProto(mountHint->mutable_eden_store_ids(), tablet->EdenStoreIds());

            // TODO(gritukan): Does it make sense for hunk chunk lists?
            i64 cumulativeDataWeight = 0;
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                cumulativeDataWeight += tablet->GetChunkList(contentType)->Statistics().LogicalDataWeight;
            }
            reqReplicatable.set_cumulative_data_weight(cumulativeDataWeight);

            YT_LOG_DEBUG("Mounting tablet (TableId: %v, TabletId: %v, CellId: %v, ChunkCount: %v, "
                "Atomicity: %v, CommitOrdering: %v, Freeze: %v, UpstreamReplicaId: %v, "
                "NodeEndpointId: %v)",
                table->GetId(),
                tablet->GetId(),
                cell->GetId(),
                chunksOrViews.size(),
                table->GetAtomicity(),
                table->GetCommitOrdering(),
                freeze,
                table->GetUpstreamReplicaId(),
                tablet->GetNodeEndpointId());

            if (!tablet->IsMountedWithAvenue()) {
                ++NonAvenueTabletCount_;
            }

            hiveManager->PostMessage(mailbox, req);
        }

        {
            TTabletStatistics delta;

            // The latter should be zero, but we observe the formalities.
            delta.PreloadPendingStoreCount = preloadPendingStoreCount -
                tablet->NodeStatistics().preload_pending_store_count();
            table->AccountTabletStatisticsDelta(delta);

            // COMPAT(ifsmirnov)
            if (GetDynamicConfig()->AccumulatePreloadPendingStoreCountCorrectly) {
                cell->GossipStatistics().Local() += delta;
            }

            tablet->NodeStatistics().set_preload_pending_store_count(preloadPendingStoreCount);
        }

        for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
            auto* replica = it->first;
            auto& replicaInfo = it->second;
            switch (replica->GetState()) {
                case ETableReplicaState::Enabled:
                case ETableReplicaState::Enabling: {
                    TReqAlterTableReplica req;
                    ToProto(req.mutable_tablet_id(), tablet->GetId());
                    ToProto(req.mutable_replica_id(), replica->GetId());
                    req.set_enabled(true);
                    hiveManager->PostMessage(mailbox, req);

                    if (replica->GetState() == ETableReplicaState::Enabled) {
                        StartReplicaTransition(tablet, replica, &replicaInfo, ETableReplicaState::Enabling);
                    }
                    break;
                }

                case ETableReplicaState::Disabled:
                case ETableReplicaState::Disabling:
                    replicaInfo.SetState(ETableReplicaState::Disabled);
                    break;

                default:
                    YT_ABORT();
            }
        }

        if (mountTimestamp != NullTimestamp) {
            tablet->NodeStatistics().set_unflushed_timestamp(mountTimestamp);
        }
    }

    void DoMountHunkTablet(
        THunkTablet* tablet,
        const TSerializedHunkStorageSettings& serializedSettings,
        TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        TReqMountHunkTablet request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->Servant().GetMountRevision());

        MaybeSetTabletAvenueEndpointId(tablet, cell->GetId(), &request);

        FillHunkStorageSettings(&request, serializedSettings);

        auto chunks = EnumerateChunksInChunkTree(tablet->GetChunkList());
        for (auto* chunk : chunks) {
            ToProto(request.add_store_ids(), chunk->GetId());
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, request);

        YT_LOG_DEBUG(
            "Mounting hunk tablet (TabletId: %v, CellId: %v, NodeEndpointId: %v)",
            tablet->GetId(),
            cell->GetId(),
            tablet->GetNodeEndpointId());

        if (!tablet->IsMountedWithAvenue()) {
            ++NonAvenueTabletCount_;
        }
    }

    void AllocateAuxiliaryServant(
        TTablet* tablet,
        TTabletCell* cell,
        const TSerializedTableSettings& serializedTableSettings)
    {
        YT_VERIFY(tablet->GetState() == ETabletState::Mounted);

        auto revision = GetCurrentMutationContext()->GetVersion().ToRevision();
        auto avenueEndpointId = GenerateAvenueEndpointId(tablet);

        YT_LOG_DEBUG("Mounting tablet to auxiliary cell "
            "(TableId: %v, TabletId: %v, CellId: %v, "
            "MountRevision: %x, AuxiliaryCellId: %v, AuxiliaryMountRevision: %x, "
            "TabletwiseAvenueEndpointId: %v)",
            tablet->GetTable()->GetId(),
            tablet->GetId(),
            tablet->GetCell()->GetId(),
            tablet->Servant().GetMountRevision(),
            cell->GetId(),
            revision,
            avenueEndpointId);

        YT_VERIFY(!tablet->GetTabletwiseAvenueEndpointId());
        tablet->SetTabletwiseAvenueEndpointId(avenueEndpointId);

        auto& auxiliaryServant = tablet->AuxiliaryServant();
        YT_VERIFY(!auxiliaryServant);

        auxiliaryServant.SetCell(cell);
        auxiliaryServant.SetState(ETabletState::Mounting);
        auxiliaryServant.SetMountRevision(revision);
        auxiliaryServant.SetMountTime(GetCurrentMutationContext()->GetTimestamp());

        auxiliaryServant.SetMovementRole(NTabletNode::ESmoothMovementRole::Target);
        auxiliaryServant.SetMovementStage(NTabletNode::ESmoothMovementStage::None);

        auto& mainServant = tablet->Servant();
        mainServant.SetMovementRole(NTabletNode::ESmoothMovementRole::Source);
        mainServant.SetMovementStage(NTabletNode::ESmoothMovementStage::None);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(cell);
        InsertOrCrash(cell->Tablets(), tablet);

        tablet->AuxiliaryNodeStatistics() = tablet->NodeStatistics();
        cell->GossipStatistics().Local() += tablet->GetTabletStatistics(/*fromAuxiliaryCell*/ true);

        auto req = PrepareMountRequestStem(tablet, serializedTableSettings);
        ToProto(req.mutable_movement_source_cell_id(), tablet->GetCell()->GetId());
        req.set_mount_revision(auxiliaryServant.GetMountRevision());
        req.set_movement_source_mount_revision(tablet->Servant().GetMountRevision());
        ToProto(req.mutable_movement_source_avenue_endpoint_id(), avenueEndpointId);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, req);
    }

    void DoFreezeTablet(TTabletBase* tablet)
    {
        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* cell = tablet->GetCell();
        auto state = tablet->GetState();
        YT_VERIFY(state == ETabletState::Mounted ||
            state == ETabletState::FrozenMounting ||
            state == ETabletState::Frozen ||
            state == ETabletState::Freezing);

        if (tablet->GetState() == ETabletState::Mounted) {
            YT_LOG_DEBUG("Freezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                tablet->GetOwner()->GetId(),
                tablet->GetId(),
                cell->GetId());

            tablet->SetState(ETabletState::Freezing);

            tablet->Servant().SetState(ETabletState::Freezing);
            if (auto& auxiliaryServant = tablet->AuxiliaryServant()) {
                YT_LOG_DEBUG("Freezing auxiliary tablet servant (TabletId: %v)",
                    tablet->GetId());
                auxiliaryServant.SetState(ETabletState::Freezing);
            }

            TReqFreezeTablet request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            hiveManager->PostMessage(mailbox, request);
        }
    }

    void DoUnfreezeTablet(TTabletBase* tablet)
    {
        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);
        auto* table = tablet->As<TTablet>()->GetTable();

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* cell = tablet->GetCell();
        auto state = tablet->GetState();
        YT_VERIFY(state == ETabletState::Mounted ||
            state == ETabletState::Frozen ||
            state == ETabletState::Unfreezing);

        if (tablet->GetState() == ETabletState::Frozen) {
            YT_LOG_DEBUG("Unfreezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                table->GetId(),
                tablet->GetId(),
                cell->GetId());

            tablet->Servant().SetState(ETabletState::Unfreezing);
            YT_VERIFY(!tablet->AuxiliaryServant());
            tablet->SetState(ETabletState::Unfreezing);

            TReqUnfreezeTablet request;

            if (IsDynamicStoreReadEnabled(table, GetDynamicConfig())) {
                CreateAndAttachDynamicStores(tablet->As<TTablet>(), &request);
            }

            ToProto(request.mutable_tablet_id(), tablet->GetId());

            auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
            hiveManager->PostMessage(mailbox, request);
        }
    }

    void HydraOnTabletLocked(NProto::TRspLockTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);

        auto* tabletBase = FindTablet(tabletId);
        if (!IsObjectAlive(tabletBase)) {
            return;
        }
        auto* tablet = tabletBase->As<TTablet>();
        auto* table = tablet->GetTable();

        auto transactionIds = FromProto<std::vector<TTransactionId>>(response->transaction_ids());

        for (auto transactionId : transactionIds) {
            if (auto it = tablet->UnconfirmedDynamicTableLocks().find(transactionId)) {
                tablet->UnconfirmedDynamicTableLocks().erase(it);
                table->ConfirmDynamicTableLock(transactionId);

                int pendingTabletCount = 0;
                if (auto it = table->DynamicTableLocks().find(transactionId)) {
                    pendingTabletCount = it->second.PendingTabletCount;
                }

                YT_LOG_DEBUG("Confirmed tablet lock (TabletId: %v, TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
                    tabletId,
                    table->GetId(),
                    transactionId,
                    pendingTabletCount);
            }
        }
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();

        for (auto tableIt : GetSortedIterators(
            transaction->LockedDynamicTables(),
            TObjectIdComparer()))
        {
            auto* table = *tableIt;
            if (!IsObjectAlive(table)) {
                continue;
            }

            for (auto* tablet : table->Tablets()) {
                if (tablet->GetState() == ETabletState::Unmounted) {
                    continue;
                }

                tablet->As<TTablet>()->UnconfirmedDynamicTableLocks().erase(transaction->GetId());

                auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
                TReqUnlockTablet req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                ToProto(req.mutable_transaction_id(), transaction->GetId());
                req.set_mount_revision(tablet->Servant().GetMountRevision());
                // Aborted bulk insert should not conflict with concurrent tablet transactions.
                req.set_commit_timestamp(static_cast<i64>(MinTimestamp));

                hiveManager->PostMessage(mailbox, req);
            }

            table->RemoveDynamicTableLock(transaction->GetId());
        }

        transaction->LockedDynamicTables().clear();
    }

    void DoRemount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        auto currentRevision = GetCurrentMutationContext()->GetVersion().ToRevision();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];

            if (tablet->GetState() == ETabletState::Unmounted) {
                continue;
            }

            if (tablet->GetSettingsRevision() < table->GetSettingsUpdateRevision()) {
                int newCount = table->GetRemountNeededTabletCount() - 1;
                YT_ASSERT(newCount >= 0);
                table->SetRemountNeededTabletCount(newCount);
            }

            tablet->SetSettingsRevision(currentRevision);
        }

        if (IsTableType(table->GetType())) {
            return DoRemountTable(
                table->As<TTableNode>(),
                firstTabletIndex,
                lastTabletIndex);
        } else {
            YT_ABORT();
        }
    }

    void DoRemountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        auto tableSettings = GetTableSettings(
            table,
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetChunkManager(),
            GetDynamicConfig());
        auto serializedTableSettings = SerializeTableSettings(tableSettings);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();
            auto state = tablet->GetState();

            if (state != ETabletState::Unmounted) {
                YT_LOG_DEBUG("Remounting tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                YT_VERIFY(tablet->GetInMemoryMode() == tableSettings.EffectiveMountConfig->InMemoryMode);

                const auto& hiveManager = Bootstrap_->GetHiveManager();

                TReqRemountTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                FillTableSettings(&request, serializedTableSettings);

                auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
                hiveManager->PostMessage(mailbox, request);
            }
        }
    }

    int DoReshard(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts)
    {
        if (IsTableType(table->GetType())) {
            return DoReshardTable(
                table->As<TTableNode>(),
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                pivotKeys,
                trimmedRowCounts);
        } else if (table->GetType() == EObjectType::HunkStorage) {
            return DoReshardHunkStorage(
                table->As<THunkStorageNode>(),
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount);
        } else {
            YT_ABORT();
        }
    }

    int DoReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts)
    {
        if (!pivotKeys.empty() || !table->IsPhysicallySorted()) {
            ReshardTableImpl(
                table,
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                pivotKeys,
                trimmedRowCounts);
            return newTabletCount;
        } else {
            auto newPivotKeys = CalculatePivotKeys(table, firstTabletIndex, lastTabletIndex, newTabletCount);
            newTabletCount = std::ssize(newPivotKeys);
            ReshardTableImpl(
                table,
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                newPivotKeys,
                /*trimmedRowCounts*/ {});
            return newTabletCount;
        }
    }

    int DoReshardHunkStorage(
        THunkStorageNode* hunkStorage,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(hunkStorage->IsTrunk());
        YT_VERIFY(!hunkStorage->IsExternal());

        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto& tablets = hunkStorage->MutableTablets();

        ParseTabletRange(hunkStorage, &firstTabletIndex, &lastTabletIndex);

        YT_LOG_DEBUG(
            "Resharding hunk storage (HunkStorageId: %v, FirstTabletIndex: %v, LastTabletIndex: %v, TabletCount: %v)",
            hunkStorage->GetId(),
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount);

        auto resourceUsageBefore = hunkStorage->GetTabletResourceUsage();

        // Create new tablets.
        std::vector<THunkTablet*> newTablets;
        newTablets.reserve(newTabletCount);
        for (int index = 0; index < newTabletCount; ++index) {
            auto* newTablet = CreateTablet(hunkStorage, EObjectType::HunkTablet)->As<THunkTablet>();
            newTablets.push_back(newTablet);
        }

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            hunkStorage->DiscountTabletStatistics(tablet->GetTabletStatistics());
            tablet->SetOwner(nullptr);

            objectManager->UnrefObject(tablet);
        }

        // Replace old tablets with new.
        tablets.erase(tablets.begin() + firstTabletIndex, tablets.begin() + (lastTabletIndex + 1));
        tablets.insert(tablets.begin() + firstTabletIndex, newTablets.begin(), newTablets.end());

        // Update tablet indices.
        for (int index = 0; index < std::ssize(tablets); ++index) {
            auto* tablet = tablets[index];
            tablet->SetIndex(index);
        }

        TabletChunkManager_->ReshardHunkStorage(
            hunkStorage,
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount);

        // Account new tablet statistics.
        for (auto* newTablet : newTablets) {
            hunkStorage->AccountTabletStatistics(newTablet->GetTabletStatistics());
        }

        auto resourceUsageDelta = hunkStorage->GetTabletResourceUsage() - resourceUsageBefore;
        UpdateResourceUsage(hunkStorage, resourceUsageDelta);

        return newTabletCount;
    }

    void ReshardTableImpl(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());
        YT_VERIFY(!table->IsExternal());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        auto resourceUsageBefore = table->GetTabletResourceUsage();

        auto& tablets = table->MutableTablets();
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            YT_VERIFY(tablets.size() == table->GetChunkList(contentType)->Children().size());
        }

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;

        YT_LOG_DEBUG("Resharding table (TableId: %v, FirstTabletIndex: %v, LastTabletIndex: %v, "
            "TabletCount %v, PivotKeys: %v)",
            table->GetId(),
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            pivotKeys);

        // Calculate retained timestamp for removed tablets.
        auto retainedTimestamp = MinTimestamp;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            retainedTimestamp = std::max(retainedTimestamp, tablets[index]->As<TTablet>()->GetRetainedTimestamp());
        }

        // Save eden stores of removed tablets.
        // NB. Since new chunk views may be created over existing chunks, we mark underlying
        // chunks themselves as eden. It gives different result only in rare cases when a chunk
        // under a chunk view was in eden in some tablet but not in the adjacent tablet.
        THashSet<TStoreId> oldEdenStoreIds;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            for (auto storeId : tablets[index]->As<TTablet>()->EdenStoreIds()) {
                if (auto chunkView = chunkManager->FindChunkView(storeId)) {
                    oldEdenStoreIds.insert(chunkView->GetUnderlyingTree()->GetId());
                } else {
                    oldEdenStoreIds.insert(storeId);
                }
            }
        }

        // Create new tablets.
        std::vector<TTablet*> newTablets;
        for (int index = 0; index < newTabletCount; ++index) {
            auto* newTablet = CreateTablet(table, EObjectType::Tablet)->As<TTablet>();
            auto* oldTablet = index < oldTabletCount ? tablets[index + firstTabletIndex]->As<TTablet>() : nullptr;
            if (table->IsSorted()) {
                newTablet->SetPivotKey(pivotKeys[index]);
            } else {
                if (oldTablet) {
                    newTablet->SetTrimmedRowCount(oldTablet->GetTrimmedRowCount());
                } else if (!trimmedRowCounts.empty()) {
                    int relativeIndex = index - oldTabletCount;
                    YT_VERIFY(relativeIndex < ssize(trimmedRowCounts));
                    newTablet->SetTrimmedRowCount(trimmedRowCounts[relativeIndex]);
                }
            }
            newTablet->SetRetainedTimestamp(retainedTimestamp);
            newTablets.push_back(newTablet);

            if (table->IsReplicated()) {
                const auto* replicatedTable = table->As<TReplicatedTableNode>();
                for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                    YT_VERIFY(newTablet->Replicas().emplace(replica, TTableReplicaInfo()).second);
                }
            }
        }

        // Copy replication progress.
        {
            std::vector<TReplicationProgress> progresses;
            std::vector<TLegacyKey> pivotKeys;
            std::vector<TLegacyOwningKey> buffer;
            bool nonEmpty = false;

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = tablets[index]->As<TTablet>();
                if (!tablet->ReplicationProgress().Segments.empty()) {
                    nonEmpty = true;
                }
                progresses.push_back(std::move(tablet->ReplicationProgress()));
                pivotKeys.push_back(GetTabletReplicationProgressPivotKey(tablet, index, &buffer));
            }

            if (nonEmpty) {
                auto upperKey = lastTabletIndex + 1 < std::ssize(tablets)
                    ? tablets[lastTabletIndex + 1]->As<TTablet>()->GetPivotKey().Get()
                    : MaxKey().Get();
                auto progress = NChaosClient::GatherReplicationProgress(
                    std::move(progresses),
                    pivotKeys,
                    upperKey);
                pivotKeys.clear();
                for (int index = 0; index < std::ssize(newTablets); ++index) {
                    auto* tablet = newTablets[index];
                    pivotKeys.push_back(GetTabletReplicationProgressPivotKey(tablet, firstTabletIndex + index, &buffer));
                }

                auto newProgresses = NChaosClient::ScatterReplicationProgress(
                    std::move(progress),
                    pivotKeys,
                    upperKey);
                for (int index = 0; index < std::ssize(newTablets); ++index) {
                    auto* tablet = newTablets[index];
                    tablet->ReplicationProgress() = std::move(newProgresses[index]);
                }
            }
        }

        std::vector<TLegacyOwningKey> oldPivotKeys;

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index]->As<TTablet>();
            if (table->IsPhysicallySorted()) {
                oldPivotKeys.push_back(tablet->GetPivotKey());
            }
            table->DiscountTabletStatistics(tablet->GetTabletStatistics());
            tablet->SetOwner(nullptr);
            objectManager->UnrefObject(tablet);
        }

        if (table->IsPhysicallySorted()) {
            if (lastTabletIndex + 1 < std::ssize(tablets)) {
                oldPivotKeys.push_back(tablets[lastTabletIndex + 1]->As<TTablet>()->GetPivotKey());
            } else {
                oldPivotKeys.push_back(MaxKey());
            }
        }

        // NB: Evaluation order is important here, consider the case lastTabletIndex == -1.
        tablets.erase(tablets.begin() + firstTabletIndex, tablets.begin() + (lastTabletIndex + 1));
        tablets.insert(tablets.begin() + firstTabletIndex, newTablets.begin(), newTablets.end());
        // Update all indexes.
        for (int index = 0; index < static_cast<int>(tablets.size()); ++index) {
            auto* tablet = tablets[index];
            tablet->SetIndex(index);
        }

        TabletChunkManager_->ReshardTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            oldPivotKeys,
            pivotKeys,
            oldEdenStoreIds);

        // Account new tablet statistics.
        for (auto* newTablet : newTablets) {
            table->AccountTabletStatistics(newTablet->GetTabletStatistics());
        }

        // TODO(savrus) Looks like this is unnecessary. Need to check.
        RecomputeTableSnapshotStatistics(table);

        auto resourceUsageDelta = table->GetTabletResourceUsage() - resourceUsageBefore;
        UpdateResourceUsage(table, resourceUsageDelta);

        table->RecomputeTabletMasterMemoryUsage();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
    }

    void RecomputeTableSnapshotStatistics(TTableNode* table)
    {
        table->SnapshotStatistics() = {};
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            table->SnapshotStatistics() += table->GetChunkList(contentType)->Statistics().ToDataStatistics();
        }
    }

    void SetSyncTabletActionsKeepalive(const std::vector<TTabletActionId>& actionIds)
    {
        for (auto actionId : actionIds) {
            auto* action = GetTabletAction(actionId);
            action->SetExpirationTimeout(DefaultSyncTabletActionKeepalivePeriod);
        }
    }

    const TDynamicTabletManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
    {
        const auto& config = GetDynamicConfig();

        {
            const auto& gossipConfig = config->MulticellGossip;

            if (TabletCellStatisticsGossipExecutor_) {
                TabletCellStatisticsGossipExecutor_->SetPeriod(gossipConfig->TabletCellStatisticsGossipPeriod);
            }
            if (BundleResourceUsageGossipExecutor_) {
                BundleResourceUsageGossipExecutor_->SetPeriod(gossipConfig->BundleResourceUsageGossipPeriod);
            }
            EnableUpdateStatisticsOnHeartbeat_ = gossipConfig->EnableUpdateStatisticsOnHeartbeat;
        }

        TabletCellDecommissioner_->Reconfigure(config->TabletCellDecommissioner);
        TabletActionManager_->Reconfigure(config->TabletActionManager);
        TabletBalancer_->Reconfigure(config->TabletBalancer);

        if (ProfilingExecutor_) {
            ProfilingExecutor_->SetPeriod(config->ProfilingPeriod);
        }

        // COMPAT(ifsmirnov)
        if (!oldConfig->TabletManager->AccumulatePreloadPendingStoreCountCorrectly &&
            config->AccumulatePreloadPendingStoreCountCorrectly)
        {
            YT_LOG_DEBUG("Recomputing statistics of all tablet cells");
            RecomputeAllTabletCellStatistics();
        }
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
        TableReplicaMap_.SaveKeys(context);
        TabletActionMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        TabletMap_.SaveValues(context);
        TableReplicaMap_.SaveValues(context);
        TabletActionMap_.SaveValues(context);

        Save(context, MountConfigKeysFromNodes_);
        Save(context, LocalMountConfigKeys_);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
        TableReplicaMap_.LoadKeys(context);
        TabletActionMap_.LoadKeys(context);

        NeedResetErrorCountOfUnmountedTablets_ = context.GetVersion() < EMasterReign::ResetErrorCountOfUnmountedTablets;
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadValues(context);
        TableReplicaMap_.LoadValues(context);
        TabletActionMap_.LoadValues(context);

        Load(context, MountConfigKeysFromNodes_);
        Load(context, LocalMountConfigKeys_);

        // Update mount config keys whenever the reign changes.
        FillMountConfigKeys_ = context.GetVersion() != static_cast<EMasterReign>(GetCurrentReign());
    }

    void RecomputeHunkResourceUsage()
    {
        for (const auto& [id, tabletBase] : Tablets()) {
            if (tabletBase->GetType() != EObjectType::Tablet) {
                continue;
            }
            auto* tablet = tabletBase->As<TTablet>();

            auto* table = tablet->GetTable();
            if (!table) {
                continue;
            }

            YT_LOG_DEBUG("Recomputing hunk resource usage (TabletId: %v, TableId: %v, "
                "HunkUncompressedDataSize: %v, HunkCompressedDataSize: %v)",
                tablet->GetId(),
                table->GetId(),
                tablet->GetHunkUncompressedDataSize(),
                tablet->GetHunkCompressedDataSize());

            i64 memoryDelta = 0;
            switch (tablet->GetInMemoryMode()) {
                case EInMemoryMode::Uncompressed:
                    memoryDelta = -tablet->GetHunkUncompressedDataSize();
                    break;
                case EInMemoryMode::Compressed:
                    memoryDelta = -tablet->GetHunkCompressedDataSize();
                    break;
                case EInMemoryMode::None:
                    memoryDelta = 0;
                    break;
                default:
                    YT_ABORT();
            }

            TTabletStatistics statisticsDelta;
            statisticsDelta.HunkUncompressedDataSize = tablet->GetHunkUncompressedDataSize();
            statisticsDelta.HunkCompressedDataSize = tablet->GetHunkCompressedDataSize();
            statisticsDelta.MemorySize = memoryDelta;
            table->AccountTabletStatisticsDelta(statisticsDelta);

            auto resourcesDelta = TTabletResources{}
                .SetTabletStaticMemory(memoryDelta);
            UpdateResourceUsage(table, resourcesDelta);
        }
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        RecomputeAggregateTabletStatistics_ = false;
        RecomputeHunkResourceUsage_ = false;
        FillMountConfigKeys_ = false;
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        if (RecomputeAggregateTabletStatistics_) {
            THashSet<TTabletOwnerBase*> resetTables;
            for (auto [id, tablet] : Tablets()) {
                if (auto* table = tablet->GetOwner()) {
                    if (resetTables.insert(table).second) {
                        table->ResetTabletStatistics();
                    }
                    table->AccountTabletStatistics(tablet->GetTabletStatistics());
                }
            }
        }

        InitBuiltins();

        const auto& avenueDirectory = Bootstrap_->GetAvenueDirectory();
        for (auto [id, tablet] : Tablets()) {
            if (tablet->IsMountedWithAvenue()) {
                YT_VERIFY(tablet->Servant().GetCell());
                avenueDirectory->UpdateEndpoint(
                    tablet->GetNodeEndpointId(),
                    tablet->Servant().GetCell()->GetId());
            }
        }

        if (FillMountConfigKeys_) {
            auto mountConfig = New<NTabletNode::TTableMountConfig>();
            LocalMountConfigKeys_ = mountConfig->GetRegisteredKeys();
        }

        NonAvenueTabletCount_ = 0;
        for (auto [id, tablet] : Tablets()) {
            if (tablet->GetState() != ETabletState::Unmounted && !tablet->IsMountedWithAvenue()) {
                ++NonAvenueTabletCount_;
            }
        }

        if (NeedResetErrorCountOfUnmountedTablets_) {
            for (auto [id, tabletBase] : Tablets()) {
                if (tabletBase->GetState() == ETabletState::Unmounted) {
                    tabletBase->SetTabletErrorCount(0);
                    if (tabletBase->GetType() == EObjectType::Tablet) {
                        auto* tablet = tabletBase->As<TTablet>();
                        tablet->SetReplicationErrorCount(0);
                    }
                }
            }
        }
    }

    void OnAfterCellManagerSnapshotLoaded()
    {
        InitBuiltins();

        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        for (auto* cellBase : cellManager->Cells(ECellarType::Tablet)) {
            YT_VERIFY(cellBase->GetType() == EObjectType::TabletCell);
            auto* cell = cellBase->As<TTabletCell>();
            cell->GossipStatistics().Initialize(Bootstrap_);
        }

        for (auto* bundleBase : cellManager->CellBundles(ECellarType::Tablet)) {
            YT_VERIFY(bundleBase->GetType() == EObjectType::TabletCellBundle);
            auto* bundle = bundleBase->As<TTabletCellBundle>();
            bundle->ResourceUsage().Initialize(Bootstrap_);
        }

        // COMPAT(ifsmirnov)
        if (RecomputeHunkResourceUsage_) {
            RecomputeHunkResourceUsage();
        }

        for (auto [actionId, action] : TabletActionMap_) {
            // NB: Process non-alive objects to pair with DestroyTabletAction.
            auto bundle = action->GetTabletCellBundle();
            if (!bundle) {
                continue;
            }

            bundle->TabletActions().insert(action);
            if (!action->IsFinished()) {
                bundle->IncreaseActiveTabletActionCount();
            }
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TabletMap_.Clear();
        TableReplicaMap_.Clear();
        TabletActionMap_.Clear();

        DefaultTabletCellBundle_ = nullptr;
        SequoiaTabletCellBundle_ = nullptr;
        NonAvenueTabletCount_ = 0;
    }

    void SetZeroState() override
    {
        InitBuiltins();

        auto mountConfig = New<NTabletNode::TTableMountConfig>();
        LocalMountConfigKeys_ = mountConfig->GetRegisteredKeys();
    }

    template <class T>
    T* GetBuiltin(T*& builtin)
    {
        if (!builtin) {
            InitBuiltins();
        }
        YT_VERIFY(builtin);
        return builtin;
    }

    void InitBuiltins()
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        // Cell bundles

        // default
        if (EnsureBuiltinCellBundleInitialized(DefaultTabletCellBundle_, DefaultTabletCellBundleId_, DefaultTabletCellBundleName)) {
            DefaultTabletCellBundle_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
            DefaultTabletCellBundle_->ResourceLimits().TabletCount = 100'000;
            DefaultTabletCellBundle_->ResourceLimits().TabletStaticMemory = 1_TB;
        }

        // sequoia
        if (EnsureBuiltinCellBundleInitialized(SequoiaTabletCellBundle_, SequoiaTabletCellBundleId_, SequoiaTabletCellBundleName)) {
            SequoiaTabletCellBundle_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
            SequoiaTabletCellBundle_->ResourceLimits().TabletCount = 100'000;
            SequoiaTabletCellBundle_->ResourceLimits().TabletStaticMemory = 1_TB;

            auto options = SequoiaTabletCellBundle_->GetOptions();
            options->ChangelogAccount = NSecurityClient::SequoiaAccountName;
            options->SnapshotAccount = NSecurityClient::SequoiaAccountName;
            SequoiaTabletCellBundle_->SetOptions(std::move(options));
        }
    }

    bool EnsureBuiltinCellBundleInitialized(TTabletCellBundle*& cellBundle, TTabletCellBundleId id, const TString& name)
    {
        if (cellBundle) {
            return false;
        }
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        if (auto* bundle = cellManager->FindCellBundle(id)) {
            YT_VERIFY(bundle->GetType() == EObjectType::TabletCellBundle);
            cellBundle = bundle->As<TTabletCellBundle>();
            return false;
        }
        auto options = New<TTabletCellOptions>();
        options->ChangelogAccount = DefaultStoreAccountName;
        options->SnapshotAccount = DefaultStoreAccountName;

        auto holder = TPoolAllocator::New<TTabletCellBundle>(id);
        holder->ResourceUsage().Initialize(Bootstrap_);
        cellBundle = cellManager->CreateCellBundle(name, std::move(holder), std::move(options))
            ->As<TTabletCellBundle>();
        return true;
    }

    void OnTabletCellStatisticsGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        YT_LOG_INFO("Sending tablet cell statistics gossip message");

        NProto::TReqSetTabletCellStatistics request;
        request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto* cellBase : GetValuesSortedByKey(cellManager->Cells(ECellarType::Tablet))) {
            if (!IsObjectAlive(cellBase)) {
                continue;
            }

            YT_VERIFY(cellBase->GetType() == EObjectType::TabletCell);
            auto* cell = cellBase->As<TTabletCell>();
            auto* entry = request.add_entries();
            ToProto(entry->mutable_tablet_cell_id(), cell->GetId());

            if (multicellManager->IsPrimaryMaster()) {
                ToProto(entry->mutable_statistics(), cell->GossipStatistics().Cluster());
            } else {
                ToProto(entry->mutable_statistics(), cell->GossipStatistics().Local());
            }
        }

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToPrimaryMaster(request, false);
        }
    }

    void HydraSetTabletCellStatistics(NProto::TReqSetTabletCellStatistics* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR("Received tablet cell statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO("Received tablet cell statistics gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto cellId = FromProto<TTabletCellId>(entry.tablet_cell_id());
            auto* cell = FindTabletCell(cellId);
            if (!IsObjectAlive(cell)) {
                continue;
            }

            auto newStatistics = FromProto<TTabletCellStatistics>(entry.statistics());

            if (multicellManager->IsPrimaryMaster()) {
                *cell->GossipStatistics().Remote(cellTag) = newStatistics;
            } else {
                cell->GossipStatistics().Cluster() = newStatistics;
            }
        }
    }

    void OnTabletNodeHeartbeat(
        TNode* node,
        NTabletNodeTrackerClient::NProto::TReqHeartbeat* request,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat* /*response*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        const auto& tableManager = Bootstrap_->GetTableManager();

        // Copy tablet statistics, update performance counters and table replica statistics.
        auto now = TInstant::Now();

        for (auto& tabletInfo : request->tablets()) {
            auto tabletId = FromProto<TTabletId>(tabletInfo.tablet_id());
            auto mountRevision = tabletInfo.mount_revision();

            auto* tabletBase = FindTablet(tabletId)->As<TTablet>();
            if (!IsObjectAlive(tabletBase)) {
                continue;
            }

            auto* servant = tabletBase->FindServant(mountRevision);
            if (!servant || servant->GetState() == ETabletState::Unmounted) {
                continue;
            }

            YT_VERIFY(tabletBase->GetType() == EObjectType::Tablet);
            auto* tablet = tabletBase->As<TTablet>();

            auto* cell = servant->GetCell();
            if (!IsObjectAlive(cell)){
                continue;
            }

            auto* slot = node->FindCellSlot(cell);
            if (!slot || (slot->PeerState != EPeerState::Leading && slot->PeerState != EPeerState::LeaderRecovery)) {
                continue;
            }

            auto tabletStatistics = tablet->GetTabletStatistics(servant->IsAuxiliary());
            if (!servant->IsAuxiliary()) {
                tablet->GetTable()->DiscountTabletStatistics(tabletStatistics);
            }
            cell->GossipStatistics().Local() -= tabletStatistics;

            if (servant->IsAuxiliary()) {
                tablet->AuxiliaryNodeStatistics() = tabletInfo.statistics();
            } else {
                tablet->NodeStatistics() = tabletInfo.statistics();
            }

            tabletStatistics = tablet->GetTabletStatistics(servant->IsAuxiliary());
            if (!servant->IsAuxiliary()) {
                tablet->GetTable()->AccountTabletStatistics(tabletStatistics);
            }
            cell->GossipStatistics().Local() += tabletStatistics;

            auto* table = tablet->GetTable();
            if (table) {
                table->SetLastCommitTimestamp(std::max(
                    table->GetLastCommitTimestamp(),
                    tablet->NodeStatistics().last_commit_timestamp()));

                if (tablet->NodeStatistics().has_modification_time()) {
                    table->SetModificationTime(std::max(
                        table->GetModificationTime(),
                        FromProto<TInstant>(tablet->NodeStatistics().modification_time())));
                }

                if (tablet->NodeStatistics().has_access_time()) {
                    table->SetAccessTime(std::max(
                        table->GetAccessTime(),
                        FromProto<TInstant>(tablet->NodeStatistics().access_time())));
                }

                if (EnableUpdateStatisticsOnHeartbeat_) {
                    tableManager->ScheduleStatisticsUpdate(table, true, false);
                }
            }

            #define XX(name, Name) tablet->PerformanceCounters().Name.Update( \
                tabletInfo.performance_counters().name ## _count(), \
                now);
            ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
            #undef XX

            tablet->SetTabletErrorCount(tabletInfo.error_count());

            int replicationErrorCount = 0;
            for (const auto& protoReplicaInfo : tabletInfo.replicas()) {
                auto replicaId = FromProto<TTableReplicaId>(protoReplicaInfo.replica_id());
                auto* replica = FindTableReplica(replicaId);
                if (!replica) {
                    continue;
                }

                auto* replicaInfo = tablet->FindReplicaInfo(replica);
                if (!replicaInfo) {
                    continue;
                }

                PopulateTableReplicaInfoFromStatistics(replicaInfo, protoReplicaInfo.statistics());

                replicaInfo->SetHasError(protoReplicaInfo.has_error());
                replicationErrorCount += protoReplicaInfo.has_error();
            }
            tablet->SetReplicationErrorCount(replicationErrorCount);

            TabletBalancer_->OnTabletHeartbeat(tablet);
        }

        TabletNodeHeartbeatCounter_.Add(timer.GetElapsedTime());
    }

    void HydraUpdateUpstreamTabletState(NProto::TReqUpdateUpstreamTabletState* request)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        auto transactionId = FromProto<TTransactionId>(request->last_mount_transaction_id());
        auto actualState = request->has_actual_tablet_state()
            ? std::optional(FromProto<ETabletState>(request->actual_tablet_state()))
            : std::nullopt;
        auto expectedState = request->has_expected_tablet_state()
            ? std::optional(FromProto<ETabletState>(request->expected_tablet_state()))
            : std::nullopt;

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
        if (!IsObjectAlive(node)) {
            return;
        }

        YT_VERIFY(IsTabletOwnerType(node->GetType()));
        auto* tabletOwner = node->As<TTabletOwnerBase>();

        YT_LOG_DEBUG("Received update upstream tablet state request "
            "(TableId: %v, ActualTabletState: %v, ExpectedTabletState: %v, ExpectedLastMountTransactionId: %v, ActualLastMountTransactionId: %v)",
            tableId,
            actualState,
            expectedState,
            transactionId,
            tabletOwner->GetLastMountTransactionId());

        if (actualState) {
            tabletOwner->SetActualTabletState(*actualState);
        }

        if (transactionId == tabletOwner->GetLastMountTransactionId()) {
            if (expectedState) {
                tabletOwner->SetExpectedTabletState(*expectedState);
            }
            tabletOwner->SetLastMountTransactionId(TTransactionId());
        }
    }

    void HydraUpdateTabletState(NProto::TReqUpdateTabletState* request)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto tableId = FromProto<TTableId>(request->table_id());
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
        if (!IsObjectAlive(node)) {
            return;
        }

        YT_VERIFY(IsTabletOwnerType(node->GetType()));
        auto* tabletOwner = node->As<TTabletOwnerBase>();
        auto transactionId = FromProto<TTransactionId>(request->last_mount_transaction_id());
        tabletOwner->SetPrimaryLastMountTransactionId(transactionId);

        YT_LOG_DEBUG(
            "Table tablet state check request received (TableId: %v, LastMountTransactionId: %v, PrimaryLastMountTransactionId: %v)",
            tabletOwner->GetId(),
            tabletOwner->GetLastMountTransactionId(),
            tabletOwner->GetPrimaryLastMountTransactionId());

        UpdateTabletState(tabletOwner);
    }

    void UpdateTabletState(TTabletOwnerBase* table)
    {
        if (!IsObjectAlive(table)) {
            return;
        }

        if (table->IsExternal()) {
            // Primary master is the coordinator of 2pc and commits after secondary to hold the exclusive lock.
            // (It is necessary for primary master to hold the lock longer to prevent
            // user from locking the node while secondary master still performs 2pc.)
            // Thus, secondary master can commit and send updates when primary master is not ready yet.
            // Here we ask secondary master to resend tablet state.

            NProto::TReqUpdateTabletState request;
            ToProto(request.mutable_table_id(), table->GetId());
            ToProto(request.mutable_last_mount_transaction_id(), table->GetLastMountTransactionId());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(request, table->GetExternalCellTag());

            YT_LOG_DEBUG("Table tablet state check requested (TableId: %v, LastMountTransactionId: %v)",
                table->GetId(),
                table->GetLastMountTransactionId());
            return;
        }

        // TODO(savrus): Remove this after testing multicell on real cluster is done.
        YT_LOG_DEBUG("Table tablet state check started (TableId: %v, LastMountTransactionId: %v, PrimaryLastMountTransactionId: %v, TabletCountByState: %v, TabletCountByExpectedState: %v)",
            table->GetId(),
            table->GetLastMountTransactionId(),
            table->GetPrimaryLastMountTransactionId(),
            ConvertToYsonString(table->TabletCountByState(), EYsonFormat::Text).ToString(),
            ConvertToYsonString(table->TabletCountByExpectedState(), EYsonFormat::Text).ToString());


        if (table->TabletCountByExpectedState()[ETabletState::Unmounting] > 0 ||
            table->TabletCountByExpectedState()[ETabletState::Freezing] > 0 ||
            table->TabletCountByExpectedState()[ETabletState::FrozenMounting] > 0 ||
            table->TabletCountByExpectedState()[ETabletState::Mounting] > 0 ||
            table->TabletCountByExpectedState()[ETabletState::Unfreezing] > 0)
        {
            return;
        }

        {
            // Just sanity check.
            auto tabletCount =
                table->TabletCountByExpectedState()[ETabletState::Mounted] +
                table->TabletCountByExpectedState()[ETabletState::Unmounted] +
                table->TabletCountByExpectedState()[ETabletState::Frozen];
            YT_VERIFY(tabletCount == std::ssize(table->Tablets()));
        }

        auto actualState = table->ComputeActualTabletState();
        std::optional<ETabletState> expectedState;

        if (table->GetLastMountTransactionId()) {
            if (table->TabletCountByExpectedState()[ETabletState::Mounted] > 0) {
                expectedState = ETabletState::Mounted;
            } else if (table->TabletCountByExpectedState()[ETabletState::Frozen] > 0) {
                expectedState = ETabletState::Frozen;
            } else {
                expectedState = ETabletState::Unmounted;
            }
        }

        YT_LOG_DEBUG("Table tablet state updated "
            "(TableId: %v, ActualTabletState: %v, ExpectedTabletState: %v, LastMountTransactionId: %v, PrimaryLastMountTransactionId: %v)",
            table->GetId(),
            actualState,
            expectedState,
            table->GetLastMountTransactionId(),
            table->GetPrimaryLastMountTransactionId());

        table->SetActualTabletState(actualState);
        if (expectedState) {
            table->SetExpectedTabletState(*expectedState);
        }

        if (table->IsNative()) {
            YT_VERIFY(!table->GetPrimaryLastMountTransactionId());
            table->SetLastMountTransactionId(TTransactionId());
        } else {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            YT_VERIFY(multicellManager->IsSecondaryMaster());

            // Check that primary master is waiting to clear LastMountTransactionId.
            bool clearLastMountTransactionId = table->GetLastMountTransactionId() &&
                table->GetLastMountTransactionId() == table->GetPrimaryLastMountTransactionId();

            // Statistics should be correct before setting the tablet state.
            const auto& tableManager = Bootstrap_->GetTableManager();
            tableManager->SendStatisticsUpdate(table);

            NProto::TReqUpdateUpstreamTabletState request;
            ToProto(request.mutable_table_id(), table->GetId());
            request.set_actual_tablet_state(ToProto<int>(actualState));
            if (clearLastMountTransactionId) {
                ToProto(request.mutable_last_mount_transaction_id(), table->GetLastMountTransactionId());
            }
            if (expectedState) {
                request.set_expected_tablet_state(ToProto<int>(*expectedState));
            }

            multicellManager->PostToMaster(request, table->GetNativeCellTag());

            if (clearLastMountTransactionId) {
                table->SetLastMountTransactionId(TTransactionId());
                table->SetPrimaryLastMountTransactionId(TTransactionId());
            }
        }
    }

    TTabletServant* FindServantForStateTransition(
        TTabletBase* tablet,
        TRevision mountRevision,
        TCellId senderId,
        bool senderIsCell,
        TStringBuf changeType)
    {
        if (tablet->GetState() == ETabletState::Unmounted) {
            if (!tablet->GetWasForcefullyUnmounted()) {
                YT_LOG_ALERT("%v notification received by an unmounted tablet, ignored "
                    "(TabletId: %v, SenderId: %v)",
                    changeType,
                    tablet->GetId(),
                    senderId);
            }
            return nullptr;
        }

        TTabletServant* servant = nullptr;

        // COMPAT(ifsmirnov): remove when 24.1 is deployed.
        // This should not happen unless masters are updated before nodes.
        if (!mountRevision) {
            servant = &tablet->Servant();
            YT_LOG_ALERT("%v notification received without mount revision "
                "(TabletId: %v, SenderId: %v)",
                changeType,
                tablet->GetId(),
                senderId);

        } else {
            servant = tablet->FindServant(mountRevision);
            if (!servant) {
                YT_LOG_WARNING("%v notification received by a tablet with wrong mount revision, ignored "
                    "(TabletId: %v, MainServantMountRevision: %v, RequestMountRevision: %v, SenderId: %v)",
                    changeType,
                    tablet->GetId(),
                    tablet->Servant().GetMountRevision(),
                    mountRevision,
                    senderId);
                return nullptr;
            }
        }

        auto expectedSenderId = senderIsCell
            ? GetObjectId(servant->GetCell())
            : tablet->GetNodeEndpointId();

        // This condition should never be violated given that mount revision check passed.
        if (senderId != expectedSenderId) {
            YT_LOG_ALERT(
                "%v notification received from unexpected sender, ignored "
                "(TabletId: %v, State: %v, SenderId: %v, TabletCellId: %v, MountRevision: %v)",
                changeType,
                tablet->GetId(),
                tablet->GetState(),
                senderId,
                GetObjectId(tablet->Servant().GetCell()),
                servant->GetMountRevision());
            return nullptr;
        }

        return servant;
    }

    void HydraOnTabletMounted(NProto::TRspMountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);

        auto frozen = response->frozen();
        OnTabletMounted(tabletId, frozen, response->mount_revision());
    }

    void HydraOnHunkTabletMounted(NProto::TRspMountHunkTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::HunkTablet);
        OnTabletMounted(tabletId, /*frozen*/ false, response->mount_revision());
    }

    void OnTabletMounted(TTabletId tabletId, bool frozen, TRevision mountRevision)
    {
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto senderId = GetHiveMutationSenderId();
        auto* servant = FindServantForStateTransition(
            tablet,
            mountRevision,
            senderId,
            /*senderIsCell*/ true,
            "Mounted");
        if (!servant) {
            return;
        }

        if (servant->IsAuxiliary()) {
            YT_VERIFY(tablet->GetState() == ETabletState::Mounted);
            YT_VERIFY(servant->GetState() == ETabletState::Mounting);
            servant->SetState(ETabletState::Mounted);

            OnTabletActionStateChanged(tablet->GetAction());

            YT_LOG_DEBUG("Auxiliary servant mounted "
                "(TableId: %v, TabletId: %v)",
                tablet->GetOwner()->GetId(),
                tablet->GetId());

            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounting && state != ETabletState::FrozenMounting) {
            if (!tablet->GetWasForcefullyUnmounted()) {
                YT_LOG_ALERT("Mounted notification received for a tablet in wrong state, ignored (TabletId: %v, State: %v)",
                    tabletId,
                    state);
            }
            return;
        }

        auto* table = tablet->GetOwner();
        auto* cell = tablet->GetCell();

        YT_LOG_DEBUG("Tablet mounted (TableId: %v, TabletId: %v, MountRevision: %x, CellId: %v, Frozen: %v)",
            table->GetId(),
            tablet->GetId(),
            tablet->Servant().GetMountRevision(),
            cell->GetId(),
            frozen);

        {
            auto newState = frozen ? ETabletState::Frozen : ETabletState::Mounted;
            servant->SetState(newState);
            tablet->SetState(newState);
        }

        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnmounted(NProto::TRspUnmountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);

        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto senderId = GetHiveMutationSenderId();
        auto* servant = FindServantForStateTransition(
            tablet,
            response->mount_revision(),
            senderId,
            /*senderIsCell*/ true,
            "Unmounted");
        if (!servant) {
            return;
        }

        if (!ValidateTabletServantUnmounted(tablet, servant, senderId)) {
            return;
        }

        if (auto& auxiliaryServant = tablet->AuxiliaryServant()) {
            DeallocateAuxiliaryServant(tablet);
        }

        auto* typedTablet = tablet->As<TTablet>();
        if (response->has_replication_progress()) {
            typedTablet->ReplicationProgress() = FromProto<TReplicationProgress>(response->replication_progress());
        }

        TabletChunkManager_->SetTabletEdenStoreIds(
            typedTablet,
            FromProto<std::vector<TStoreId>>(response->mount_hint().eden_store_ids()));
        DiscardDynamicStores(typedTablet);

        DoTabletServantUnmounted(tablet, servant, /*force*/ false);

        OnTabletActionStateChanged(tablet->GetAction());
    }

    void HydraOnHunkTabletUnmounted(NProto::TRspUnmountHunkTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::HunkTablet);

        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        if (!ValidateTabletServantUnmounted(tablet, &tablet->Servant(), GetHiveMutationSenderId())) {
            return;
        }

        auto* hunkTablet = tablet->As<THunkTablet>();
        DoTabletServantUnmounted(hunkTablet, &hunkTablet->Servant(), /*force*/ false);
    }

    void HydraOnTabletFrozen(NProto::TRspFreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tablet = FindTablet(tabletId)->As<TTablet>();
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto senderId = GetHiveMutationSenderId();
        auto* servant = FindServantForStateTransition(
            tablet,
            response->mount_revision(),
            senderId,
            /*senderIsCell*/ false,
            "Frozen");
        if (!servant) {
            return;
        }

        YT_VERIFY(!tablet->AuxiliaryServant());

        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Freezing) {
            if (!tablet->GetWasForcefullyUnmounted()) {
                YT_LOG_ALERT("Frozen notification received for a tablet in wrong state, ignored (TabletId: %v, State: %v)",
                    tabletId,
                    state);
            }
            return;
        }

        TabletChunkManager_->SetTabletEdenStoreIds(
            tablet->As<TTablet>(),
            FromProto<std::vector<TStoreId>>(response->mount_hint().eden_store_ids()));

        DiscardDynamicStores(tablet->As<TTablet>());

        YT_LOG_DEBUG("Tablet frozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        servant->SetState(ETabletState::Frozen);
        tablet->SetState(ETabletState::Frozen);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnfrozen(NProto::TRspUnfreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto senderId = GetHiveMutationSenderId();
        auto* servant = FindServantForStateTransition(
            tablet,
            response->mount_revision(),
            senderId,
            /*senderIsCell*/ false,
            "Unfrozen");
        if (!servant) {
            return;
        }

        auto* table = tablet->GetOwner();
        auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Unfreezing) {
            if (!tablet->GetWasForcefullyUnmounted()) {
                YT_LOG_ALERT("Unfrozen notification received for a tablet in wrong state, ignored (TabletId: %v, State: %v)",
                    tabletId,
                    state);
            }
            return;
        }

        YT_LOG_DEBUG("Tablet unfrozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        servant->SetState(ETabletState::Mounted);
        tablet->SetState(ETabletState::Mounted);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraSwitchServant(NProto::TReqSwitchServant* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto sourceMountRevision = request->source_mount_revision();
        auto targetMountRevision = request->target_mount_revision();

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto& mainServant = tablet->Servant();
        auto& auxiliaryServant = tablet->AuxiliaryServant();

        if (mainServant.GetMountRevision() != sourceMountRevision ||
            auxiliaryServant.GetMountRevision() != targetMountRevision)
        {
            YT_LOG_DEBUG("Mount revision mismatch, will not switch servants "
                "(TabletId: %v, ExpectedSourceMountRevision: %v, ExpectedTargetMountRevision: %v, ",
                "ActualSourceMountRevision: %v, ActualTargetMountRevision: %v)",
                tablet->GetId(),
                sourceMountRevision,
                targetMountRevision,
                mainServant.GetMountRevision(),
                auxiliaryServant.GetMountRevision());
            return;
        }

        mainServant.Swap(&auxiliaryServant);

        if (tablet->GetType() == EObjectType::Tablet) {
            auto* typedTablet = tablet->As<TTablet>();
            auto* table = tablet->GetOwner()->As<TTableNode>();

            table->DiscountTabletStatistics(typedTablet->GetTabletStatistics());
            std::swap(typedTablet->NodeStatistics(), typedTablet->AuxiliaryNodeStatistics());
            table->AccountTabletStatistics(typedTablet->GetTabletStatistics());
        }

        Bootstrap_->GetAvenueDirectory()->UpdateEndpoint(
            tablet->GetNodeEndpointId(),
            mainServant.GetCell()->GetId());

        YT_LOG_DEBUG("Servant switched (TabletId: %v, SourceCellId: %v, TargetCellId: %v)",
            tabletId,
            GetObjectId(auxiliaryServant.GetCell()),
            GetObjectId(mainServant.GetCell()));
    }

    void DeallocateAuxiliaryServant(TTabletBase* tablet)
    {
        auto& auxiliaryServant = tablet->AuxiliaryServant();

        YT_LOG_DEBUG("Deallocating auxiliary servant (TabletId: %v, CellId: %v)",
            tablet->GetId(),
            auxiliaryServant.GetCell()->GetId());

        NTabletNode::NProto::TReqUnmountTablet req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_force(true);
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(auxiliaryServant.GetCell()->GetId());
        hiveManager->PostMessage(mailbox, req);

        DoTabletServantUnmounted(tablet, &auxiliaryServant, /*force*/ true);

        tablet->SetTabletwiseAvenueEndpointId({});
        tablet->Servant().SetMovementRole(NTabletNode::ESmoothMovementRole::None);

        OnTabletActionStateChanged(tablet->GetAction());
    }

    void HydraDeallocateServant(NProto::TReqDeallocateServant* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto auxiliaryMountRevision = request->auxiliary_mount_revision();

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto& mainServant = tablet->Servant();
        auto& auxiliaryServant = tablet->AuxiliaryServant();

        if (auxiliaryServant.GetMountRevision() != auxiliaryMountRevision) {
            YT_LOG_DEBUG("Mount revision mismatch, will not deallocate servant "
                "(TabletId: %v, ExpectedAuxiliaryMountRevision: %v, ",
                "ActualSourceMountRevision: %v, ActualTargetMountRevision: %v)",
                tablet->GetId(),
                auxiliaryMountRevision,
                mainServant.GetMountRevision(),
                auxiliaryServant.GetMountRevision());

            return;
        }

        DeallocateAuxiliaryServant(tablet);
    }

    void HydraReportSmoothMovementProgress(NProto::TReqReportSmoothMovementProgress* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto stage = FromProto<NTabletNode::ESmoothMovementStage>(request->stage());

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto* servant = tablet->FindServant(mountRevision);
        if (!servant) {
            return;
        }

        servant->SetMovementStage(stage);
    }

    void HydraReportSmoothMovementAborted(NProto::TReqReportSmoothMovementAborted* response)
    {
        // This mutation is always triggered by a Hive message from node which can be sent
        // in two contexts.
        // 1. Master has aborted smooth movement by itself, and node responds with
        // TReqReportSmoothMovementAborted. Tablet is already unbound from the action
        // but its auxiliary servant (if present) must be deallocated now.
        // 2. Smooth movement is aborted by node. In this case master must deallocate
        // auxiliary servant and fail tablet action immediately.

        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto error = FromProto<TError>(response->error());

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (!tablet->AuxiliaryServant()) {
            return;
        }

        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Smooth movement aborted by node "
                "(TabletId: %v, MainCellId: %v, AuxiliaryCellId: %v)",
                tablet->GetId(),
                GetObjectId(tablet->Servant().GetCell()),
                GetObjectId(tablet->AuxiliaryServant().GetCell()));
        }

        // NB: At this moment master definitely knows which of the servants is active
        // since both TReqReportSmoothMovementAborted and TReqSwitchServant go via the same
        // avenue channel.

        YT_LOG_DEBUG("Deallocating auxiliary servant on smooth movement abort "
            "(TabletId: %v, MainCellId: %v, AuxiliaryCellId: %v)",
            tablet->GetId(),
            GetObjectId(tablet->Servant().GetCell()),
            GetObjectId(tablet->AuxiliaryServant().GetCell()));

        DeallocateAuxiliaryServant(tablet);

        if (auto* action = tablet->GetAction()) {
            OnTabletActionDisturbed(action, error);
        }
    }

    void HydraUpdateTableReplicaStatistics(NProto::TReqUpdateTableReplicaStatistics* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tabletBase = FindTablet(tabletId);
        if (!IsObjectAlive(tabletBase)) {
            return;
        }

        auto* tablet = tabletBase->As<TTablet>();

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->Servant().GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->GetReplicaInfo(replica);
        PopulateTableReplicaInfoFromStatistics(replicaInfo, request->statistics());

        YT_LOG_DEBUG("Table replica statistics updated (TabletId: %v, ReplicaId: %v, "
            "CommittedReplicationRowIndex: %v, CurrentReplicationTimestamp: %v)",
            tabletId,
            replicaId,
            replicaInfo->GetCommittedReplicationRowIndex(),
            replicaInfo->GetCurrentReplicationTimestamp());
    }

    void HydraOnTableReplicaEnabled(NProto::TRspEnableTableReplica* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tabletBase = FindTablet(tabletId);
        if (!IsObjectAlive(tabletBase)) {
            return;
        }

        auto* tablet = tabletBase->As<TTablet>();

        auto replicaId = FromProto<TTableReplicaId>(response->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = response->mount_revision();
        if (tablet->Servant().GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->GetReplicaInfo(replica);
        if (replicaInfo->GetState() != ETableReplicaState::Enabling) {
            YT_LOG_WARNING("Enabled replica notification received for a replica in a wrong state, "
                "ignored (TabletId: %v, ReplicaId: %v, State: %v)",
                tabletId,
                replicaId,
                replicaInfo->GetState());
            return;
        }

        StopReplicaTransition(tablet, replica, replicaInfo, ETableReplicaState::Enabled);
        CheckTransitioningReplicaTablets(replica);
    }

    void HydraOnTableReplicaDisabled(NProto::TRspDisableTableReplica* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        auto replicaId = FromProto<TTableReplicaId>(response->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = response->mount_revision();
        if (tablet->Servant().GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->As<TTablet>()->GetReplicaInfo(replica);
        if (replicaInfo->GetState() != ETableReplicaState::Disabling) {
            YT_LOG_WARNING("Disabled replica notification received for a replica in a wrong state, "
                "ignored (TabletId: %v, ReplicaId: %v, State: %v)",
                tabletId,
                replicaId,
                replicaInfo->GetState());
            return;
        }

        StopReplicaTransition(tablet->As<TTablet>(), replica, replicaInfo, ETableReplicaState::Disabled);
        CheckTransitioningReplicaTablets(replica);
    }

    void StartReplicaTransition(TTablet* tablet, TTableReplica* replica, TTableReplicaInfo* replicaInfo, ETableReplicaState newState)
    {
        YT_LOG_DEBUG("Table replica is now transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
            tablet->GetTable()->GetId(),
            tablet->GetId(),
            replica->GetId(),
            replicaInfo->GetState(),
            newState);
        replicaInfo->SetState(newState);
        YT_VERIFY(replica->TransitioningTablets().insert(tablet).second);
    }

    void StopReplicaTransition(TTablet* tablet, TTableReplica* replica, TTableReplicaInfo* replicaInfo, ETableReplicaState newState)
    {
        YT_LOG_DEBUG("Table replica is no longer transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
            tablet->GetTable()->GetId(),
            tablet->GetId(),
            replica->GetId(),
            replicaInfo->GetState(),
            newState);
        replicaInfo->SetState(newState);
        YT_VERIFY(replica->TransitioningTablets().erase(tablet) == 1);
    }

    void CheckTransitioningReplicaTablets(TTableReplica* replica)
    {
        auto state = replica->GetState();
        if (state != ETableReplicaState::Enabling && state != ETableReplicaState::Disabling) {
            return;
        }

        if (!replica->TransitioningTablets().empty()) {
            return;
        }

        auto* table = replica->GetTable();

        switch (state) {
            case ETableReplicaState::Enabling:
                YT_LOG_DEBUG("Table replica enabled (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabled);
                break;

            case ETableReplicaState::Disabling:
                YT_LOG_DEBUG("Table replica disabled (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Disabled);
                break;

            default:
                YT_ABORT();
        }

        FireUponTableReplicaUpdate(replica);
    }

    void DiscardDynamicStores(TTablet* tablet)
    {
        auto stores = EnumerateStoresInChunkTree(tablet->GetChunkList());

        std::vector<TChunkTree*> dynamicStores;
        for (auto* store : stores) {
            if (IsDynamicTabletStoreType(store->GetType())) {
                dynamicStores.push_back(store);
                store->AsDynamicStore()->SetFlushedChunk(nullptr);
            }
        }

        if (dynamicStores.empty()) {
            return;
        }

        // NB: Dynamic stores can be detached unambiguously since they are direct children of a tablet.
        TabletChunkManager_->CopyChunkListsIfShared(tablet->GetTable(), tablet->GetIndex(), tablet->GetIndex(), /*force*/ false);

        TabletChunkManager_->DetachChunksFromTablet(
            tablet,
            dynamicStores,
            tablet->GetTable()->IsPhysicallySorted()
                ? EChunkDetachPolicy::SortedTablet
                : EChunkDetachPolicy::OrderedTabletSuffix);

        auto* table = tablet->GetTable();
        RecomputeTableSnapshotStatistics(table);

        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            table,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false);

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = -ssize(dynamicStores);
        table->AccountTabletStatisticsDelta(statisticsDelta);

        YT_VERIFY(tablet->Servant());
        tablet->Servant().GetCell()->GossipStatistics().Local() += statisticsDelta;
    }

    void AbandonDynamicStores(TTablet* tablet)
    {
        // Making a copy since store->Abandon() will remove elements from tablet->DynamicStores().
        auto stores = tablet->DynamicStores();

        for (auto* store : stores) {
            store->Abandon();
        }
    }

    bool ValidateTabletServantUnmounted(
        const TTabletBase* tablet,
        const TTabletServant* servant,
        TTabletCellId senderId) const
    {
        auto state = servant->GetState();
        if (state != ETabletState::Unmounting) {
            if (!tablet->GetWasForcefullyUnmounted()) {
                YT_LOG_ALERT(
                    "Unmounted notification received for a tablet in %Qlv state, ignored "
                    "(TabletId: %v, SenderId: %v)",
                    state,
                    tablet->GetId(),
                    senderId);
            }
            return false;
        }

        return true;
    }

    void DoTabletServantUnmounted(TTabletBase* tablet, TTabletServant* servant, bool force)
    {
        auto* table = tablet->GetOwner();
        auto* cell = servant->GetCell();
        YT_VERIFY(cell);

        YT_LOG_DEBUG(
            "Tablet unmounted from servant (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        if (tablet->GetType() == EObjectType::Tablet) {
            auto tabletStatistics = tablet->GetTabletStatistics(servant->IsAuxiliary());
            cell->GossipStatistics().Local() -= tabletStatistics;
        }

        CheckIfFullyUnmounted(cell);

        EraseOrCrash(cell->Tablets(), tablet);

        servant->Clear();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(cell);

        if (servant->IsAuxiliary()) {
            // Auxiliary servant should never be unmounted before the main one.
            YT_VERIFY(tablet->Servant());

            if (tablet->GetType() == EObjectType::Tablet) {
                tablet->As<TTablet>()->AuxiliaryNodeStatistics().Clear();
            }
        } else {
            YT_VERIFY(!tablet->AuxiliaryServant());

            switch (tablet->GetType()) {
                case EObjectType::Tablet:
                    DoTabletUnmounted(tablet->As<TTablet>(), force);
                    break;

                case EObjectType::HunkTablet:
                    DoTabletUnmounted(tablet->As<THunkTablet>(), force);
                    break;

                default:
                    YT_ABORT();
            }
        }
    }

    void DoTabletUnmountedBase(TTabletBase* tablet, bool force)
    {
        auto* table = tablet->GetOwner();

        auto resourceUsageDelta = TTabletResources()
            .SetTabletStaticMemory(tablet->GetTabletStaticMemorySize());

        if (tablet->IsMountedWithAvenue()) {
            auto nodeEndpointId = tablet->GetNodeEndpointId();
            auto masterEndpointId = GetSiblingAvenueEndpointId(nodeEndpointId);

            tablet->SetNodeAvenueEndpointId({});

            Bootstrap_->GetAvenueDirectory()->UpdateEndpoint(nodeEndpointId, /*cellId*/ {});
            Bootstrap_->GetHiveManager()->UnregisterAvenueEndpoint(masterEndpointId);
        } else {
            --NonAvenueTabletCount_;
        }

        tablet->SetInMemoryMode(EInMemoryMode::None);
        tablet->SetState(ETabletState::Unmounted);
        tablet->SetStoresUpdatePreparedTransaction(nullptr);
        tablet->SetWasForcefullyUnmounted(force);

        if (tablet->GetSettingsRevision() < table->GetSettingsUpdateRevision()) {
            int newCount = table->GetRemountNeededTabletCount() - 1;
            YT_ASSERT(newCount >= 0);
            table->SetRemountNeededTabletCount(newCount);
        }
        tablet->SetSettingsRevision(NullRevision);

        UpdateResourceUsage(table, -resourceUsageDelta);
        UpdateTabletState(table);
    }

    void DoTabletUnmounted(TTablet* tablet, bool force)
    {
        auto tabletStatistics = tablet->GetTabletStatistics();
        tablet->GetOwner()->DiscountTabletStatistics(tabletStatistics);
        tablet->NodeStatistics().Clear();
        tablet->AuxiliaryNodeStatistics().Clear();
        tablet->PerformanceCounters() = {};
        tablet->SetTabletErrorCount(0);
        tablet->SetReplicationErrorCount(0);

        DoTabletUnmountedBase(tablet, force);

        if (tablet->GetBackupState() != ETabletBackupState::None) {
            const auto& backupManager = Bootstrap_->GetBackupManager();
            backupManager->OnBackupInterruptedByUnmount(tablet);
        }

        auto* table = tablet->GetTable();
        auto* chunkList = tablet->GetChunkList();
        if (!chunkList) {
            YT_VERIFY(force);
        } else {
            if (chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet) {
                const auto& chunkListStatistics = chunkList->Statistics();
                if (tablet->GetTrimmedRowCount() > chunkListStatistics.LogicalRowCount) {
                    auto message = Format(
                        "Trimmed row count exceeds total row count of the tablet "
                        "and will be rolled back (TableId: %v, TabletId: %v, "
                        "TrimmedRowCount: %v, LogicalRowCount: %v)",
                        table->GetId(),
                        tablet->GetId(),
                        tablet->GetTrimmedRowCount(),
                        chunkListStatistics.LogicalRowCount);
                    if (force) {
                        YT_LOG_WARNING(message);
                        tablet->SetTrimmedRowCount(chunkListStatistics.LogicalRowCount);
                    } else {
                        YT_LOG_ALERT(message);
                    }
                }
            }
        }

        for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
            auto* replica = it->first;
            auto& replicaInfo = it->second;
            if (replica->TransitioningTablets().erase(tablet) == 1) {
                YT_LOG_ALERT("Table replica is still transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v)",
                    tablet->GetTable()->GetId(),
                    tablet->GetId(),
                    replica->GetId(),
                    replicaInfo.GetState());
            } else {
                YT_LOG_DEBUG("Table replica state updated (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
                    tablet->GetTable()->GetId(),
                    tablet->GetId(),
                    replica->GetId(),
                    replicaInfo.GetState(),
                    ETableReplicaState::None);
            }
            replicaInfo.SetState(ETableReplicaState::None);
            CheckTransitioningReplicaTablets(replica);
        }

        for (auto transactionId : tablet->UnconfirmedDynamicTableLocks()) {
            table->ConfirmDynamicTableLock(transactionId);
        }
        tablet->UnconfirmedDynamicTableLocks().clear();

        tablet->GetOwner()->AccountTabletStatistics(tablet->GetTabletStatistics());
    }

    void DoTabletUnmounted(THunkTablet* tablet, bool force)
    {
        DoTabletUnmountedBase(tablet, force);

        auto* owner = tablet->GetOwner();
        owner->AccountTabletStatistics(tablet->GetTabletStatistics());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto chunks = EnumerateChunksInChunkTree(tablet->GetChunkList());
        for (auto* chunk : chunks) {
            chunk->SetSealable(true);
            chunkManager->ScheduleChunkSeal(chunk);
        }
    }

    template <class TRequest>
    void CreateAndAttachDynamicStores(TTablet* tablet, TRequest* request)
    {
        for (int index = 0; index < DynamicStoreIdPoolSize; ++index) {
            auto* dynamicStore = TabletChunkManager_->CreateDynamicStore(tablet);
            AttachDynamicStoreToTablet(tablet, dynamicStore);
            ToProto(request->add_dynamic_store_ids(), dynamicStore->GetId());
        }
    }

    void HydraPrepareUpdateTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::Tablet);
        auto* tablet = GetTabletOrThrow(tabletId)->As<TTablet>();

        PrepareUpdateTabletStoresBase(tablet);

        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        TabletChunkManager_->PrepareUpdateTabletStores(tablet, request);

        tablet->SetStoresUpdatePreparedTransaction(transaction);

        YT_LOG_DEBUG(
            "Tablet stores update prepared (TransactionId: %v, TableId: %v, TabletId: %v)",
            transaction->GetId(),
            tablet->GetOwner()->GetId(),
            tabletId);
    }

    void HydraPrepareUpdateHunkTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateHunkTabletStores* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        YT_VERIFY(TypeFromId(tabletId) == EObjectType::HunkTablet);
        auto* tablet = GetTabletOrThrow(tabletId)->As<THunkTablet>();

        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        PrepareUpdateTabletStoresBase(tablet);

        tablet->SetStoresUpdatePreparedTransaction(transaction);

        YT_LOG_DEBUG(
            "Hunk tablet stores update prepared "
            "(TransactionId: %v, HunkStorageId: %v, TabletId: %v)",
            transaction->GetId(),
            tablet->GetOwner()->GetId(),
            tabletId);
    }

    void PrepareUpdateTabletStoresBase(TTabletBase* tablet)
    {
        if (tablet->GetStoresUpdatePreparedTransaction()) {
            THROW_ERROR_EXCEPTION("Stores update for tablet %v is already prepared by transaction %v",
                tablet->GetId(),
                tablet->GetStoresUpdatePreparedTransaction()->GetId());
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounted &&
            state != ETabletState::Unmounting &&
            state != ETabletState::Freezing)
        {
            THROW_ERROR_EXCEPTION("Cannot update stores while tablet %v is in %Qlv state",
                tablet->GetId(),
                state);
        }
    }

    void HydraCommitUpdateTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        if (!CommitUpdateTabletStoresBase(transaction, tablet, request->mount_revision())) {
            return;
        }

        auto* owner = tablet->GetOwner();
        YT_VERIFY(IsObjectAlive(owner));

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        CommitUpdateTabletStores(
            tablet->As<TTablet>(),
            transaction,
            request,
            updateReason);
    }

    void HydraCommitUpdateHunkTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateHunkTabletStores* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        if (!CommitUpdateTabletStoresBase(transaction, tablet, request->mount_revision())) {
            return;
        }

        CommitUpdateHunkTabletStores(tablet->As<THunkTablet>(), transaction, request);
    }

    bool CommitUpdateTabletStoresBase(
        TTransaction* transaction,
        TTabletBase* tablet,
        NHydra::TRevision mountRevision)
    {
        if (tablet->GetStoresUpdatePreparedTransaction() != transaction) {
            YT_LOG_DEBUG(
                "Tablet stores update commit for an improperly prepared tablet; ignored "
                "(TabletId: %v, ExpectedTransactionId: %v, ActualTransactionId: %v)",
                tablet->GetId(),
                transaction->GetId(),
                GetObjectId(tablet->GetStoresUpdatePreparedTransaction()));
            return false;
        }

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        auto* servant = tablet->FindServant(mountRevision);

        if (!servant) {
            YT_LOG_DEBUG(
                "Invalid mount revision on tablet stores update commit; ignored "
                "(TabletId: %v, TransactionId: %v, ExpectedMountRevision: %x, ActualMountRevision: %x)",
                tablet->GetId(),
                transaction->GetId(),
                mountRevision,
                tablet->Servant().GetMountRevision());
            return false;
        }

        auto* owner = tablet->GetOwner();
        if (!IsObjectAlive(owner)) {
            return false;
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetModified(owner, EModificationType::Content);

        return true;
    }

    void CommitUpdateTabletStores(
        TTablet* tablet,
        TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        ETabletStoresUpdateReason updateReason)
    {
        auto* table = tablet->GetTable();

        // Update retained timestamp.
        auto retainedTimestamp = std::max(
            tablet->GetRetainedTimestamp(),
            static_cast<TTimestamp>(request->retained_timestamp()));
        tablet->SetRetainedTimestamp(retainedTimestamp);

        // Save old tablet resource usage.
        auto oldMemorySize = tablet->GetTabletStaticMemorySize();
        auto oldStatistics = tablet->GetTabletStatistics();
        auto oldAuxiliaryStatistics = tablet->AuxiliaryServant()
            ? std::make_optional(tablet->GetTabletStatistics(/*fromAuxiliaryCell*/ true))
            : std::nullopt;

        auto tabletChunkManagerLoggingString = TabletChunkManager_->CommitUpdateTabletStores(
            tablet,
            transaction,
            request,
            updateReason);

        RecomputeTableSnapshotStatistics(table);

        // Get new tablet resource usage.
        auto newMemorySize = tablet->GetTabletStaticMemorySize();
        auto newStatistics = tablet->GetTabletStatistics();
        auto deltaStatistics = newStatistics - oldStatistics;

        // Update cell and table statistics.
        tablet->Servant().GetCell()->GossipStatistics().Local() += deltaStatistics;
        if (auto& auxiliaryServant = tablet->AuxiliaryServant()) {
            auto newAuxiliaryStatistics = tablet->GetTabletStatistics(/*fromAuxiliaryCell*/ true);
            auxiliaryServant.GetCell()->GossipStatistics().Local() += newAuxiliaryStatistics - *oldAuxiliaryStatistics;
        }
        table->DiscountTabletStatistics(oldStatistics);
        table->AccountTabletStatistics(newStatistics);

        // Update table resource usage.
        UpdateResourceUsage(
            table,
            TTabletResources().SetTabletStaticMemory(newMemorySize - oldMemorySize));

        YT_LOG_DEBUG(
            "Tablet stores update committed (TransactionId: %v, TableId: %v, TabletId: %v, %v, "
            "RetainedTimestamp: %v, UpdateReason: %v)",
            transaction->GetId(),
            table->GetId(),
            tablet->GetId(),
            tabletChunkManagerLoggingString,
            retainedTimestamp,
            updateReason);
    }

    void CommitUpdateHunkTabletStores(
        THunkTablet* tablet,
        TTransaction* transaction,
        NProto::TReqUpdateHunkTabletStores* request)
    {
        auto oldStatistics = tablet->GetTabletStatistics();

        auto tabletChunkManagerLoggingString = TabletChunkManager_->CommitUpdateHunkTabletStores(
            tablet,
            request);

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        auto newStatistics = tablet->GetTabletStatistics();
        auto deltaStatistics = newStatistics - oldStatistics;

        auto* cell = tablet->GetCell();
        cell->GossipStatistics().Local() += deltaStatistics;

        auto* owner = tablet->GetOwner();
        owner->DiscountTabletStatistics(oldStatistics);
        owner->AccountTabletStatistics(newStatistics);

        YT_LOG_DEBUG(
            "Hunk tablet stores update committed (TransactionId: %v, HunkStorageId: %v, TabletId: %v, "
            "%v)",
            transaction->GetId(),
            tablet->GetOwner()->GetId(),
            tablet->GetId(),
            tabletChunkManagerLoggingString);
    }

    void HydraAbortUpdateTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        const TTransactionAbortOptions& /*options*/)
    {
        AbortUpdateTabletStores(
            transaction,
            FromProto<TTabletId>(request->tablet_id()));
    }

    void HydraAbortUpdateHunkTabletStores(
        TTransaction* transaction,
        NProto::TReqUpdateHunkTabletStores* request,
        const TTransactionAbortOptions& /*options*/)
    {
        AbortUpdateTabletStores(
            transaction,
            FromProto<TTabletId>(request->tablet_id()));
    }

    void AbortUpdateTabletStores(
        TTransaction* transaction,
        TTabletId tabletId)
    {
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        if (tablet->GetStoresUpdatePreparedTransaction() != transaction) {
            return;
        }

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        const auto* table = tablet->GetOwner();

        YT_LOG_DEBUG(
            "Tablet stores update aborted (TransactionId: %v, TableId: %v, TabletId: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId);
    }

    void HydraUpdateTabletTrimmedRowCount(NProto::TReqUpdateTabletTrimmedRowCount* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        auto mountRevision = request->mount_revision();
        if (tablet->Servant().GetMountRevision() != mountRevision) {
            return;
        }

        if (tablet->GetState() == ETabletState::Unmounted) {
            return;
        }

        auto trimmedRowCount = request->trimmed_row_count();

        tablet->As<TTablet>()->SetTrimmedRowCount(trimmedRowCount);

        YT_LOG_DEBUG("Tablet trimmed row count updated (TabletId: %v, TrimmedRowCount: %v)",
            tabletId,
            trimmedRowCount);
    }

    void HydraAllocateDynamicStore(NProto::TReqAllocateDynamicStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        auto mountRevision = request->mount_revision();
        if (tablet->Servant().GetMountRevision() != mountRevision) {
            return;
        }

        auto* dynamicStore = TabletChunkManager_->CreateDynamicStore(tablet->As<TTablet>());
        AttachDynamicStoreToTablet(tablet->As<TTablet>(), dynamicStore);

        TRspAllocateDynamicStore rsp;
        ToProto(rsp.mutable_dynamic_store_id(), dynamicStore->GetId());
        ToProto(rsp.mutable_tablet_id(), tabletId);
        rsp.set_mount_revision(tablet->Servant().GetMountRevision());

        YT_LOG_DEBUG("Dynamic store allocated (StoreId: %v, TabletId: %v, TableId: %v)",
            dynamicStore->GetId(),
            tabletId,
            tablet->As<TTablet>()->GetTable()->GetId());

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(tablet->GetNodeEndpointId());
        hiveManager->PostMessage(mailbox, rsp);
    }

    void HydraCreateTabletAction(NProto::TReqCreateTabletAction* request)
    {
        auto kind = ETabletActionKind(request->kind());
        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());
        auto cellIds = FromProto<std::vector<TTabletId>>(request->cell_ids());
        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());
        TInstant expirationTime = TInstant::Zero();
        if (request->has_expiration_time()) {
            expirationTime = FromProto<TInstant>(request->expiration_time());
        }
        auto expirationTimeout = request->has_expiration_timeout()
            ? std::optional(FromProto<TDuration>(request->expiration_timeout()))
            : std::nullopt;
        std::optional<int> tabletCount = request->has_tablet_count()
            ? std::optional(request->tablet_count())
            : std::nullopt;

        TGuid correlationId;
        if (request->has_correlation_id()) {
            FromProto(&correlationId, request->correlation_id());
        }

        std::vector<TTabletBase*> tablets;
        std::vector<TTabletCell*> cells;

        for (auto tabletId : tabletIds) {
            tablets.push_back(GetTabletOrThrow(tabletId));
        }

        for (auto cellId : cellIds) {
            cells.push_back(GetTabletCellOrThrow(cellId));
        }

        try {
            CreateTabletAction(
                NullObjectId,
                kind,
                tablets,
                cells,
                pivotKeys,
                tabletCount,
                /*skipFreezing*/ false,
                correlationId,
                expirationTime,
                expirationTimeout);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(TError(ex), "Error creating tablet action (Kind: %v, "
                "Tablets: %v, TabletCells: %v, PivotKeys: %v, TabletCount: %v, TabletBalancerCorrelationId: %v)",
                kind,
                tablets,
                cells,
                pivotKeys,
                tabletCount,
                correlationId);
        }
    }

    void HydraDestroyTabletActions(NProto::TReqDestroyTabletActions* request)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto actionIds = FromProto<std::vector<TTabletActionId>>(request->tablet_action_ids());
        for (const auto& id : actionIds) {
            auto* action = FindTabletAction(id);
            if (IsObjectAlive(action)) {
                UnbindTabletAction(action);
                objectManager->UnrefObject(action);
            }
        }
    }

    void HydraSetTabletCellBundleResourceUsage(NProto::TReqSetTabletCellBundleResourceUsage* request)
    {
        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR("Received tablet cell bundle "
                "resource usage gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO("Received tablet cell bundle "
            "resource usage gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto bundleId = FromProto<TTabletCellBundleId>(entry.bundle_id());
            auto* bundle = FindTabletCellBundle(bundleId);
            if (!IsObjectAlive(bundle)) {
                continue;
            }

            auto newResourceUsage = FromProto<TTabletResources>(entry.resource_usage());
            if (multicellManager->IsPrimaryMaster()) {
                *bundle->ResourceUsage().Remote(cellTag) = newResourceUsage;
            } else {
                bundle->ResourceUsage().Cluster() = newResourceUsage;
            }
        }
    }

    void HydraUpdateTabletCellBundleResourceUsage(NProto::TReqUpdateTabletCellBundleResourceUsage* /*request*/)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        YT_VERIFY(multicellManager->IsPrimaryMaster());

        for (auto* bundleBase : GetValuesSortedByKey(cellManager->CellBundles(ECellarType::Tablet))) {
            if (!IsObjectAlive(bundleBase)) {
                continue;
            }

            YT_VERIFY(bundleBase->GetType() == EObjectType::TabletCellBundle);
            auto* bundle = bundleBase->As<TTabletCellBundle>();
            bundle->RecomputeClusterResourceUsage();
        }
    }

    void OnTabletCellBundleResourceUsageGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        YT_LOG_INFO("Sending tablet cell bundle resource usage gossip");

        NProto::TReqSetTabletCellBundleResourceUsage request;
        request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto* bundleBase : cellManager->CellBundles(ECellarType::Tablet)) {
            if (!IsObjectAlive(bundleBase)) {
                continue;
            }

            YT_VERIFY(bundleBase->GetType() == EObjectType::TabletCellBundle);
            auto* bundle = bundleBase->As<TTabletCellBundle>();
            auto* entry = request.add_entries();
            ToProto(entry->mutable_bundle_id(), bundle->GetId());

            if (multicellManager->IsPrimaryMaster()) {
                ToProto(entry->mutable_resource_usage(), bundle->ResourceUsage().Cluster());
            } else {
                ToProto(entry->mutable_resource_usage(), bundle->ResourceUsage().Local());
            }
        }

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTagSentinel, false);
        }

        if (multicellManager->IsMulticell() && multicellManager->IsPrimaryMaster()) {
            NProto::TReqUpdateTabletCellBundleResourceUsage request;
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger));
        }
    }

    void ValidateResourceUsageIncrease(
        const TTabletOwnerBase* table,
        const TTabletResources& delta,
        TAccount* account = nullptr) const
    {
        // Old-fashioned account validation.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidateResourceUsageIncrease(
            account ? account : table->Account().Get(),
            ConvertToClusterResources(delta));

        // Brand-new bundle validation.
        if (GetDynamicConfig()->EnableTabletResourceValidation) {
            const auto& bundle = table->TabletCellBundle();
            if (!bundle) {
                YT_LOG_ALERT("Failed to validate tablet resource usage increase since table lacks tablet cell bundle "
                    "(TableId: %v, Delta: %v)",
                    table->GetId(),
                    delta);
                return;
            }
            bundle->ValidateResourceUsageIncrease(delta);
        }
    }

    void UpdateResourceUsage(
        TTabletOwnerBase* table,
        const TTabletResources& delta,
        bool scheduleTableDataStatisticsUpdate = true)
    {
        // Old-fashioned account accounting.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateTabletResourceUsage(
            table,
            ConvertToClusterResources(delta));

        // Brand-new bundle accounting.
        const auto& bundle = table->TabletCellBundle();
        if (!bundle) {
            YT_LOG_ALERT("Failed to update tablet resource usage since table lacks tablet cell bundle "
                "(TableId: %v, Delta: %v)",
                table->GetId(),
                delta);
            return;
        }
        bundle->UpdateResourceUsage(delta);

        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(table, scheduleTableDataStatisticsUpdate);
    }

    void OnProfiling()
    {
        if (!IsLeader()) {
            return;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            return;
        }

        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        for (auto* bundleBase : cellManager->CellBundles(ECellarType::Tablet)) {
            if (!IsObjectAlive(bundleBase)) {
                continue;
            }

            YT_VERIFY(bundleBase->GetType() == EObjectType::TabletCellBundle);
            auto* bundle = bundleBase->As<TTabletCellBundle>();
            auto counters = GetOrCreateBundleProfilingCounters(bundle);
            bundle->OnProfiling(&counters);
        }
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& dynamicConfig = GetDynamicConfig();

        TabletCellDecommissioner_->Start();
        TabletBalancer_->Start();
        TabletActionManager_->Start();

        TabletCellStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
            BIND(&TImpl::OnTabletCellStatisticsGossip, MakeWeak(this)),
            dynamicConfig->MulticellGossip->TabletCellStatisticsGossipPeriod);
        TabletCellStatisticsGossipExecutor_->Start();

        BundleResourceUsageGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
            BIND(&TImpl::OnTabletCellBundleResourceUsageGossip, MakeWeak(this)),
            dynamicConfig->MulticellGossip->BundleResourceUsageGossipPeriod);
        BundleResourceUsageGossipExecutor_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            dynamicConfig->ProfilingPeriod);
        ProfilingExecutor_->Start();

        Bootstrap_->GetReplicatedTableTrackerStateProvider()->EnableStateMonitoring();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        TabletCellDecommissioner_->Stop();
        TabletBalancer_->Stop();
        TabletActionManager_->Stop();

        if (TabletCellStatisticsGossipExecutor_) {
            YT_UNUSED_FUTURE(TabletCellStatisticsGossipExecutor_->Stop());
            TabletCellStatisticsGossipExecutor_.Reset();
        }

        if (BundleResourceUsageGossipExecutor_) {
            YT_UNUSED_FUTURE(BundleResourceUsageGossipExecutor_->Stop());
            BundleResourceUsageGossipExecutor_.Reset();
        }

        if (ProfilingExecutor_) {
            YT_UNUSED_FUTURE(ProfilingExecutor_->Stop());
            ProfilingExecutor_.Reset();
        }

        BundleIdToProfilingCounters_.clear();

        Bootstrap_->GetReplicatedTableTrackerStateProvider()->DisableStateMonitoring();
    }


    bool CheckHasHealthyCells(TTabletCellBundle* bundle)
    {
        for (auto* cellBase : bundle->Cells()) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            if (!IsCellActive(cell)) {
                continue;
            }
            if (cell->IsHealthy()) {
                return true;
            }
        }

        return false;
    }

    void ValidateHasHealthyCells(TTabletCellBundle* bundle)
    {
        if (!CheckHasHealthyCells(bundle)) {
            THROW_ERROR_EXCEPTION("No healthy tablet cells in bundle %Qv",
                bundle->GetName());
        }
    }

    bool IsCellActive(TTabletCell* cell)
    {
        return IsObjectAlive(cell) && !cell->IsDecommissionStarted();
    }

    TTabletCell* FindTabletCell(TTabletCellId id)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCell(id);
        if (cell && cell->GetType() != EObjectType::TabletCell) {
            return nullptr;
        }
        return cell->As<TTabletCell>();
    }


    std::vector<std::pair<TTabletBase*, TTabletCell*>> ComputeTabletAssignment(
        TTabletOwnerBase* table,
        TTabletCell* hintCell,
        std::vector<TTabletBase*> tabletsToMount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (IsCellActive(hintCell)) {
            std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;
            for (auto* tablet : tabletsToMount) {
                assignment.emplace_back(tablet, hintCell);
            }
            return assignment;
        }

        struct TCellKey
        {
            i64 Size;
            TTabletCell* Cell;

            //! Compares by |(size, cellId)|.
            bool operator < (const TCellKey& other) const
            {
                if (Size < other.Size) {
                    return true;
                } else if (Size > other.Size) {
                    return false;
                }
                return Cell->GetId() < other.Cell->GetId();
            }
        };

        auto mutationContext = GetCurrentMutationContext();

        auto getCellSize = [&] (const TTabletCell* cell) -> i64 {
            i64 result = 0;
            i64 tabletCount;
            switch (table->GetInMemoryMode()) {
                case EInMemoryMode::None:
                    result = mutationContext->RandomGenerator()->Generate<i64>();
                    break;
                case EInMemoryMode::Uncompressed:
                case EInMemoryMode::Compressed: {
                    result += cell->GossipStatistics().Local().MemorySize;
                    tabletCount = cell->GossipStatistics().Local().TabletCountPerMemoryMode[EInMemoryMode::Uncompressed] +
                        cell->GossipStatistics().Local().TabletCountPerMemoryMode[EInMemoryMode::Compressed];
                    result += tabletCount * GetDynamicConfig()->TabletDataSizeFootprint;
                    break;
                }
                default:
                    YT_ABORT();
            }
            return result;
        };

        std::vector<TCellKey> cellKeys;
        for (auto* cellBase : GetValuesSortedByKey(table->TabletCellBundle()->Cells())) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            if (!IsCellActive(cell)) {
                continue;
            }

            if (cell->CellBundle() == table->TabletCellBundle()) {
                cellKeys.push_back(TCellKey{getCellSize(cell), cell});
            }
        }
        if (cellKeys.empty()) {
            cellKeys.push_back(TCellKey{0, nullptr});
        }
        std::sort(cellKeys.begin(), cellKeys.end());

        auto getTabletSize = [&] (const TTabletBase* tablet) -> i64 {
            i64 result = 0;
            auto statistics = tablet->GetTabletStatistics();
            switch (table->GetInMemoryMode()) {
                case EInMemoryMode::None:
                case EInMemoryMode::Uncompressed:
                    result += statistics.UncompressedDataSize;
                    break;
                case EInMemoryMode::Compressed:
                    result += statistics.CompressedDataSize;
                    break;
                default:
                    YT_ABORT();
            }
            result += GetDynamicConfig()->TabletDataSizeFootprint;
            return result;
        };

        // Sort tablets by decreasing size to improve greedy heuristic performance.
        std::sort(
            tabletsToMount.begin(),
            tabletsToMount.end(),
            [&] (const TTabletBase* lhs, const TTabletBase* rhs) {
                return
                    std::tuple(getTabletSize(lhs), lhs->GetId()) >
                    std::tuple(getTabletSize(rhs), rhs->GetId());
            });

        // Assign tablets to cells iteratively looping over cell array.
        int cellIndex = 0;
        std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;
        for (auto* tablet : tabletsToMount) {
            assignment.emplace_back(tablet, cellKeys[cellIndex].Cell);
            if (++cellIndex == std::ssize(cellKeys)) {
                cellIndex = 0;
            }
        }

        return assignment;
    }

    void DoUnmount(
        TTabletOwnerBase* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex,
        bool onDestroy)
    {
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            UnmountTablet(tablet, force, onDestroy);
        }
    }

    void DoUnmountTabletBase(TTabletBase* tablet, bool force)
    {
        auto state = tablet->GetState();
        if (!force) {
            YT_VERIFY(
                state == ETabletState::Mounted ||
                state == ETabletState::Frozen ||
                state == ETabletState::Freezing ||
                state == ETabletState::Unmounting);
        }

        auto* table = tablet->GetOwner();

        auto* cell = tablet->Servant().GetCell();
        YT_VERIFY(cell);

        YT_LOG_DEBUG(
            "Unmounting tablet (TableId: %v, TabletId: %v, CellId: %v, Force: %v)",
            table->GetId(),
            tablet->GetId(),
            GetObjectId(tablet->Servant().GetCell()),
            force);

        tablet->SetState(ETabletState::Unmounting);
    }

    void DoUnmountTablet(
        TTablet* tablet,
        bool force,
        bool onDestroy)
    {
        if (tablet->GetState() == ETabletState::Unmounted) {
            return;
        }

        DoUnmountTabletBase(tablet, force);

        YT_VERIFY(tablet->Servant());

        auto& servant = tablet->Servant();
        auto& auxiliaryServant = tablet->AuxiliaryServant();

        if (auxiliaryServant) {
            if (force) {
                DeallocateAuxiliaryServant(tablet);
            } else {
                auxiliaryServant.SetState(ETabletState::Unmounting);
            }
        }

        auto* cell = servant.GetCell();
        if (!cell) {
            return;
        }

        servant.SetState(ETabletState::Unmounting);

        TReqUnmountTablet request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_force(force);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(force
            ? tablet->GetCell()->GetId()
            : tablet->GetNodeEndpointId());
        hiveManager->PostMessage(mailbox, request);

        for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
            auto* replica = it->first;
            auto& replicaInfo = it->second;
            if (replica->TransitioningTablets().count(tablet) > 0) {
                StopReplicaTransition(tablet, replica, &replicaInfo, ETableReplicaState::None);
            }
            CheckTransitioningReplicaTablets(replica);
        }

        if (force) {
            AbandonDynamicStores(tablet);
            // NB: CopyChunkListIfShared may be called. It expects the table to be the owning node
            // of the root chunk list, which is not the case upon destruction.
            if (!onDestroy) {
                DiscardDynamicStores(tablet);
            }
            TabletChunkManager_->SetTabletEdenStoreIds(tablet, {});

            YT_VERIFY(tablet->Servant().GetState() != ETabletState::Unmounted);
            DoTabletServantUnmounted(tablet, &tablet->Servant(), /*force*/ true);
        }
    }

    void DoUnmountHunkTablet(THunkTablet* tablet, bool force)
    {
        if (tablet->GetState() == ETabletState::Unmounted) {
            return;
        }

        // NB: Cell can be destroyed in DoUnmountTabletBase.
        auto cellId = tablet->GetCell()->GetId();

        DoUnmountTabletBase(tablet, force);

        auto& servant = tablet->Servant();
        if (!servant) {
            return;
        }

        servant.SetState(ETabletState::Unmounting);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        {
            TReqUnmountHunkTablet request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_force(force);
            auto* mailbox = hiveManager->GetMailbox(cellId);
            hiveManager->PostMessage(mailbox, request);
        }

        if (force) {
            DoTabletServantUnmounted(tablet, &tablet->Servant(), force);
        }
    }

    void UnmountTablet(
        TTabletBase* tablet,
        bool force,
        bool onDestroy)
    {
        switch (tablet->GetType()) {
            case EObjectType::Tablet:
                DoUnmountTablet(tablet->As<TTablet>(), force, onDestroy);
                break;
            case EObjectType::HunkTablet:
                DoUnmountHunkTablet(tablet->As<THunkTablet>(), force);
                break;
            default:
                YT_ABORT();
        }
    }

    void ValidateTabletStaticMemoryUpdate(
        const TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        i64 memorySize = 0;

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = table->Tablets()[index];
            if (tablet->GetState() != ETabletState::Unmounted) {
                continue;
            }
            memorySize += tablet->GetTabletStaticMemorySize(table->GetInMemoryMode());
        }

        ValidateResourceUsageIncrease(
            table,
            TTabletResources().SetTabletStaticMemory(memorySize));
    }

    static EStoreType GetStoreType(
        const TTableNode* table,
        const TChunkTree* chunkOrView)
    {
        if (IsPhysicalChunkType(chunkOrView->GetType())) {
            const auto* chunk = chunkOrView->AsChunk();
            if (chunk->GetChunkType() == EChunkType::Hunk) {
                return EStoreType::HunkChunk;
            }
        }
        return table->IsPhysicallySorted() ? EStoreType::SortedChunk : EStoreType::OrderedChunk;
    }

    void FillStoreDescriptor(
        const TTableNode* table,
        const TChunkTree* chunkOrView,
        NTabletNode::NProto::TAddStoreDescriptor* descriptor,
        i64* startingRowIndex)
    {
        descriptor->set_store_type(ToProto<int>(GetStoreType(table, chunkOrView)));
        ToProto(descriptor->mutable_store_id(), chunkOrView->GetId());

        const TChunk* chunk;
        if (chunkOrView->GetType() == EObjectType::ChunkView) {
            const auto* chunkView = chunkOrView->AsChunkView();
            chunk = chunkView->GetUnderlyingTree()->AsChunk();
            auto* viewDescriptor = descriptor->mutable_chunk_view_descriptor();
            ToProto(viewDescriptor->mutable_chunk_view_id(), chunkView->GetId());
            ToProto(viewDescriptor->mutable_underlying_chunk_id(), chunk->GetId());
            ToProto(viewDescriptor->mutable_read_range(), chunkView->ReadRange());

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            if (auto overrideTimestamp = transactionManager->GetTimestampHolderTimestamp(
                chunkView->GetTransactionId()))
            {
                viewDescriptor->set_override_timestamp(overrideTimestamp);
            }

            if (auto maxClipTimestamp = chunkView->GetMaxClipTimestamp()) {
                viewDescriptor->set_max_clip_timestamp(maxClipTimestamp);
            }
        } else {
            chunk = chunkOrView->AsChunk();
        }

        ToProto(descriptor->mutable_chunk_meta(), chunk->ChunkMeta());
        descriptor->set_starting_row_index(*startingRowIndex);
        *startingRowIndex += chunk->GetRowCount();
    }

    void FillHunkChunkDescriptor(
        const TChunk* chunk,
        NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor)
    {
        ToProto(descriptor->mutable_chunk_id(), chunk->GetId());
        ToProto(descriptor->mutable_chunk_meta(), chunk->ChunkMeta());
    }

    void ValidateNodeCloneMode(TTabletOwnerBase* trunkNode, ENodeCloneMode mode)
    {
        if (IsTableType(trunkNode->GetType())) {
            DoValidateNodeCloneMode(trunkNode->As<TTableNode>(), mode);
        } else if (trunkNode->GetType() == EObjectType::HunkStorage) {
            DoValidateNodeCloneMode(trunkNode->As<THunkStorageNode>(), mode);
        } else {
            YT_ABORT();
        }
    }

    void DoValidateNodeCloneMode(TTableNode* trunkNode, ENodeCloneMode mode)
    {
        try {
            switch (mode) {
                case ENodeCloneMode::Copy:
                    trunkNode->ValidateAllTabletsFrozenOrUnmounted("Cannot copy dynamic table");
                    break;

                case ENodeCloneMode::Move:
                    if (trunkNode->IsPhysicallyLog()) {
                        THROW_ERROR_EXCEPTION("Cannot move a table of type %Qlv",
                            trunkNode->GetType());
                    }
                    trunkNode->ValidateAllTabletsUnmounted("Cannot move dynamic table");
                    break;

                case ENodeCloneMode::Backup:
                    trunkNode->ValidateNotBackup("Cannot backup a backup table");
                    if (trunkNode->IsPhysicallyLog() && !trunkNode->IsReplicated()) {
                        THROW_ERROR_EXCEPTION("Cannot backup a table of type %Qlv",
                            trunkNode->GetType());
                    }
                    break;

                case ENodeCloneMode::Restore:
                    if (trunkNode->GetBackupState() != ETableBackupState::BackupCompleted) {
                        THROW_ERROR_EXCEPTION("Cannot restore table since it is not a backup table");
                    }
                    if (trunkNode->IsPhysicallyLog() && !trunkNode->IsReplicated()) {
                        THROW_ERROR_EXCEPTION("Cannot restore a table of type %Qlv",
                            trunkNode->GetType());
                    }
                    break;

                default:
                    YT_ABORT();
            }
        } catch (const std::exception& ex) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            THROW_ERROR_EXCEPTION("Error cloning table %v",
                cypressManager->GetNodePath(trunkNode->GetTrunkNode(), trunkNode->GetTransaction()))
                << ex;
        }
    }

    void DoValidateNodeCloneMode(THunkStorageNode* trunkNode, ENodeCloneMode mode)
    {
        try {
            switch (mode) {
                case ENodeCloneMode::Move:
                case ENodeCloneMode::Copy:
                case ENodeCloneMode::Backup:
                case ENodeCloneMode::Restore:
                    THROW_ERROR_EXCEPTION("Hunk storage does not support clone mode %Qlv", mode);

                default:
                    YT_ABORT();
            }
        } catch (const std::exception& ex) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            THROW_ERROR_EXCEPTION("Error cloning hunk storage %v",
                cypressManager->GetNodePath(trunkNode->GetTrunkNode(), trunkNode->GetTransaction()))
                << ex;
        }
    }

    static void PopulateTableReplicaDescriptor(TTableReplicaDescriptor* descriptor, const TTableReplica* replica, const TTableReplicaInfo& info)
    {
        ToProto(descriptor->mutable_replica_id(), replica->GetId());
        descriptor->set_cluster_name(replica->GetClusterName());
        descriptor->set_replica_path(replica->GetReplicaPath());
        descriptor->set_start_replication_timestamp(replica->GetStartReplicationTimestamp());
        descriptor->set_mode(ToProto<int>(replica->GetMode()));
        descriptor->set_preserve_timestamps(replica->GetPreserveTimestamps());
        descriptor->set_atomicity(ToProto<int>(replica->GetAtomicity()));
        info.Populate(descriptor->mutable_statistics());
    }

    static void PopulateTableReplicaInfoFromStatistics(
        TTableReplicaInfo* info,
        const NTabletClient::NProto::TTableReplicaStatistics& statistics)
    {
        // Updates may be reordered but we can rely on monotonicity here.
        info->SetCommittedReplicationRowIndex(std::max(
            info->GetCommittedReplicationRowIndex(),
            statistics.committed_replication_row_index()));
        info->SetCurrentReplicationTimestamp(std::max(
            info->GetCurrentReplicationTimestamp(),
            statistics.current_replication_timestamp()));
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTabletBase, TabletMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TableReplica, TTableReplica, TableReplicaMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletAction, TTabletAction, TabletActionMap_);

////////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTabletManager::~TTabletManager() = default;

void TTabletManager::Initialize()
{
    return Impl_->Initialize();
}

IYPathServicePtr TTabletManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

const ITabletChunkManagerPtr& TTabletManager::GetTabletChunkManager() const
{
    return Impl_->GetTabletChunkManager();
}

void TTabletManager::PrepareMount(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCellId hintCellId,
    const std::vector<TTabletCellId>& targetCellIds,
    bool freeze)
{
    Impl_->PrepareMount(
        table,
        firstTabletIndex,
        lastTabletIndex,
        hintCellId,
        targetCellIds,
        freeze);
}

void TTabletManager::PrepareUnmount(
    TTabletOwnerBase* table,
    bool force,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareUnmount(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareRemount(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareRemount(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareFreeze(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareFreeze(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareUnfreeze(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareUnfreeze(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareReshard(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<TLegacyOwningKey>& pivotKeys,
    const std::vector<i64>& trimmedRowCounts,
    bool create)
{
    Impl_->PrepareReshard(
        table,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys,
        trimmedRowCounts,
        create);
}

void TTabletManager::ValidateMakeTableDynamic(TTableNode* table)
{
    Impl_->ValidateMakeTableDynamic(table);
}

void TTabletManager::ValidateMakeTableStatic(TTableNode* table)
{
    Impl_->ValidateMakeTableStatic(table);
}

void TTabletManager::Mount(
    TTabletOwnerBase* table,
    const TString& path,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCellId hintCellId,
    const std::vector<TTabletCellId>& targetCellIds,
    bool freeze,
    TTimestamp mountTimestamp)
{
    Impl_->Mount(
        table,
        path,
        firstTabletIndex,
        lastTabletIndex,
        hintCellId,
        targetCellIds,
        freeze,
        mountTimestamp);
}

void TTabletManager::Unmount(
    TTabletOwnerBase* table,
    bool force,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->Unmount(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::Remount(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->Remount(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::Freeze(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->Freeze(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::Unfreeze(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->Unfreeze(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::Reshard(
    TTabletOwnerBase* table,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<TLegacyOwningKey>& pivotKeys,
    const std::vector<i64>& trimmedRowCounts)
{
    Impl_->Reshard(
        table,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys,
        trimmedRowCounts);
}

void TTabletManager::ValidateCloneTabletOwner(
    TTabletOwnerBase* sourceNode,
    ENodeCloneMode mode,
    TAccount* account)
{
    return Impl_->ValidateCloneTabletOwner(
        sourceNode,
        mode,
        account);
}

void TTabletManager::ValidateBeginCopyTabletOwner(
    TTabletOwnerBase* sourceNode,
    ENodeCloneMode mode)
{
    return Impl_->ValidateBeginCopyTabletOwner(
        sourceNode,
        mode);
}

void TTabletManager::CloneTabletOwner(
    TTabletOwnerBase* sourceNode,
    TTabletOwnerBase* clonedNode,
    ENodeCloneMode mode)
{
    return Impl_->CloneTabletOwner(
        sourceNode,
        clonedNode,
        mode);
}

void TTabletManager::MakeTableDynamic(TTableNode* table, i64 trimmedRowCount)
{
    Impl_->MakeTableDynamic(table, trimmedRowCount);
}

void TTabletManager::MakeTableStatic(TTableNode* table)
{
    Impl_->MakeTableStatic(table);
}

void TTabletManager::LockDynamicTable(
    TTableNode* table,
    TTransaction* transaction,
    TTimestamp timestamp)
{
    Impl_->LockDynamicTable(table, transaction, timestamp);
}

void TTabletManager::CheckDynamicTableLock(
    TTableNode* table,
    TTransaction* transaction,
    NTableClient::NProto::TRspCheckDynamicTableLock* response)
{
    Impl_->CheckDynamicTableLock(table, transaction, response);
}

TTabletBase* TTabletManager::GetTabletOrThrow(TTabletId id)
{
    return Impl_->GetTabletOrThrow(id);
}

TTabletCell* TTabletManager::GetTabletCellOrThrow(TTabletCellId id)
{
    return Impl_->GetTabletCellOrThrow(id);
}

TTabletCellBundle* TTabletManager::GetTabletCellBundleOrThrow(TTabletCellBundleId id)
{
    return Impl_->GetTabletCellBundleOrThrow(id);
}

TTabletCellBundle* TTabletManager::FindTabletCellBundle(TTabletCellBundleId id)
{
    return Impl_->FindTabletCellBundle(id);
}

TTabletCellBundle* TTabletManager::GetTabletCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly)
{
    return Impl_->GetTabletCellBundleByNameOrThrow(name, activeLifeStageOnly);
}

TTabletCellBundle* TTabletManager::GetDefaultTabletCellBundle()
{
    return Impl_->GetDefaultTabletCellBundle();
}

void TTabletManager::SetTabletCellBundle(TTabletOwnerBase* owner, TTabletCellBundle* cellBundle)
{
    Impl_->SetTabletCellBundle(owner, cellBundle);
}

void TTabletManager::ZombifyTabletCell(TTabletCell* cell)
{
    Impl_->ZombifyTabletCell(cell);
}

void TTabletManager::DestroyTablet(TTabletBase* tablet)
{
    Impl_->DestroyTablet(tablet);
}

void TTabletManager::DestroyTabletOwner(TTabletOwnerBase* table)
{
    Impl_->DestroyTabletOwner(table);
}

TNode* TTabletManager::FindTabletLeaderNode(const TTabletBase* tablet) const
{
    return Impl_->FindTabletLeaderNode(tablet);
}

void TTabletManager::UpdateExtraMountConfigKeys(std::vector<TString> keys)
{
    Impl_->UpdateExtraMountConfigKeys(std::move(keys));
}

TTableReplica* TTabletManager::CreateTableReplica(
    TReplicatedTableNode* table,
    const TString& clusterName,
    const TYPath& replicaPath,
    ETableReplicaMode mode,
    bool preserveTimestamps,
    EAtomicity atomicity,
    bool enabled,
    TTimestamp startReplicationTimestamp,
    const std::optional<std::vector<i64>>& startReplicationRowIndexes,
    bool enableReplicatedTableTracker)
{
    return Impl_->CreateTableReplica(
        table,
        clusterName,
        replicaPath,
        mode,
        preserveTimestamps,
        atomicity,
        enabled,
        startReplicationTimestamp,
        startReplicationRowIndexes,
        enableReplicatedTableTracker);
}

void TTabletManager::ZombifyTableReplica(TTableReplica* replica)
{
    Impl_->ZombifyTableReplica(replica);
}

void TTabletManager::AlterTableReplica(
    TTableReplica* replica,
    std::optional<bool> enabled,
    std::optional<ETableReplicaMode> mode,
    std::optional<EAtomicity> atomicity,
    std::optional<bool> preserveTimestamps,
    std::optional<bool> enableReplicatedTableTracker)
{
    Impl_->AlterTableReplica(
        replica,
        std::move(enabled),
        std::move(mode),
        std::move(atomicity),
        std::move(preserveTimestamps),
        std::move(enableReplicatedTableTracker));
}

std::vector<TTabletActionId> TTabletManager::SyncBalanceCells(
    TTabletCellBundle* bundle,
    const std::optional<std::vector<TTableNode*>>& tables,
    bool keepActions)
{
    return Impl_->SyncBalanceCells(bundle, tables, keepActions);
}

std::vector<TTabletActionId> TTabletManager::SyncBalanceTablets(TTableNode* table, bool keepActions)
{
    return Impl_->SyncBalanceTablets(table, keepActions);
}

TTabletAction* TTabletManager::CreateTabletAction(
    NObjectClient::TObjectId hintId,
    ETabletActionKind kind,
    const std::vector<TTabletBase*>& tablets,
    const std::vector<TTabletCell*>& cells,
    const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
    const std::optional<int>& tabletCount,
    bool skipFreezing,
    TGuid correlationId,
    TInstant expirationTime,
    std::optional<TDuration> expirationTimeout)
{
    return Impl_->CreateTabletAction(
        hintId,
        kind,
        tablets,
        cells,
        pivotKeys,
        tabletCount,
        skipFreezing,
        correlationId,
        expirationTime,
        expirationTimeout);
}

void TTabletManager::DestroyTabletAction(TTabletAction* action)
{
    Impl_->DestroyTabletAction(action);
}

void TTabletManager::MergeTable(TTableNode* originatingNode, NTableServer::TTableNode* branchedNode)
{
    Impl_->MergeTable(originatingNode, branchedNode);
}

void TTabletManager::OnNodeStorageParametersUpdated(TChunkOwnerBase* node)
{
    Impl_->OnNodeStorageParametersUpdated(node);
}

void TTabletManager::RecomputeTabletCellStatistics(TCellBase* cellBase)
{
    return Impl_->RecomputeTabletCellStatistics(cellBase);
}

void TTabletManager::OnHunkJournalChunkSealed(TChunk* chunk)
{
    Impl_->OnHunkJournalChunkSealed(chunk);
}

void TTabletManager::AttachDynamicStoreToTablet(TTablet* tablet, TDynamicStore* dynamicStore)
{
    Impl_->AttachDynamicStoreToTablet(tablet, dynamicStore);
}

void TTabletManager::FireUponTableReplicaUpdate(TTableReplica* replica)
{
    Impl_->FireUponTableReplicaUpdate(replica);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTabletBase, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TableReplica, TTableReplica, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletAction, TTabletAction, *Impl_);

DELEGATE_SIGNAL_WITH_ACCESSOR(TTabletManager, void(TReplicatedTableData), ReplicatedTableCreated, *Impl_);
DELEGATE_SIGNAL_WITH_ACCESSOR(TTabletManager, void(TTableId), ReplicatedTableDestroyed, *Impl_);
DELEGATE_SIGNAL(TTabletManager, void(TReplicaData), ReplicaCreated, *Impl_);
DELEGATE_SIGNAL(TTabletManager, void(TTableReplicaId), ReplicaDestroyed, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
