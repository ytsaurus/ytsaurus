#include "balancing_helpers.h"
#include "config.h"
#include "cypress_integration.h"
#include "private.h"
#include "table_replica.h"
#include "table_replica_type_handler.h"
#include "tablet.h"
#include "tablet_action_manager.h"
#include "tablet_balancer.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_type_handler.h"
#include "tablet_cell_decommissioner.h"
#include "tablet_cell_type_handler.h"
#include "tablet_manager.h"
#include "tablet_resources.h"
#include "tablet_service.h"
#include "tablet_type_handler.h"
#include "tablet_node_tracker.h"

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
#include <yt/yt/server/master/chunk_server/dynamic_store.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>
#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/subject.h>

#include <yt/yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/master/table_server/shared_table_schema.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <algorithm>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
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
using namespace NSecurityServer;
using namespace NTableClient::NProto;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletNodeTrackerClient::NProto;
using namespace NTransactionServer;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCellServer;
using namespace NProfiling;

using NNodeTrackerClient::TNodeDescriptor;
using NTabletNode::EStoreType;
using NTabletNode::TTableMountConfigPtr;
using NTabletNode::DynamicStoreIdPoolSize;
using NTransactionServer::TTransaction;

using NSecurityServer::ConvertToTabletResources;
using NSecurityServer::ConvertToClusterResources;

using NYT::FromProto;
using NYT::ToProto;

using TTabletResources = NTabletServer::TTabletResources;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

static constexpr auto ProfilingPeriod = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TProfilingCounters
{
    TProfilingCounters() = default;

    explicit TProfilingCounters(const TProfiler& profiler)
        : CopyChunkListIfSharedActionCount(profiler.Counter("/copy_chunk_list_if_shared/action_count"))
        , UpdateTabletStoresStoreCount(profiler.Counter("/update_tablet_stores/store_count"))
        , UpdateTabletStoreTime(profiler.TimeCounter("/update_tablet_stores/cumulative_time"))
        , CopyChunkListTime(profiler.TimeCounter("/copy_chunk_list_if_shared/cumulative_time"))
    { }

    TCounter CopyChunkListIfSharedActionCount;
    TCounter UpdateTabletStoresStoreCount;
    TTimeCounter UpdateTabletStoreTime;
    TTimeCounter CopyChunkListTime;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap,  NCellMaster::EAutomatonThreadQueue::TabletManager)
        , TabletService_(New<TTabletService>(Bootstrap_))
        , TabletBalancer_(New<TTabletBalancer>(Bootstrap_))
        , TabletCellDecommissioner_(New<TTabletCellDecommissioner>(Bootstrap_))
        , TabletActionManager_(New<TTabletActionManager>(Bootstrap_))
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
        objectManager->RegisterHandler(CreateTabletTypeHandler(Bootstrap_, &TabletMap_));
        objectManager->RegisterHandler(CreateTableReplicaTypeHandler(Bootstrap_, &TableReplicaMap_));
        objectManager->RegisterHandler(CreateTabletActionTypeHandler(Bootstrap_, &TabletActionMap_));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionAborted, MakeWeak(this)));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUpdateTabletStores, MakeStrong(this))));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeAfterSnapshotLoaded(BIND(&TImpl::OnAfterCellManagerSnapshotLoaded, MakeWeak(this)));
        cellManager->SubscribeCellBundleDestroyed(BIND(&TImpl::OnTabletCellBundleDestroyed, MakeWeak(this)));
        cellManager->SubscribeCellDecommissionStarted(BIND(&TImpl::OnTabletCellDecommissionStarted, MakeWeak(this)));

        TabletService_->Initialize();
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

    TTablet* GetTabletOrThrow(TTabletId id)
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

    TTablet* CreateTablet(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Tablet, NullObjectId);
        auto tabletHolder = std::make_unique<TTablet>(id);
        tabletHolder->SetTable(table);

        auto* tablet = TabletMap_.Insert(id, std::move(tabletHolder));
        objectManager->RefObject(tablet);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet created (TableId: %v, TabletId: %v, Account: %v)",
            table->GetId(),
            tablet->GetId(),
            table->GetAccount()->GetName());

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // XXX(savrus): this is a workaround for YTINCIDENTS-42
        if (auto* cell = tablet->GetCell()) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Destroying tablet with non-null tablet cell (TabletId: %v, CellId: %v)",
                tablet->GetId(),
                cell->GetId());

            // Replicated table not supported.
            YT_VERIFY(tablet->Replicas().empty());

            if (cell->Tablets().erase(tablet) > 0) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Unbinding tablet from tablet cell since tablet is destroyed (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    cell->GetId());
            }

            if (tablet->GetState() == ETabletState::Mounted) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Sending force unmount request to node since tablet is destroyed (TabletId: %v, CellId: %v)",
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

        YT_VERIFY(!tablet->GetTable());

        if (auto* action = tablet->GetAction()) {
            OnTabletActionTabletsTouched(
                action,
                THashSet<TTablet*>{tablet},
                TError("Tablet %v has been removed", tablet->GetId()));
        }
    }


    TTableReplica* CreateTableReplica(
        TReplicatedTableNode* table,
        const TString& clusterName,
        const TYPath& replicaPath,
        ETableReplicaMode mode,
        bool preserveTimestamps,
        NTransactionClient::EAtomicity atomicity,
        TTimestamp startReplicationTimestamp,
        const std::optional<std::vector<i64>>& startReplicationRowIndexes)
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

        if (!preserveTimestamps && atomicity == NTransactionClient::EAtomicity::None) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::InvalidTabletState,
                "Cannot set atomicity %v with preserveTimestamps %v",
                atomicity,
                preserveTimestamps);
        }

        YT_VERIFY(!startReplicationRowIndexes || startReplicationRowIndexes->size() == table->Tablets().size());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TableReplica, NullObjectId);
        auto replicaHolder = std::make_unique<TTableReplica>(id);
        replicaHolder->SetTable(table);
        replicaHolder->SetClusterName(clusterName);
        replicaHolder->SetReplicaPath(replicaPath);
        replicaHolder->SetMode(mode);
        replicaHolder->SetPreserveTimestamps(preserveTimestamps);
        replicaHolder->SetAtomicity(atomicity);
        replicaHolder->SetStartReplicationTimestamp(startReplicationTimestamp);
        replicaHolder->SetState(ETableReplicaState::Disabled);

        auto* replica = TableReplicaMap_.Insert(id, std::move(replicaHolder));
        objectManager->RefObject(replica);

        YT_VERIFY(table->Replicas().insert(replica).second);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica created (TableId: %v, ReplicaId: %v, Mode: %v, StartReplicationTimestamp: %llx)",
            table->GetId(),
            replica->GetId(),
            mode,
            startReplicationTimestamp);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (int tabletIndex = 0; tabletIndex < table->Tablets().size(); ++tabletIndex) {
            auto* tablet = table->Tablets()[tabletIndex];
            auto pair = tablet->Replicas().emplace(replica, TTableReplicaInfo());
            YT_VERIFY(pair.second);
            auto& replicaInfo = pair.first->second;

            if (startReplicationRowIndexes) {
                replicaInfo.SetCurrentReplicationRowIndex((*startReplicationRowIndexes)[tabletIndex]);
            }

            if (!tablet->IsActive()) {
                replicaInfo.SetState(ETableReplicaState::None);
                continue;
            }

            replicaInfo.SetState(ETableReplicaState::Disabled);

            auto* cell = tablet->GetCell();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            TReqAddTableReplica req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            PopulateTableReplicaDescriptor(req.mutable_replica(), replica, replicaInfo);
            hiveManager->PostMessage(mailbox, req);
        }

        return replica;
    }

    void DestroyTableReplica(TTableReplica* replica)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* table = replica->GetTable();
        if (table) {
            YT_VERIFY(table->Replicas().erase(replica) == 1);

            const auto& hiveManager = Bootstrap_->GetHiveManager();
            for (auto* tablet : table->Tablets()) {
                YT_VERIFY(tablet->Replicas().erase(replica) == 1);

                if (!tablet->IsActive()) {
                    continue;
                }

                auto* cell = tablet->GetCell();
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                TReqRemoveTableReplica req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                ToProto(req.mutable_replica_id(), replica->GetId());
                hiveManager->PostMessage(mailbox, req);
            }
        }
    }

    void AlterTableReplica(
        TTableReplica* replica,
        std::optional<bool> enabled,
        std::optional<ETableReplicaMode> mode,
        std::optional<NTransactionClient::EAtomicity> atomicity,
        std::optional<bool> preserveTimestamps)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* table = replica->GetTable();
        auto state = replica->GetState();

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

        if (!preserveTimestamps && atomicity == NTransactionClient::EAtomicity::None) {
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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica updated (TableId: %v, ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v)",
            table->GetId(),
            replica->GetId(),
            enabled,
            mode,
            atomicity,
            preserveTimestamps);

        if (mode) {
            replica->SetMode(*mode);
        }

        if (atomicity) {
            replica->SetAtomicity(*atomicity);
        }

        if (preserveTimestamps) {
            replica->SetPreserveTimestamps(*preserveTimestamps);
        }

        if (enabled) {
            if (*enabled) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Enabling table replica (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabling);
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Disabling table replica (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Disabling);
            }
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (auto* tablet : table->Tablets()) {
            if (!tablet->IsActive()) {
                continue;
            }

            auto* replicaInfo = tablet->GetReplicaInfo(replica);

            auto* cell = tablet->GetCell();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
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
        const std::vector<TTablet*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablets.empty()) {
            THROW_ERROR_EXCEPTION("Invalid number of tablets: expected more than zero");
        }

        auto* table = tablets[0]->GetTable();

        // Validate that table is not in process of mount/unmount/etc.
        table->ValidateNoCurrentMountTransaction("Cannot create tablet action");

        for (const auto* tablet : tablets) {
            if (tablet->GetTable() != table) {
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

        auto* bundle = table->GetTabletCellBundle();

        for (auto* cell : cells) {
            if (!IsCellActive(cell)) {
                THROW_ERROR_EXCEPTION("Tablet cell %v is not active", cell->GetId());
            }

            if (cell->GetCellBundle() != bundle) {
                THROW_ERROR_EXCEPTION("Table %v and tablet cell %v belong to different bundles",
                    table->GetId(),
                    cell->GetId());
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(bundle, EPermission::Use);

        switch (kind) {
            case ETabletActionKind::Move:
                if (cells.size() != 0 && cells.size() != tablets.size()) {
                    THROW_ERROR_EXCEPTION("Number of destination cells and tablets mismatch: %v tablets, %v cells",
                        cells.size());
                }
                if (pivotKeys.size() != 0) {
                    THROW_ERROR_EXCEPTION("Invalid number of pivot keys: expected 0, actual %v",
                        pivotKeys.size());
                }
                if (!!tabletCount) {
                    THROW_ERROR_EXCEPTION("Invalid number of tablets: expected std::nullopt, actual %v",
                        *tabletCount);
                }
                break;

            case ETabletActionKind::Reshard:
                if (cells.size() != 0 && cells.size() != pivotKeys.size()) {
                    THROW_ERROR_EXCEPTION("Number of destination cells and pivot keys mismatch: pivot keys %v, cells %",
                        cells.size());
                }
                if (pivotKeys.size() == 0 && (!tabletCount || *tabletCount < 1)) {
                    THROW_ERROR_EXCEPTION("Invalid number of new tablets: expected pivot keys or tablet count greater than 1");
                }
                for (int index = 1; index < tablets.size(); ++index) {
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
            expirationTime);

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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet action destroyed (ActionId: %v, TabletBalancerCorrelationId: %v)",
            action->GetId(),
            action->GetCorrelationId());
    }


    TTabletStatistics GetTabletStatistics(const TTablet* tablet)
    {
        const auto* table = tablet->GetTable();
        const auto* tabletChunkList = tablet->GetChunkList();
        const auto& treeStatistics = tabletChunkList->Statistics();
        const auto& nodeStatistics = tablet->NodeStatistics();

        TTabletStatistics tabletStatistics;
        tabletStatistics.PartitionCount = nodeStatistics.partition_count();
        tabletStatistics.StoreCount = nodeStatistics.store_count();
        tabletStatistics.PreloadPendingStoreCount = nodeStatistics.preload_pending_store_count();
        tabletStatistics.PreloadCompletedStoreCount = nodeStatistics.preload_completed_store_count();
        tabletStatistics.PreloadFailedStoreCount = nodeStatistics.preload_failed_store_count();
        tabletStatistics.OverlappingStoreCount = nodeStatistics.overlapping_store_count();
        tabletStatistics.DynamicMemoryPoolSize = nodeStatistics.dynamic_memory_pool_size();
        tabletStatistics.UnmergedRowCount = treeStatistics.RowCount;
        tabletStatistics.UncompressedDataSize = treeStatistics.UncompressedDataSize;
        tabletStatistics.CompressedDataSize = treeStatistics.CompressedDataSize;
        switch (tablet->GetInMemoryMode()) {
            case EInMemoryMode::Compressed:
                tabletStatistics.MemorySize = tabletStatistics.CompressedDataSize;
                break;
            case EInMemoryMode::Uncompressed:
                tabletStatistics.MemorySize = tabletStatistics.UncompressedDataSize;
                break;
            case EInMemoryMode::None:
                tabletStatistics.MemorySize = 0;
                break;
            default:
                YT_ABORT();
        }
        for (const auto& entry : table->Replication()) {
            tabletStatistics.DiskSpacePerMedium[entry.GetMediumIndex()] = CalculateDiskSpaceUsage(
                entry.Policy().GetReplicationFactor(),
                treeStatistics.RegularDiskSpace,
                treeStatistics.ErasureDiskSpace);
        }
        tabletStatistics.ChunkCount = treeStatistics.ChunkCount;
        tabletStatistics.TabletCount = 1;
        tabletStatistics.TabletCountPerMemoryMode[tablet->GetInMemoryMode()] = 1;
        return tabletStatistics;
    }


    void PrepareMountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot mount a static table");
        }

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        auto validateCellBundle = [table] (const TTabletCell* cell) {
            if (cell->GetCellBundle() != table->GetTabletCellBundle()) {
                THROW_ERROR_EXCEPTION("Cannot mount tablets into cell %v since it belongs to bundle %Qv while the table "
                    "is configured to use bundle %Qv",
                    cell->GetId(),
                    cell->GetCellBundle()->GetName(),
                    table->GetTabletCellBundle()->GetName());
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
                if (!targetCellIds.empty() && targetCellIds.size() != tabletCount) {
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
            ValidateHasHealthyCells(table->GetTabletCellBundle()); // may throw
        }

        const auto& allTablets = table->Tablets();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = allTablets[index];
            auto state = tablet->GetState();
            if (state != ETabletState::Unmounted && (freeze
                    ? state != ETabletState::Frozen &&
                        state != ETabletState::Freezing &&
                        state != ETabletState::FrozenMounting
                    : state != ETabletState::Mounted &&
                        state != ETabletState::Mounting &&
                        state != ETabletState::Unfreezing))
            {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        auto tableSettings = GetTableSettings(table);
        ValidateTableMountConfig(table, tableSettings.MountConfig);
        ValidateTabletStaticMemoryUpdate(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tableSettings.MountConfig);

        if (tableSettings.MountConfig->InMemoryMode != EInMemoryMode::None &&
            tableSettings.StoreWriterOptions->ErasureCodec != NErasure::ECodec::None)
        {
            THROW_ERROR_EXCEPTION("Cannot mount erasure coded table in memory");
        }

        // Check for store duplicates.
        auto* rootChunkList = table->GetChunkList();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = allTablets[index];
            auto* tabletChunkList = rootChunkList->Children()[index]->AsChunkList();

            std::vector<TChunkTree*> stores;
            EnumerateStoresInChunkTree(tabletChunkList, &stores);

            THashSet<TObjectId> storeSet;
            storeSet.reserve(stores.size());
            for (auto* store : stores) {
                if (!storeSet.insert(store->GetId()).second) {
                    THROW_ERROR_EXCEPTION("Cannot mount table: tablet %v contains duplicate store %v of type %Qlv",
                        tablet->GetId(),
                        store->GetId(),
                        store->GetType());
                }
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "mount_table");
    }

    void MountTable(
        TTableNode* table,
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

        auto tableSettings = GetTableSettings(table);
        auto serializedTableSettings = SerializeTableSettings(tableSettings);

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        std::vector<std::pair<TTablet*, TTabletCell*>> assignment;

        if (!targetCellIds.empty()) {
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = allTablets[index];
                if (!tablet->GetCell()) {
                    auto* cell = FindTabletCell(targetCellIds[index - firstTabletIndex]);
                    assignment.emplace_back(tablet, cell);
                }
            }
        } else {
            std::vector<TTablet*> tabletsToMount;
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = allTablets[index];
                if (!tablet->GetCell()) {
                    tabletsToMount.push_back(tablet);
                }
            }

            assignment = ComputeTabletAssignment(
                table,
                tableSettings.MountConfig,
                hintCell,
                std::move(tabletsToMount));
        }

        DoMountTablets(
            table,
            assignment,
            tableSettings.MountConfig->InMemoryMode,
            freeze,
            serializedTableSettings,
            mountTimestamp);

        UpdateTabletState(table);
    }

    void PrepareUnmountTable(
        TTableNode* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot unmount a static table");
        }

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
                auto state = tablet->GetState();
                if (state != ETabletState::Mounted &&
                    state != ETabletState::Frozen &&
                    state != ETabletState::Freezing &&
                    state != ETabletState::Unmounted &&
                    state != ETabletState::Unmounting)
                {
                    THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                        tablet->GetId(),
                        state);
                }

                for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
                    const auto* replica = it->first;
                    const auto& replicaInfo = it->second;
                    if (replica->TransitioningTablets().count(tablet) > 0) {
                        THROW_ERROR_EXCEPTION("Cannot unmount tablet %v since replica %v is in %Qlv state",
                            tablet->GetId(),
                            replica->GetId(),
                            replicaInfo.GetState());
                    }
                }
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "unmount_table");
    }

    void UnmountTable(
        TTableNode* table,
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

        DoUnmountTable(table, force, firstTabletIndex, lastTabletIndex, /*onDestroy*/ false);
        UpdateTabletState(table);
    }

    void PrepareRemountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot remount a static table");
        }

        if (table->IsNative()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (table->IsExternal()) {
            return;
        }

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        auto tableSettings = GetTableSettings(table);
        ValidateTableMountConfig(table, tableSettings.MountConfig);

        if (tableSettings.MountConfig->InMemoryMode != EInMemoryMode::None &&
            tableSettings.StoreWriterOptions->ErasureCodec != NErasure::ECodec::None)
        {
            THROW_ERROR_EXCEPTION("Cannot mount erasure coded table in memory");
        }
    }

    void RemountTable(
        TTableNode* table,
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

        auto tableSettings = GetTableSettings(table);
        auto serializedTableSettings = SerializeTableSettings(tableSettings);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();
            auto state = tablet->GetState();

            if (state != ETabletState::Unmounted) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Remounting tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                YT_VERIFY(tablet->GetInMemoryMode() == tableSettings.MountConfig->InMemoryMode);

                const auto& hiveManager = Bootstrap_->GetHiveManager();

                TReqRemountTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                FillTableSettings(&request, serializedTableSettings);

                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }

        UpdateResourceUsage(table, table->GetTabletResourceUsage() - resourceUsageBefore);
    }

    void PrepareFreezeTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot freeze a static table");
        }

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
            auto state = tablet->GetState();
            if (state != ETabletState::Mounted &&
                state != ETabletState::FrozenMounting &&
                state != ETabletState::Freezing &&
                state != ETabletState::Frozen)
            {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "freeze_table");
    }

    void FreezeTable(
        TTableNode* table,
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

    void PrepareUnfreezeTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot unfreeze a static table");
        }

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
            auto state = tablet->GetState();
            if (state != ETabletState::Mounted &&
                state != ETabletState::Frozen &&
                state != ETabletState::Unfreezing)
            {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "unfreeze_table");
    }

    void UnfreezeTable(
        TTableNode* table,
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

    void PrepareReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        bool create = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        // First, check parameters with little knowledge of the table.
        // Primary master must ensure that the table could be created.

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        if (newTabletCount <= 0) {
            THROW_ERROR_EXCEPTION("Tablet count must be positive");
        }

        if (newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
                MaxTabletCount);
        }

        if (table->DynamicTableLocks().size() > 0) {
            THROW_ERROR_EXCEPTION("Dynamic table is locked by some bulk insert");
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        if (!create && !table->IsForeign()) {
            securityManager->ValidatePermission(table, EPermission::Mount);
        }

        if (create) {
            int oldTabletCount = table->IsExternal() ? 0 : 1;
            ValidateResourceUsageIncrease(
                table,
                TTabletResources().SetTabletCount(newTabletCount - oldTabletCount));
        }

        if (table->IsSorted()) {
            // NB: We allow reshard without pivot keys.
            // Pivot keys will be calculated when ReshardTable is called so we don't need to check them.
            if (!pivotKeys.empty()) {
                if (pivotKeys.size() != newTabletCount) {
                    THROW_ERROR_EXCEPTION("Wrong pivot key count: %v instead of %v",
                        pivotKeys.size(),
                        newTabletCount);
                }

                // Validate first pivot key (on primary master before the table is created).
                if (firstTabletIndex == 0 && pivotKeys[0] != EmptyKey()) {
                    THROW_ERROR_EXCEPTION("First pivot key must be empty");
                }

                for (int index = 0; index < static_cast<int>(pivotKeys.size()) - 1; ++index) {
                    if (pivotKeys[index] >= pivotKeys[index + 1]) {
                        THROW_ERROR_EXCEPTION("Pivot keys must be strictly increasing");
                    }
                }

                // Validate pivot keys against table schema.
                for (const auto& pivotKey : pivotKeys) {
                    ValidatePivotKey(pivotKey, table->GetTableSchema());
                }
            }

            if (!table->IsPhysicallySorted() && pivotKeys.empty()) {
                THROW_ERROR_EXCEPTION("Pivot keys must be provided to reshard a replicated table");
            }
        } else {
            if (!pivotKeys.empty()) {
                THROW_ERROR_EXCEPTION("Table is ordered; must provide tablet count");
            }
        }

        if (table->IsExternal()) {
            return;
        }

        if (table->IsReplicated() && !table->IsLogicallyEmpty()) {
            THROW_ERROR_EXCEPTION("Cannot reshard non-empty replicated table");
        }

        // Now check against tablets.
        // This is a job of secondary master in a two-phase commit.
        // Should not throw when table is created.

        const auto& tablets = table->Tablets();
        YT_VERIFY(tablets.size() == table->GetChunkList()->Children().size());

        ParseTabletRangeOrThrow(table, &firstTabletIndex, &lastTabletIndex); // may throw

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;

        if (tablets.size() - oldTabletCount + newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
                MaxTabletCount);
        }

        ValidateResourceUsageIncrease(
            table,
            TTabletResources().SetTabletCount(newTabletCount - oldTabletCount));

        if (table->IsSorted()) {
            // NB: We allow reshard without pivot keys.
            // Pivot keys will be calculated when ReshardTable is called so we don't need to check them.
            if (!pivotKeys.empty()) {
                if (pivotKeys[0] != tablets[firstTabletIndex]->GetPivotKey()) {
                    THROW_ERROR_EXCEPTION(
                        "First pivot key must match that of the first tablet "
                        "in the resharded range");
                }

                if (lastTabletIndex != tablets.size() - 1) {
                    if (pivotKeys.back() >= tablets[lastTabletIndex + 1]->GetPivotKey()) {
                        THROW_ERROR_EXCEPTION(
                            "Last pivot key must be strictly less than that of the tablet "
                            "which follows the resharded range");
                    }
                }
            }
        }

        // Validate that all affected tablets are unmounted.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = tablets[index];
            if (tablet->GetState() != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION("Cannot reshard table since tablet %v is not unmounted",
                    tablet->GetId());
            }
        }

        // For ordered tablets, if the number of tablets decreases then validate that the trailing ones
        // (which we are about to drop) are properly trimmed.
        if (newTabletCount < oldTabletCount) {
            for (int index = firstTabletIndex + newTabletCount; index < firstTabletIndex + oldTabletCount; ++index) {
                const auto* tablet = tablets[index];
                const auto& chunkListStatistics = tablet->GetChunkList()->Statistics();
                if (tablet->GetTrimmedRowCount() != chunkListStatistics.LogicalRowCount - chunkListStatistics.RowCount) {
                    THROW_ERROR_EXCEPTION("Some chunks of tablet %v are not fully trimmed; such a tablet cannot "
                        "participate in resharding",
                        tablet->GetId());
                }
            }
        }

        // Do after all validations.
        TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "reshard_table");
    }

    void ReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys)
    {
        if (table->IsExternal()) {
            UpdateTabletState(table);
            return;
        }

        DoReshardTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            pivotKeys);

        UpdateTabletState(table);
    }

    void DestroyTable(TTableNode* table)
    {
        if (!table->Tablets().empty()) {
            int firstTabletIndex = 0;
            int lastTabletIndex = static_cast<int>(table->Tablets().size()) - 1;

            TouchAffectedTabletActions(table, firstTabletIndex, lastTabletIndex, "remove");

            DoUnmountTable(table, true, firstTabletIndex, lastTabletIndex, /*onDestroy*/ true);

            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* tablet : table->Tablets()) {
                tablet->SetTable(nullptr);
                YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);
                objectManager->UnrefObject(tablet);
            }

            YT_VERIFY(!table->IsExternal());
            auto* bundle = table->GetTabletCellBundle();
            bundle->UpdateResourceUsage(-table->GetTabletResourceUsage());

            table->MutableTablets().clear();

            // NB: security manager has already been informed when node's account was reset.
        }

        if (auto* bundle = table->GetTabletCellBundle()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(bundle);
            table->SetTabletCellBundle(nullptr);
        }

        if (table->GetType() == EObjectType::ReplicatedTable) {
            auto* replicatedTable = table->As<TReplicatedTableNode>();
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                replica->SetTable(nullptr);
                replica->TransitioningTablets().clear();
                objectManager->UnrefObject(replica);
            }
            replicatedTable->Replicas().clear();
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (auto [transactionId, lock] : table->DynamicTableLocks()) {
            auto* transaction = transactionManager->FindTransaction(transactionId);
            if (!IsObjectAlive(transaction)) {
                continue;
            }

            transaction->LockedDynamicTables().erase(table);
        }
    }

    void MergeTable(TTableNode* originatingNode, TTableNode* branchedNode)
    {
        YT_VERIFY(originatingNode->IsTrunk());

        auto updateMode = branchedNode->GetUpdateMode();
        if (updateMode == EUpdateMode::Append) {
            CopyChunkListIfShared(originatingNode, 0, originatingNode->Tablets().size() - 1);
        }

        auto* originatingChunkList = originatingNode->GetChunkList();
        auto* branchedChunkList = branchedNode->GetChunkList();
        auto* transaction = branchedNode->GetTransaction();

        YT_VERIFY(originatingNode->IsPhysicallySorted());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        transaction->LockedDynamicTables().erase(originatingNode);

        i64 totalMemorySizeDelta = 0;

        // Deaccumulate old tablet statistics.
        for (int index = 0; index < branchedChunkList->Children().size(); ++index) {
            auto* tablet = originatingNode->Tablets()[index];

            auto tabletStatistics = GetTabletStatistics(tablet);
            originatingNode->DiscountTabletStatistics(tabletStatistics);

            if (tablet->GetState() != ETabletState::Unmounted) {
                totalMemorySizeDelta -= tablet->GetTabletStaticMemorySize();

                auto* cell = tablet->GetCell();
                cell->GossipStatistics().Local() -= tabletStatistics;
            }
        }

        // Replace root chunk list.
        if (updateMode == EUpdateMode::Overwrite) {
            originatingChunkList->RemoveOwningNode(originatingNode);
            branchedChunkList->AddOwningNode(originatingNode);
            originatingNode->SetChunkList(branchedChunkList);
        }

        // Merge tablet chunk lists and accumulate new tablet statistics.
        for (int index = 0; index < branchedChunkList->Children().size(); ++index) {
            auto* appendChunkList = branchedChunkList->Children()[index]->AsChunkList();
            auto* tabletChunkList = originatingChunkList->Children()[index]->AsChunkList();
            auto* tablet = originatingNode->Tablets()[index];

            if (updateMode == EUpdateMode::Overwrite) {
                AbandonDynamicStores(tablet);
                YT_VERIFY(appendChunkList->GetKind() == EChunkListKind::SortedDynamicTablet);
                appendChunkList->SetPivotKey(tabletChunkList->GetPivotKey());
            }

            if (updateMode == EUpdateMode::Append) {
                if (!appendChunkList->Children().empty()) {
                    chunkManager->AttachToChunkList(tabletChunkList, appendChunkList);
                }
            }

            auto newStatistics = GetTabletStatistics(tablet);
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
            req.set_mount_revision(tablet->GetMountRevision());
            req.set_commit_timestamp(static_cast<i64>(
                transactionManager->GetTimestampHolderTimestamp(transaction->GetId())));
            req.set_update_mode(ToProto<int>(updateMode));

            auto stores = EnumerateStoresInChunkTree(appendChunkList);

            i64 startingRowIndex = 0;
            for (const auto* store : stores) {
                auto* descriptor = req.add_stores_to_add();
                FillStoreDescriptor(originatingNode, store, descriptor, &startingRowIndex);
            }

            if (updateMode == EUpdateMode::Overwrite &&
                tablet->GetState() == ETabletState::Mounted &&
                IsDynamicStoreReadEnabled(originatingNode))
            {
                CreateAndAttachDynamicStores(tablet, &req);
            }

            auto* mailbox = hiveManager->GetMailbox(tablet->GetCell()->GetId());
            hiveManager->PostMessage(mailbox, req);
        }

        // The rest of TChunkOwner::DoMerge later unconditionally replaces statistics of
        // originating node with the ones of branched node. Since dynamic stores are already
        // attached, we have to account them this way.
        branchedNode->SnapshotStatistics() = originatingNode->GetChunkList()->Statistics().ToDataStatistics();

        auto resourceUsageDelta = TTabletResources()
            .SetTabletStaticMemory(totalMemorySizeDelta);

        UpdateResourceUsage(originatingNode, resourceUsageDelta);

        originatingNode->RemoveDynamicTableLock(transaction->GetId());

        if (updateMode == EUpdateMode::Append) {
            chunkManager->ClearChunkList(branchedChunkList);
        }
    }


    TGuid GenerateTabletBalancerCorrelationId() const
    {
        auto mutationContext = GetCurrentMutationContext();
        auto& gen = mutationContext->RandomGenerator();
        ui64 lo = gen.Generate<ui64>();
        ui64 hi = gen.Generate<ui64>();
        return TGuid(lo, hi);
    }

    TTabletActionId SpawnTabletAction(const TReshardDescriptor& descriptor)
    {
        std::vector<TTabletId> tabletIds;
        for (const auto& tablet : descriptor.Tablets) {
            tabletIds.push_back(tablet->GetId());
        }

        auto* table = descriptor.Tablets[0]->GetTable();

        auto correlationId = GenerateTabletBalancerCorrelationId();

        YT_LOG_DEBUG("Automatically resharding tablets "
            "(TableId: %v, TabletIds: %v, NewTabletCount: %v, TotalSize: %v, Bundle: %v, "
            "TabletBalancerCorrelationId: %v, Sync: true)",
            table->GetId(),
            tabletIds,
            descriptor.TabletCount,
            descriptor.DataSize,
            table->GetTabletCellBundle()->GetName(),
            correlationId);

        try {
            auto* action = CreateTabletAction(
                TObjectId{},
                ETabletActionKind::Reshard,
                descriptor.Tablets,
                {}, // cells
                {}, // pivotKeys
                descriptor.TabletCount,
                false, // skipFreezing
                correlationId,
                TInstant::Zero());
            return action->GetId();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to create tablet action during sync reshard (TabletBalancerCorrelationId: %v)",
                correlationId);
            return NullObjectId;
        }

    }

    TTabletActionId SpawnTabletAction(const TTabletMoveDescriptor& descriptor)
    {
        auto* table = descriptor.Tablet->GetTable();

        auto correlationId = GenerateTabletBalancerCorrelationId();

        YT_LOG_DEBUG("Moving tablet during cell balancing "
            "(TableId: %v, InMemoryMode: %v, TabletId: %v, SrcCellId: %v, DstCellId: %v, "
            "Bundle: %v, TabletBalancerCorrelationId: %v, Sync: true)",
            table->GetId(),
            table->GetInMemoryMode(),
            descriptor.Tablet->GetId(),
            descriptor.Tablet->GetCell()->GetId(),
            descriptor.TabletCellId,
            table->GetTabletCellBundle()->GetName(),
            correlationId);

        try {
            auto* action = CreateTabletAction(
                TObjectId{},
                ETabletActionKind::Move,
                {descriptor.Tablet},
                {GetTabletCellOrThrow(descriptor.TabletCellId)},
                {}, // pivotKeys
                std::nullopt, // tabletCount
                false, // skipFreezing
                correlationId,
                TInstant::Zero());
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
            return {};
        }

        std::optional<THashSet<const TTableNode*>> tablesSet;
        if (tables) {
            tablesSet = THashSet<const TTableNode*>(tables->begin(), tables->end());
        }

        std::vector<TTabletActionId> actions;
        TTabletBalancerContext context;
        auto descriptors = ReassignInMemoryTablets(
            bundle,
            tablesSet,
            true, // ignoreConfig
            Bootstrap_->GetTabletManager());

        for (auto descriptor : descriptors) {
            if (auto actionId = SpawnTabletAction(descriptor)) {
                actions.push_back(actionId);
            }
        }

        if (keepActions) {
            SetSyncTabletActionsKeepalive(actions);
        }

        return actions;
    }

    std::vector<TTabletActionId> SyncBalanceTablets(
        TTableNode* table,
        bool keepActions)
    {
        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        if (table->IsReplicated() && !table->IsEmpty()) {
            THROW_ERROR_EXCEPTION("Cannot reshard non-empty replicated table");
        }

        for (const auto& tablet : table->Tablets()) {
            if (tablet->GetAction()) {
                return {};
            }
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        std::vector<TTabletActionId> actions;
        TTabletBalancerContext context;
        for (const auto& tablet : table->Tablets()) {
            if (!IsTabletReshardable(tablet, /*ignoreConfig*/ true) || !context.IsTabletUntouched(tablet)) {
                continue;
            }

            auto bounds = GetTabletSizeConfig(tablet);

            auto descriptors = MergeSplitTablet(tablet, bounds, &context, tabletManager);

            for (auto descriptor : descriptors) {
                if (auto actionId = SpawnTabletAction(descriptor)) {
                    actions.push_back(actionId);
                }
            }
        }

        if (keepActions) {
            SetSyncTabletActionsKeepalive(actions);
        }
        return actions;
    }


    void ValidateCloneTable(
        TTableNode* sourceTable,
        ENodeCloneMode mode,
        TAccount* account)
    {
        if (sourceTable->IsForeign()) {
            return;
        }

        auto* trunkSourceTable = sourceTable->GetTrunkNode();

        ValidateNodeCloneMode(trunkSourceTable, mode);

        if (auto* cellBundle = trunkSourceTable->GetTabletCellBundle()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle);
        }

        ValidateResourceUsageIncrease(
            trunkSourceTable,
            TTabletResources().SetTabletCount(
                trunkSourceTable->GetTabletResourceUsage().TabletCount),
            account);
    }

    void ValidateBeginCopyTable(
        TTableNode* sourceTable,
        ENodeCloneMode mode)
    {
        YT_VERIFY(sourceTable->IsNative());

        auto* trunkSourceTable = sourceTable->GetTrunkNode();
        ValidateNodeCloneMode(trunkSourceTable, mode);

        auto* cellBundle = trunkSourceTable->GetTabletCellBundle();
        if (cellBundle) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle);
        }
    }

    void ValidateEndCopyTable(TAccount* /*account*/)
    { }

    void CloneTable(
        TTableNode* sourceTable,
        TTableNode* clonedTable,
        ENodeCloneMode mode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(sourceTable->IsExternal() == clonedTable->IsExternal());

        if (sourceTable->IsExternal()) {
            return;
        }

        auto* trunkSourceTable = sourceTable->GetTrunkNode();
        auto* trunkClonedTable = clonedTable; // sic!

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

                default:
                    YT_ABORT();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), ex, "Error cloning table (TableId: %v, %v)",
                sourceTable->GetId(),
                NRpc::GetCurrentAuthenticationIdentity());
        }

        // Undo the harm done in TChunkOwnerTypeHandler::DoClone.
        auto* fakeClonedRootChunkList = trunkClonedTable->GetChunkList();
        fakeClonedRootChunkList->RemoveOwningNode(trunkClonedTable);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(fakeClonedRootChunkList);

        const auto& sourceTablets = trunkSourceTable->Tablets();
        YT_VERIFY(!sourceTablets.empty());
        auto& clonedTablets = trunkClonedTable->MutableTablets();
        YT_VERIFY(clonedTablets.empty());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* clonedRootChunkList = chunkManager->CreateChunkList(fakeClonedRootChunkList->GetKind());
        trunkClonedTable->SetChunkList(clonedRootChunkList);
        objectManager->RefObject(clonedRootChunkList);
        clonedRootChunkList->AddOwningNode(trunkClonedTable);

        clonedTablets.reserve(sourceTablets.size());
        auto* sourceRootChunkList = trunkSourceTable->GetChunkList();
        YT_VERIFY(sourceRootChunkList->Children().size() == sourceTablets.size());
        for (int index = 0; index < static_cast<int>(sourceTablets.size()); ++index) {
            const auto* sourceTablet = sourceTablets[index];

            auto* clonedTablet = CreateTablet(trunkClonedTable);
            clonedTablet->CopyFrom(*sourceTablet);

            auto* tabletChunkList = sourceRootChunkList->Children()[index];
            chunkManager->AttachToChunkList(clonedRootChunkList, tabletChunkList);

            clonedTablets.push_back(clonedTablet);
            trunkClonedTable->AccountTabletStatistics(GetTabletStatistics(clonedTablet));
        }
        trunkClonedTable->RecomputeTabletMasterMemoryUsage();

        if (sourceTable->IsReplicated()) {
            auto* replicatedSourceTable = sourceTable->As<TReplicatedTableNode>();
            auto* replicatedClonedTable = clonedTable->As<TReplicatedTableNode>();

            YT_VERIFY(mode == ENodeCloneMode::Copy);

            for (const auto* replica : GetValuesSortedByKey(replicatedSourceTable->Replicas())) {
                std::vector<i64> currentReplicationRowIndexes;

                for (int tabletIndex = 0; tabletIndex < static_cast<int>(sourceTablets.size()); ++tabletIndex) {
                    auto* tablet = replicatedSourceTable->Tablets()[tabletIndex];
                    auto* replicaInfo = tablet->GetReplicaInfo(replica);
                    auto replicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
                    currentReplicationRowIndexes.push_back(replicationRowIndex);
                }

                CreateTableReplica(
                    replicatedClonedTable,
                    replica->GetClusterName(),
                    replica->GetReplicaPath(),
                    replica->GetMode(),
                    replica->GetPreserveTimestamps(),
                    replica->GetAtomicity(),
                    replica->GetStartReplicationTimestamp(),
                    currentReplicationRowIndexes);
            }
        }

        UpdateResourceUsage(
            trunkClonedTable,
            trunkClonedTable->GetTabletResourceUsage(),
            /*scheduleTableDataStatisticsUpdate*/ false);
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

    void MakeTableDynamic(TTableNode* table)
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

        auto* oldRootChunkList = table->GetChunkList();

        auto chunks = EnumerateChunksInChunkTree(oldRootChunkList);

        // Compute last commit timestamp.
        auto lastCommitTimestamp = NTransactionClient::MinTimestamp;
        for (auto* chunk : chunks) {
            const auto& miscExt = chunk->MiscExt();
            if (miscExt.has_max_timestamp()) {
                lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt.max_timestamp()));
            }
        }

        table->SetLastCommitTimestamp(lastCommitTimestamp);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* newRootChunkList = chunkManager->CreateChunkList(table->IsPhysicallySorted()
            ? EChunkListKind::SortedDynamicRoot
            : EChunkListKind::OrderedDynamicRoot);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(newRootChunkList);

        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);

        auto* tablet = CreateTablet(table);
        tablet->SetIndex(0);
        if (table->IsSorted()) {
            tablet->SetPivotKey(EmptyKey());
        }
        table->MutableTablets() = {tablet};
        table->RecomputeTabletMasterMemoryUsage();

        auto* tabletChunkList = chunkManager->CreateChunkList(table->IsPhysicallySorted()
            ? EChunkListKind::SortedDynamicTablet
            : EChunkListKind::OrderedDynamicTablet);
        if (table->IsPhysicallySorted()) {
            tabletChunkList->SetPivotKey(EmptyKey());
        }
        chunkManager->AttachToChunkList(newRootChunkList, tabletChunkList);

        std::vector<TChunkTree*> chunkTrees(chunks.begin(), chunks.end());
        chunkManager->AttachToChunkList(tabletChunkList, chunkTrees);

        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
        UpdateResourceUsage(
            table,
            table->GetTabletResourceUsage(),
            /*scheduleTableDataStatisticsUpdate*/ false);

        table->AccountTabletStatistics(GetTabletStatistics(tablet));

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table is switched to dynamic mode (TableId: %v)",
            table->GetId());
    }

    void ValidateMakeTableStatic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (!table->IsDynamic()) {
            return;
        }

        if (table->IsReplicated()) {
            THROW_ERROR_EXCEPTION("Cannot switch mode from dynamic to static: table is replicated");
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
            table->DiscountTabletStatistics(GetTabletStatistics(tablet));
        }

        auto tabletResourceUsage = table->GetTabletResourceUsage();

        auto* oldRootChunkList = table->GetChunkList();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto newRootChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(newRootChunkList);

        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);

        auto chunks = EnumerateChunksInChunkTree(oldRootChunkList);
        chunkManager->AttachToChunkList(newRootChunkList, std::vector<TChunkTree*>{chunks.begin(), chunks.end()});

        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        for (auto* tablet : table->Tablets()) {
            tablet->SetTable(nullptr);
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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table is switched to static mode (TableId: %v)",
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

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        int pendingTabletCount = 0;

        for (auto* tablet : table->Tablets()) {
            if (tablet->GetState() == ETabletState::Unmounted) {
                continue;
            }

            ++pendingTabletCount;
            YT_VERIFY(tablet->UnconfirmedDynamicTableLocks().emplace(transaction->GetId()).second);

            auto* cell = tablet->GetCell();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            TReqLockTablet req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            ToProto(req.mutable_lock()->mutable_transaction_id(), transaction->GetId());
            req.mutable_lock()->set_timestamp(static_cast<i64>(timestamp));
            hiveManager->PostMessage(mailbox, req);
        }

        transaction->LockedDynamicTables().insert(table);
        table->AddDynamicTableLock(transaction->GetId(), timestamp, pendingTabletCount);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Waiting for tablet lock confirmation (TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
            table->GetId(),
            transaction->GetId(),
            pendingTabletCount);

    }

    void CheckDynamicTableLock(
        TTableNode* table,
        TTransaction* transaction,
        NTableClient::NProto::TRspCheckDynamicTableLock* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        auto it = table->DynamicTableLocks().find(transaction->GetId());
        response->set_confirmed(it && it->second.PendingTabletCount == 0);
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

    void ZombifyTabletCell(TTabletCell *cell)
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

    TNode* FindTabletLeaderNode(const TTablet* tablet) const
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

        YT_VERIFY(leadingPeerId < cell->Peers().size());

        return cell->Peers()[leadingPeerId].Node;
    }


    TTabletCellBundle* FindTabletCellBundle(TTabletCellBundleId id)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* bundle = cellManager->FindCellBundle(id);
        if (!bundle) {
            return nullptr;
        }
        return bundle->GetType() == EObjectType::TabletCellBundle
            ? bundle->As<TTabletCellBundle>()
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

    void SetTabletCellBundle(TTableNode* table, TTabletCellBundle* newBundle)
    {
        YT_VERIFY(table->IsTrunk());

        auto* oldBundle = table->GetTabletCellBundle();
        if (oldBundle == newBundle) {
            return;
        }

        auto resourceUsageDelta = table->GetTabletResourceUsage();

        if (table->IsNative()) {
            if (table->IsDynamic()) {
                table->ValidateAllTabletsUnmounted("Cannot change tablet cell bundle");
                if (newBundle && GetDynamicConfig()->EnableTabletResourceValidation) {
                    newBundle->ValidateResourceUsageIncrease(resourceUsageDelta);
                }
            }
        }

        if (!table->IsExternal() && table->IsDynamic()) {
            if (oldBundle) {
                oldBundle->UpdateResourceUsage(-resourceUsageDelta);
            }
            if (newBundle) {
                newBundle->UpdateResourceUsage(resourceUsageDelta);
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        if (oldBundle) {
            objectManager->UnrefObject(oldBundle);
        }

        table->SetTabletCellBundle(newBundle);
        objectManager->RefObject(newBundle);
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
        for (auto [cellId, cellBase] : cellManager->Cells()) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            cell->GossipStatistics().Local() = NTabletServer::TTabletCellStatistics();
            for (const auto* tablet : cell->Tablets()) {
                cell->GossipStatistics().Local() += GetTabletStatistics(tablet);
            }
        }
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

private:
    const TTabletServicePtr TabletService_;
    const TTabletBalancerPtr TabletBalancer_;
    const TTabletCellDecommissionerPtr TabletCellDecommissioner_;
    const TTabletActionManagerPtr TabletActionManager_;

    TEntityMap<TTablet> TabletMap_;
    TEntityMap<TTableReplica> TableReplicaMap_;
    TEntityMap<TTabletAction> TabletActionMap_;

    TPeriodicExecutorPtr TabletCellStatisticsGossipExecutor_;
    TPeriodicExecutorPtr BundleResourceUsageGossipExecutor_;
    TPeriodicExecutorPtr ProfilingExecutor_;

    using TProfilerKey = std::tuple<std::optional<ETabletStoresUpdateReason>, TString, bool>;

    TTimeCounter TabletNodeHeartbeatCounter_ = TabletServerProfiler.TimeCounter("/tablet_node_heartbeat");
    THashMap<TProfilerKey, TProfilingCounters> Counters_;

    TProfilingCounters* GetCounters(std::optional<ETabletStoresUpdateReason> reason, TTableNode* table)
    {
        static TProfilingCounters nullCounters;
        if (IsRecovery()) {
            return &nullCounters;
        }

        if (!table->GetTabletCellBundle()) {
            return &nullCounters;
        }

        TProfilerKey key{reason, table->GetTabletCellBundle()->GetName(), table->IsPhysicallySorted()};
        auto it = Counters_.find(key);
        if (it != Counters_.end()) {
            return &it->second;
        }

        auto profiler = TabletServerProfiler
            .WithSparse()
            .WithTag("tablet_cell_bundle", std::get<TString>(key))
            .WithTag("table_type", table->IsPhysicallySorted() ? "sorted" : "ordered");

        if (reason) {
            profiler = profiler.WithTag("update_reason", FormatEnum(*reason));
        }

        it = Counters_.emplace(
            key,
            TProfilingCounters{profiler}).first;
        return &it->second;
    }

    THashMap<TTabletCellBundleId, TTabletCellBundleProfilingCounters>
        BundleIdToProfilingCounters_;

    TTabletCellBundleId DefaultTabletCellBundleId_;
    TTabletCellBundle* DefaultTabletCellBundle_ = nullptr;

    bool EnableUpdateStatisticsOnHeartbeat_ = true;
    bool RecomputeAggregateTabletStatistics_ = false;
    bool RecomputeBundleResourceUsage_ = false;

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
        if (cellBase->GetType() != EObjectType::TabletCell){
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
        auto* bundle = cellManager->FindCellBundleByName(name, false);
        if (!bundle) {
            return nullptr;
        }
        return bundle->GetType() == EObjectType::TabletCellBundle
            ? bundle->As<TTabletCellBundle>()
            : nullptr;
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
        const std::vector<TTablet*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        std::optional<int> tabletCount,
        bool freeze,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(state == ETabletActionState::Preparing || state == ETabletActionState::Orphaned);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletAction, hintId);
        auto actionHolder = std::make_unique<TTabletAction>(id);
        auto* action = TabletActionMap_.Insert(id, std::move(actionHolder));
        objectManager->RefObject(action);

        for (auto* tablet : tablets) {
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
        auto* bundle = action->Tablets()[0]->GetTable()->GetTabletCellBundle();
        action->SetTabletCellBundle(bundle);
        bundle->TabletActions().insert(action);
        bundle->IncreaseActiveTabletActionCount();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet action created (%v)",
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
            auto* tablet = table->Tablets()[index];
            THashSet<TStoreId> edenStoreIds(
                tablet->EdenStoreIds().begin(),
                tablet->EdenStoreIds().end());

            auto chunksOrViews = EnumerateStoresInChunkTree(tablet->GetChunkList());
            for (const auto* chunkOrView : chunksOrViews) {
                const auto* chunk = chunkOrView->GetType() == EObjectType::ChunkView
                    ? chunkOrView->AsChunkView()->GetUnderlyingChunk()
                    : chunkOrView->AsChunk();
                if (chunk->MiscExt().eden() || edenStoreIds.contains(chunkOrView->GetId())) {
                    continue;
                }

                auto size = chunk->MiscExt().uncompressed_data_size();
                entries.push_back({
                    GetMinKeyOrThrow(chunkOrView),
                    GetUpperBoundKeyOrThrow(chunkOrView),
                    size
                });
                totalSize += size;
            }
        }

        std::sort(entries.begin(), entries.end());

        i64 desired = totalSize / newTabletCount;
        std::vector<TLegacyOwningKey> pivotKeys{table->Tablets()[firstTabletIndex]->GetPivotKey()};
        TLegacyOwningKey lastKey;
        i64 current = 0;

        for (const auto& entry : entries) {
            if (lastKey && lastKey <= entry.MinKey) {
                if (current >= desired) {
                    current = 0;
                    pivotKeys.push_back(entry.MinKey);
                    lastKey = entry.MaxKey;
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

                if (!IsObjectAlive(tablet->GetTable())) {
                    continue;
                }

                switch (tablet->GetState()) {
                    case ETabletState::Mounted:
                        break;

                    case ETabletState::Unmounted:
                        DoMountTablet(tablet, nullptr, action->GetFreeze());
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
                YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error mounting missed in action tablet "
                    "(TabletId: %v, TableId: %v, Bundle: %v, ActionId: %v, TabletBalancerCorrelationId: %v)",
                    tablet->GetId(),
                    tablet->GetTable()->GetId(),
                    action->GetTabletCellBundle()->GetName(),
                    action->GetId(),
                    action->GetCorrelationId());
            }
        }
    }

    void OnTabletActionTabletsTouched(
        TTabletAction* action,
        const THashSet<TTablet*>& touchedTablets,
        const TError& error)
    {
        bool touched = false;
        for (auto* tablet : action->Tablets()) {
            if (touchedTablets.find(tablet) != touchedTablets.end()) {
                YT_VERIFY(tablet->GetAction() == action);
                tablet->SetAction(nullptr);
                touched = true;
            }
        }

        if (!touched) {
            return;
        }

        action->SaveTabletIds();

        auto& tablets = action->Tablets();
        tablets.erase(
            std::remove_if(
                tablets.begin(),
                tablets.end(),
                [&] (auto* tablet) {
                    return touchedTablets.find(tablet) != touchedTablets.end();
                }),
            tablets.end());

        UnbindTabletActionFromCells(action);
        OnTabletActionDisturbed(action, error);
    }

    void TouchAffectedTabletActions(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const TString& request)
    {
        YT_VERIFY(firstTabletIndex >= 0 && firstTabletIndex <= lastTabletIndex && lastTabletIndex < table->Tablets().size());

        auto error = TError("User request %Qv interfered with the action", request);
        THashSet<TTablet*> touchedTablets;
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
        auto tableId = action->Tablets().empty()
            ? TTableId{}
            : action->Tablets()[0]->GetTable()->GetId();
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Change tablet action state (ActionId: %v, State: %v, "
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
            action->Error() = error.Sanitize();
            ChangeTabletActionState(action, ETabletActionState::Failed);
            return;
        }

        switch (action->GetState()) {
            case ETabletActionState::Unmounting:
            case ETabletActionState::Freezing:
                // Wait until tablets are unmounted, then mount them.
                action->Error() = error.Sanitize();
                break;

            case ETabletActionState::Mounting:
                // Nothing can be done here.
                action->Error() = error.Sanitize();
                ChangeTabletActionState(action, ETabletActionState::Failed);
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
                action->Error() = TError(ex).Sanitize();
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
                    DoUnmountTablet(tablet, /*force*/ false, /*onDestroy*/ false);
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
                auto* table = action->Tablets().front()->GetTable();
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

                        std::vector<TTablet*> oldTablets;
                        oldTablets.swap(action->Tablets());
                        for (auto* tablet : oldTablets) {
                            YT_VERIFY(tablet->GetExpectedState() == expectedState);
                            tablet->SetAction(nullptr);
                        }

                        int newTabletCount = action->GetTabletCount()
                            ? *action->GetTabletCount()
                            : action->PivotKeys().size();

                        try {
                            // TODO(ifsmirnov) Use custom locking to allow reshard when locked by operation and upload has not been started yet.
                            const auto& cypressManager = Bootstrap_->GetCypressManager();
                            cypressManager->LockNode(table, nullptr, ELockMode::Exclusive);

                            PrepareReshardTable(
                                table,
                                firstTabletIndex,
                                lastTabletIndex,
                                newTabletCount,
                                action->PivotKeys());
                            newTabletCount = DoReshardTable(
                                table,
                                firstTabletIndex,
                                lastTabletIndex,
                                newTabletCount,
                                action->PivotKeys());
                        } catch (const std::exception& ex) {
                            for (auto* tablet : oldTablets) {
                                YT_VERIFY(IsObjectAlive(tablet));
                                tablet->SetAction(action);
                            }
                            action->Tablets() = std::move(oldTablets);
                            throw;
                        }

                        action->Tablets() = std::vector<TTablet*>(
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

                auto tableSettings = GetTableSettings(table);
                auto serializedTableSttings = SerializeTableSettings(tableSettings);

                std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
                if (action->TabletCells().empty()) {
                    if (!CheckHasHealthyCells(table->GetTabletCellBundle())) {
                        ChangeTabletActionState(action, ETabletActionState::Orphaned, false);
                        break;
                    }

                    assignment = ComputeTabletAssignment(
                        table,
                        tableSettings.MountConfig,
                        nullptr,
                        action->Tablets());
                } else {
                    for (int index = 0; index < action->Tablets().size(); ++index) {
                        assignment.emplace_back(
                            action->Tablets()[index],
                            action->TabletCells()[index]);
                    }
                }

                DoMountTablets(
                    table,
                    assignment,
                    tableSettings.MountConfig->InMemoryMode,
                    action->GetFreeze(),
                    serializedTableSttings);

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

                if (mountedCount == action->Tablets().size()) {
                    ChangeTabletActionState(action, ETabletActionState::Mounted);
                }
                break;
            }

            case ETabletActionState::Mounted: {
                ChangeTabletActionState(action, ETabletActionState::Completed);
                break;
            }

            case ETabletActionState::Failing: {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), action->Error(), "Tablet action failed (ActionId: %v, TabletBalancerCorrelationId: %v)",
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

            default:
                YT_ABORT();
        }
    }

    void HydraKickOrphanedTabletActions(NProto::TReqKickOrphanedTabletActions* request)
    {

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        THashSet<TCellBundle*> healthyBundles;
        for (auto [bundleId, bundle] : cellManager->CellBundles()) {
            if (!IsObjectAlive(bundle) || bundle->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

            for (auto* cellBase : bundle->Cells()) {
                YT_VERIFY(cellBase->GetType() == EObjectType::TabletCell);
                auto* cell = cellBase->As<TTabletCell>();
                if (IsCellActive(cell)) {
                    healthyBundles.insert(cell->GetCellBundle());
                    continue;
                }
            }
        }

        auto orphanedActionIds = FromProto<std::vector<TTabletActionId>>(request->tablet_action_ids());
        for (auto actionId : orphanedActionIds) {
            auto* action = FindTabletAction(actionId);
            if (IsObjectAlive(action) && action->GetState() == ETabletActionState::Orphaned) {
                auto* bundle = action->Tablets().front()->GetTable()->GetTabletCellBundle();
                if (healthyBundles.contains(bundle)) {
                    ChangeTabletActionState(action, ETabletActionState::Unmounted);
                }
            }
        }
    }


    struct TTableSettings
    {
        TTableMountConfigPtr MountConfig;
        NTabletNode::TTabletStoreReaderConfigPtr StoreReaderConfig;
        NTabletNode::TTabletHunkReaderConfigPtr HunkReaderConfig;
        NTabletNode::TTabletStoreWriterConfigPtr StoreWriterConfig;
        NTabletNode::TTabletStoreWriterOptionsPtr StoreWriterOptions;
        NTabletNode::TTabletHunkWriterConfigPtr HunkWriterConfig;
        NTabletNode::TTabletHunkWriterOptionsPtr HunkWriterOptions;
    };

    TTableSettings GetTableSettings(TTableNode* table)
    {
        TTableSettings result;

        const auto& dynamicConfig = GetDynamicConfig();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        const auto& tableAttributes = tableProxy->Attributes();

        // Parse and prepare mount config.
        try {
            result.MountConfig = ConvertTo<TTableMountConfigPtr>(tableAttributes);
            if (!table->GetProfilingMode()) {
                result.MountConfig->ProfilingMode = dynamicConfig->DynamicTableProfilingMode;
            }
            result.MountConfig->EnableDynamicStoreRead = IsDynamicStoreReadEnabled(table);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing table mount configuration")
                << ex;
        }

        // Parse and prepare store reader config.
        try {
            result.StoreReaderConfig = UpdateYsonSerializable(
                GetDynamicConfig()->StoreChunkReader,
                // TODO(babenko): rename to store_chunk_reader
                tableAttributes.FindYson(EInternedAttributeKey::ChunkReader.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing store reader config")
                << ex;
        }

        // Parse and prepare hunk reader config.
        try {
            result.HunkReaderConfig = UpdateYsonSerializable(
                GetDynamicConfig()->HunkChunkReader,
                tableAttributes.FindYson(EInternedAttributeKey::HunkChunkReader.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing hunk reader config")
                << ex;
        }

        const auto& chunkReplication = table->Replication();
        auto primaryMediumIndex = table->GetPrimaryMediumIndex();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);
        auto replicationFactor = chunkReplication.Get(primaryMediumIndex).GetReplicationFactor();

        // Prepare store writer options.
        result.StoreWriterOptions = New<NTabletNode::TTabletStoreWriterOptions>();
        result.StoreWriterOptions->ReplicationFactor = replicationFactor;
        result.StoreWriterOptions->MediumName = primaryMedium->GetName();
        result.StoreWriterOptions->Account = table->GetAccount()->GetName();
        result.StoreWriterOptions->CompressionCodec = table->GetCompressionCodec();
        result.StoreWriterOptions->ErasureCodec = table->GetErasureCodec();
        result.StoreWriterOptions->ChunksVital = chunkReplication.GetVital();
        result.StoreWriterOptions->OptimizeFor = table->GetOptimizeFor();

        // Prepare hunk writer options.
        result.HunkWriterOptions = New<NTabletNode::TTabletHunkWriterOptions>();
        result.HunkWriterOptions->ReplicationFactor = replicationFactor;
        result.HunkWriterOptions->MediumName = primaryMedium->GetName();
        result.HunkWriterOptions->Account = table->GetAccount()->GetName();
        result.HunkWriterOptions->CompressionCodec = table->GetCompressionCodec();
        result.HunkWriterOptions->ErasureCodec = table->GetErasureCodec();
        result.HunkWriterOptions->ChunksVital = chunkReplication.GetVital();

        // Parse and prepare store writer config.
        try {
            auto config = CloneYsonSerializable(GetDynamicConfig()->StoreChunkWriter);
            config->PreferLocalHost = primaryMedium->Config()->PreferLocalHostForDynamicTables;
            if (GetDynamicConfig()->IncreaseUploadReplicationFactor ||
                table->GetTabletCellBundle()->GetDynamicOptions()->IncreaseUploadReplicationFactor)
            {
                config->UploadReplicationFactor = replicationFactor;
            }

            result.StoreWriterConfig = UpdateYsonSerializable(
                config,
                // TODO(babenko): rename to store_chunk_writer
                tableAttributes.FindYson(EInternedAttributeKey::ChunkWriter.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing store writer config")
                << ex;
        }

        // Parse and prepare hunk writer config.
        try {
            auto config = CloneYsonSerializable(GetDynamicConfig()->HunkChunkWriter);
            config->PreferLocalHost = primaryMedium->Config()->PreferLocalHostForDynamicTables;
            config->UploadReplicationFactor = replicationFactor;

            result.HunkWriterConfig = UpdateYsonSerializable(
                config,
                tableAttributes.FindYson(EInternedAttributeKey::HunkChunkWriter.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing hunk writer config")
                << ex;
        }

        return result;
    }

    struct TSerializedTableSettings
    {
        TYsonString MountConfig;
        TYsonString StoreReaderConfig;
        TYsonString HunkReaderConfig;
        TYsonString StoreWriterConfig;
        TYsonString StoreWriterOptions;
        TYsonString HunkWriterConfig;
        TYsonString HunkWriterOptions;
    };

    static TSerializedTableSettings SerializeTableSettings(const TTableSettings& tableSettings)
    {
        return {
            .MountConfig = ConvertToYsonString(tableSettings.MountConfig),
            .StoreReaderConfig = ConvertToYsonString(tableSettings.StoreReaderConfig),
            .HunkReaderConfig = ConvertToYsonString(tableSettings.HunkReaderConfig),
            .StoreWriterConfig = ConvertToYsonString(tableSettings.StoreWriterConfig),
            .StoreWriterOptions = ConvertToYsonString(tableSettings.StoreWriterOptions),
            .HunkWriterConfig = ConvertToYsonString(tableSettings.HunkWriterConfig),
            .HunkWriterOptions = ConvertToYsonString(tableSettings.HunkWriterOptions)
        };
    }

    template <class TRequest>
    void FillTableSettings(TRequest* request, const TSerializedTableSettings& serializedTableSettings)
    {
        auto* tableSettings = request->mutable_table_settings();
        tableSettings->set_mount_config(serializedTableSettings.MountConfig.ToString());
        tableSettings->set_store_reader_config(serializedTableSettings.StoreReaderConfig.ToString());
        tableSettings->set_hunk_reader_config(serializedTableSettings.HunkReaderConfig.ToString());
        tableSettings->set_store_writer_config(serializedTableSettings.StoreWriterConfig.ToString());
        tableSettings->set_store_writer_options(serializedTableSettings.StoreWriterOptions.ToString());
        tableSettings->set_hunk_writer_config(serializedTableSettings.HunkWriterConfig.ToString());
        tableSettings->set_hunk_writer_options(serializedTableSettings.HunkWriterOptions.ToString());
        // COMPAT
        request->set_mount_config(serializedTableSettings.MountConfig.ToString());
        request->set_store_reader_config(serializedTableSettings.StoreReaderConfig.ToString());
        request->set_store_writer_config(serializedTableSettings.StoreWriterConfig.ToString());
        request->set_store_writer_options(serializedTableSettings.StoreWriterOptions.ToString());
    }


    void DoMountTablet(
        TTablet* tablet,
        TTabletCell* cell,
        bool freeze)
    {
        auto* table = tablet->GetTable();
        auto tableSettings = GetTableSettings(table);
        auto serializedTableSettings = SerializeTableSettings(tableSettings);
        auto assignment = ComputeTabletAssignment(
            table,
            tableSettings.MountConfig,
            cell,
            std::vector<TTablet*>{tablet});

        DoMountTablets(
            table,
            assignment,
            tableSettings.MountConfig->InMemoryMode,
            freeze,
            serializedTableSettings);
    }

    void DoMountTablets(
        TTableNode* table,
        const std::vector<std::pair<TTablet*, TTabletCell*>>& assignment,
        EInMemoryMode inMemoryMode,
        bool freeze,
        const TSerializedTableSettings& serializedTableSettings,
        TTimestamp mountTimestamp = NullTimestamp)
    {
        table->SetMountedWithEnabledDynamicStoreRead(IsDynamicStoreReadEnabled(table));

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto resourceUsageBefore = table->GetTabletResourceUsage();
        const auto& allTablets = table->Tablets();
        for (auto [tablet, cell] : assignment) {
            YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);

            if (!IsCellActive(cell)) {
                DoCreateTabletAction(
                    TObjectId(),
                    ETabletActionKind::Move,
                    ETabletActionState::Orphaned,
                    std::vector<TTablet*>{tablet},
                    std::vector<TTabletCell*>{},
                    std::vector<NTableClient::TLegacyOwningKey>{},
                    /* tabletCount */ std::nullopt,
                    freeze,
                    /* skipFreezing */ false,
                    /* correlationId */ {},
                    /* expirationTime */ TInstant::Zero());
                continue;
            }

            int tabletIndex = tablet->GetIndex();
            const auto& chunkLists = table->GetChunkList()->Children();
            YT_VERIFY(allTablets.size() == chunkLists.size());

            tablet->SetCell(cell);
            YT_VERIFY(cell->Tablets().insert(tablet).second);
            objectManager->RefObject(cell);

            table->DiscountTabletStatistics(GetTabletStatistics(tablet));

            tablet->SetState(freeze ? ETabletState::FrozenMounting : ETabletState::Mounting);
            tablet->SetInMemoryMode(inMemoryMode);

            cell->GossipStatistics().Local() += GetTabletStatistics(tablet);
            table->AccountTabletStatistics(GetTabletStatistics(tablet));

            const auto* context = GetCurrentMutationContext();
            tablet->SetMountRevision(context->GetVersion().ToRevision());
            if (mountTimestamp != NullTimestamp) {
                tablet->NodeStatistics().set_unflushed_timestamp(mountTimestamp);
            }

            int preloadPendingStoreCount = 0;

            auto* mailbox = hiveManager->GetMailbox(cell->GetId());

            {
                TReqMountTablet req;
                req.set_retained_timestamp(tablet->GetRetainedTimestamp());
                req.set_path(table->GetMountPath());
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_mount_revision(tablet->GetMountRevision());
                ToProto(req.mutable_table_id(), table->GetId());
                ToProto(req.mutable_schema(), table->GetTableSchema());
                if (table->IsPhysicallySorted()) {
                    ToProto(req.mutable_pivot_key(), tablet->GetPivotKey());
                    ToProto(req.mutable_next_pivot_key(), tablet->GetIndex() + 1 == allTablets.size()
                        ? MaxKey()
                        : allTablets[tabletIndex + 1]->GetPivotKey());
                } else {
                    req.set_trimmed_row_count(tablet->GetTrimmedRowCount());
                }
                FillTableSettings(&req, serializedTableSettings);
                req.set_atomicity(ToProto<int>(table->GetAtomicity()));
                req.set_commit_ordering(ToProto<int>(table->GetCommitOrdering()));
                req.set_freeze(freeze);
                ToProto(req.mutable_upstream_replica_id(), table->GetUpstreamReplicaId());
                if (table->IsReplicated()) {
                    auto* replicatedTable = table->As<TReplicatedTableNode>();
                    for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                        const auto* replicaInfo = tablet->GetReplicaInfo(replica);
                        PopulateTableReplicaDescriptor(req.add_replicas(), replica, *replicaInfo);
                    }
                }

                auto* chunkList = tablet->GetChunkList();
                const auto& chunkListStatistics = chunkList->Statistics();
                auto chunksOrViews = EnumerateStoresInChunkTree(chunkList);
                i64 startingRowIndex = chunkListStatistics.LogicalRowCount - chunkListStatistics.RowCount;
                for (const auto* chunkOrView : chunksOrViews) {
                    if (IsHunkChunk(chunkOrView)) {
                        FillHunkChunkDescriptor(chunkOrView->AsChunk(), req.add_hunk_chunks());
                    } else {
                        FillStoreDescriptor(table, chunkOrView, req.add_stores(), &startingRowIndex);
                    }
                }

                for (auto [transactionId, lock] : table->DynamicTableLocks()) {
                    auto* protoLock = req.add_locks();
                    ToProto(protoLock->mutable_transaction_id(), transactionId);
                    protoLock->set_timestamp(lock.Timestamp);
                }

                if (!freeze && IsDynamicStoreReadEnabled(table)) {
                    CreateAndAttachDynamicStores(tablet, &req);
                }

                if (inMemoryMode != EInMemoryMode::None) {
                    preloadPendingStoreCount = chunksOrViews.size();
                }

                auto* mountHint = req.mutable_mount_hint();
                ToProto(mountHint->mutable_eden_store_ids(), tablet->EdenStoreIds());

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Mounting tablet (TableId: %v, TabletId: %v, CellId: %v, ChunkCount: %v, "
                    "Atomicity: %v, CommitOrdering: %v, Freeze: %v, UpstreamReplicaId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId(),
                    chunksOrViews.size(),
                    table->GetAtomicity(),
                    table->GetCommitOrdering(),
                    freeze,
                    table->GetUpstreamReplicaId());

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
        }

        UpdateResourceUsage(table, table->GetTabletResourceUsage() - resourceUsageBefore);
    }

    void DoFreezeTablet(TTablet* tablet)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* cell = tablet->GetCell();
        auto state = tablet->GetState();
        YT_VERIFY(state == ETabletState::Mounted ||
            state == ETabletState::FrozenMounting ||
            state == ETabletState::Frozen ||
            state == ETabletState::Freezing);

        if (tablet->GetState() == ETabletState::Mounted) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Freezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId(),
                cell->GetId());

            tablet->SetState(ETabletState::Freezing);

            TReqFreezeTablet request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());

            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, request);
        }
    }

    void DoUnfreezeTablet(TTablet* tablet)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* cell = tablet->GetCell();
        auto state = tablet->GetState();
        YT_VERIFY(state == ETabletState::Mounted ||
            state == ETabletState::Frozen ||
            state == ETabletState::Unfreezing);

        if (tablet->GetState() == ETabletState::Frozen) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Unfreezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId(),
                cell->GetId());

            tablet->SetState(ETabletState::Unfreezing);

            TReqUnfreezeTablet request;

            if (IsDynamicStoreReadEnabled(tablet->GetTable())) {
                CreateAndAttachDynamicStores(tablet, &request);
            }

            ToProto(request.mutable_tablet_id(), tablet->GetId());

            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, request);
        }
    }


    void HydraOnTabletLocked(NProto::TRspLockTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto transactionIds = FromProto<std::vector<TTransactionId>>(response->transaction_ids());
        auto* table = tablet->GetTable();

        for (auto transactionId : transactionIds) {
            if (auto it = tablet->UnconfirmedDynamicTableLocks().find(transactionId)) {
                tablet->UnconfirmedDynamicTableLocks().erase(it);
                table->ConfirmDynamicTableLock(transactionId);

                int pendingTabletCount = 0;
                if (auto it = table->DynamicTableLocks().find(transactionId)) {
                    pendingTabletCount = it->second.PendingTabletCount;
                }

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Confirmed tablet lock (TabletId: %v, TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
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

        for (auto* table : transaction->LockedDynamicTables()) {
            if (!IsObjectAlive(table)) {
                continue;
            }

            for (auto* tablet : table->Tablets()) {
                if (tablet->GetState() == ETabletState::Unmounted) {
                    continue;
                }

                tablet->UnconfirmedDynamicTableLocks().erase(transaction->GetId());

                auto* cell = tablet->GetCell();
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                TReqUnlockTablet req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                ToProto(req.mutable_transaction_id(), transaction->GetId());
                req.set_mount_revision(tablet->GetMountRevision());
                // Aborted bulk insert should not conflict with concurrent tablet transactions.
                req.set_commit_timestamp(static_cast<i64>(MinTimestamp));

                hiveManager->PostMessage(mailbox, req);
            }

            table->RemoveDynamicTableLock(transaction->GetId());
        }

        transaction->LockedDynamicTables().clear();
    }

    int DoReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys)
    {
        if (!pivotKeys.empty() || !table->IsPhysicallySorted()) {
            ReshardTableImpl(
                table,
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                pivotKeys);
            return newTabletCount;
        } else {
            auto newPivotKeys = CalculatePivotKeys(table, firstTabletIndex, lastTabletIndex, newTabletCount);
            newTabletCount = newPivotKeys.size();
            ReshardTableImpl(
                table,
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                newPivotKeys);
            return newTabletCount;
        }
    }

    // If there are several otherwise identical chunk views with adjacent read ranges
    // we merge them into one chunk view with the joint range.
    std::vector<TChunkTree*> MergeChunkViewRanges(
        std::vector<NChunkServer::TChunkView*> chunkViews,
        const TLegacyOwningKey& lowerPivot,
        const TLegacyOwningKey& upperPivot)
    {
        auto mergeResults = MergeAdjacentChunkViewRanges(std::move(chunkViews));
        std::vector<TChunkTree*> result;

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (const auto& mergeResult : mergeResults) {
            auto* firstChunkView = mergeResult.FirstChunkView;
            auto* lastChunkView = mergeResult.LastChunkView;
            const auto& lowerLimit = firstChunkView->ReadRange().LowerLimit().HasLegacyKey()
                ? firstChunkView->ReadRange().LowerLimit().GetLegacyKey()
                : EmptyKey();
            const auto& upperLimit = lastChunkView->ReadRange().UpperLimit().HasLegacyKey()
                ? lastChunkView->ReadRange().UpperLimit().GetLegacyKey()
                : MaxKey();

            if (firstChunkView == lastChunkView &&
                lowerPivot <= lowerLimit &&
                upperLimit <= upperPivot)
            {
                result.push_back(firstChunkView);
                continue;
            } else {
                NChunkClient::TLegacyReadRange readRange;
                const auto& adjustedLower = std::max(lowerLimit, lowerPivot);
                const auto& adjustedUpper = std::min(upperLimit, upperPivot);
                YT_VERIFY(adjustedLower < adjustedUpper);
                if (adjustedLower != EmptyKey()) {
                    readRange.LowerLimit().SetLegacyKey(adjustedLower);
                }
                if (adjustedUpper != MaxKey()) {
                    readRange.UpperLimit().SetLegacyKey(adjustedUpper);
                }
                result.push_back(chunkManager->CloneChunkView(firstChunkView, readRange));
            }
        }

        return result;
    }

    void ReshardTableImpl(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& pivotKeys)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());
        YT_VERIFY(!table->IsExternal());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        auto resourceUsageBefore = table->GetTabletResourceUsage();

        auto& tablets = table->MutableTablets();
        YT_VERIFY(tablets.size() == table->GetChunkList()->Children().size());

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Resharding table (TableId: %v, FirstTabletIndex: %v, LastTabletIndex: %v, "
            "TabletCount %v, PivotKeys: %v)",
            table->GetId(),
            firstTabletIndex,
            lastTabletIndex,
            newTabletCount,
            pivotKeys);

        // Calculate retained timestamp for removed tablets.
        auto retainedTimestamp = MinTimestamp;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            retainedTimestamp = std::max(retainedTimestamp, tablets[index]->GetRetainedTimestamp());
        }

        // Save eden stores of removed tablets.
        // NB. Since new chunk views may be created over existing chunks, we mark underlying
        // chunks themselves as eden. It gives different result only in rare cases when a chunk
        // under a chunk view was in eden in some tablet but not in the adjacent tablet.
        THashSet<TStoreId> oldEdenStoreIds;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            for (auto storeId : tablets[index]->EdenStoreIds()) {
                if (auto chunkView = chunkManager->FindChunkView(storeId)) {
                    oldEdenStoreIds.insert(chunkView->GetUnderlyingChunk()->GetId());
                } else {
                    oldEdenStoreIds.insert(storeId);
                }
            }
        }

        // Create new tablets.
        std::vector<TTablet*> newTablets;
        for (int index = 0; index < newTabletCount; ++index) {
            auto* newTablet = CreateTablet(table);
            auto* oldTablet = index < oldTabletCount ? tablets[index + firstTabletIndex] : nullptr;
            if (table->IsSorted()) {
                newTablet->SetPivotKey(pivotKeys[index]);
            } else if (oldTablet) {
                newTablet->SetTrimmedRowCount(oldTablet->GetTrimmedRowCount());
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

        std::vector<TLegacyOwningKey> oldPivotKeys;

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            if (table->IsPhysicallySorted()) {
                oldPivotKeys.push_back(tablet->GetPivotKey());
            }
            table->DiscountTabletStatistics(GetTabletStatistics(tablet));
            tablet->SetTable(nullptr);
            objectManager->UnrefObject(tablet);
        }

        if (table->IsPhysicallySorted()) {
            if (lastTabletIndex + 1 < tablets.size()) {
                oldPivotKeys.push_back(tablets[lastTabletIndex + 1]->GetPivotKey());
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

        // Copy chunk tree if somebody holds a reference.
        CopyChunkListIfShared(table, firstTabletIndex, lastTabletIndex);

        auto* oldRootChunkList = table->GetChunkList();
        const auto& oldTabletChunkTrees = oldRootChunkList->Children();

        auto* newRootChunkList = chunkManager->CreateChunkList(oldRootChunkList->GetKind());

        std::vector<TChunkTree*> newTabletChunkTrees;
        newTabletChunkTrees.reserve(newTabletCount);

        // Initialize new tablet chunk lists.
        if (table->IsPhysicallySorted()) {
            // This excludes hunk chunks.
            std::vector<TChunkTree*> chunksOrViews;

            const auto& tabletChunkTrees = table->GetChunkList()->Children();

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                std::vector<TChunkTree*> tabletStores;
                EnumerateStoresInChunkTree(tabletChunkTrees[index]->AsChunkList(), &tabletStores);

                const auto& lowerPivot = oldPivotKeys[index - firstTabletIndex];
                const auto& upperPivot = oldPivotKeys[index - firstTabletIndex + 1];

                for (auto* chunkTree : tabletStores) {
                    if (IsHunkChunk(chunkTree)) {
                        continue;
                    }

                    if (chunkTree->GetType() == EObjectType::ChunkView) {
                        auto* chunkView = chunkTree->AsChunkView();
                        auto readRange = chunkView->GetCompleteReadRange();

                        // Check if chunk view fits into the old tablet completely.
                        // This might not be the case if the chunk view comes from bulk insert and has no read range.
                        if (readRange.LowerLimit().GetLegacyKey() < lowerPivot ||
                            upperPivot < readRange.UpperLimit().GetLegacyKey())
                        {
                            if (!chunkView->GetTransactionId()) {
                                YT_LOG_ALERT("Chunk view without transaction id is not fully inside its tablet "
                                    "(ChunkViewId: %v, UnderlyingChunkId: %v, "
                                    "EffectiveLowerLimit: %v, EffectiveUpperLimit: %v, "
                                    "PivotKey: %v, NextPivotKey: %v)",
                                    chunkView->GetId(),
                                    chunkView->GetUnderlyingChunk()->GetId(),
                                    readRange.LowerLimit().GetLegacyKey(),
                                    readRange.UpperLimit().GetLegacyKey(),
                                    lowerPivot,
                                    upperPivot);
                            }

                            NChunkClient::TLegacyReadRange newReadRange;
                            if (readRange.LowerLimit().GetLegacyKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetLegacyKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetLegacyKey()) {
                                newReadRange.UpperLimit().SetLegacyKey(upperPivot);
                            }
                            auto* newChunkView = chunkManager->CreateChunkView(chunkView, newReadRange);

                            chunkTree = newChunkView;
                        }
                    }

                    chunksOrViews.push_back(chunkTree);
                }
            }

            std::sort(chunksOrViews.begin(), chunksOrViews.end(), TObjectRefComparer::Compare);
            chunksOrViews.erase(
                std::unique(chunksOrViews.begin(), chunksOrViews.end()),
                chunksOrViews.end());
            int keyColumnCount = table->GetTableSchema().GetKeyColumnCount();

            // Create new tablet chunk lists.
            for (int index = 0; index < newTabletCount; ++index) {
                auto* tabletChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicTablet);
                tabletChunkList->SetPivotKey(pivotKeys[index]);
                newTabletChunkTrees.push_back(tabletChunkList);
            }

            // Move chunks or views from the resharded tablets to appropriate chunk lists.
            std::vector<std::vector<NChunkServer::TChunkView*>> newTabletChildrenToBeMerged(newTablets.size());
            std::vector<std::vector<TStoreId>> newEdenStoreIds(newTablets.size());

            for (auto* chunkOrView : chunksOrViews) {
                NChunkClient::TLegacyReadRange readRange;
                if (chunkOrView->GetType() == EObjectType::ChunkView) {
                    readRange = chunkOrView->AsChunkView()->GetCompleteReadRange();
                } else {
                    auto keyPair = GetChunkBoundaryKeys(chunkOrView->AsChunk()->ChunkMeta(), keyColumnCount);
                    readRange = {
                        NChunkClient::TLegacyReadLimit(keyPair.first),
                        NChunkClient::TLegacyReadLimit(GetKeySuccessor(keyPair.second))
                    };
                }

                auto tabletsRange = GetIntersectingTablets(newTablets, readRange);
                for (auto it = tabletsRange.first; it != tabletsRange.second; ++it) {
                    auto* tablet = *it;
                    const auto& lowerPivot = tablet->GetPivotKey();
                    const auto& upperPivot = tablet->GetIndex() == tablets.size() - 1
                        ? MaxKey()
                        : tablets[tablet->GetIndex() + 1]->GetPivotKey();
                    int relativeIndex = it - newTablets.begin();

                    // Chunks or chunk views created directly from chunks may be attached to tablets as is.
                    // On the other hand, old chunk views may link to the same chunk and have adjacent ranges,
                    // so we handle them separately.
                    if (chunkOrView->GetType() == EObjectType::ChunkView) {
                        // Read range given by tablet's pivot keys will be enforced later.
                        newTabletChildrenToBeMerged[relativeIndex].push_back(chunkOrView->AsChunkView());
                    } else {
                        if (lowerPivot <= readRange.LowerLimit().GetLegacyKey() &&
                            readRange.UpperLimit().GetLegacyKey() <= upperPivot)
                        {
                            // Chunk fits into the tablet.
                            chunkManager->AttachToChunkList(
                                newTabletChunkTrees[relativeIndex]->AsChunkList(),
                                chunkOrView);
                            if (oldEdenStoreIds.contains(chunkOrView->GetId())) {
                                newEdenStoreIds[relativeIndex].push_back(chunkOrView->GetId());
                            }
                        } else {
                            // Chunk does not fit into the tablet, create chunk view.
                            NChunkClient::TLegacyReadRange newReadRange;
                            if (readRange.LowerLimit().GetLegacyKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetLegacyKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetLegacyKey()) {
                                newReadRange.UpperLimit().SetLegacyKey(upperPivot);
                            }
                            auto* newChunkView = chunkManager->CreateChunkView(chunkOrView, newReadRange);
                            chunkManager->AttachToChunkList(
                                newTabletChunkTrees[relativeIndex]->AsChunkList(),
                                newChunkView);
                            if (oldEdenStoreIds.contains(chunkOrView->GetId())) {
                                newEdenStoreIds[relativeIndex].push_back(newChunkView->GetId());
                            }
                        }
                    }
                }
            }

            for (int relativeIndex = 0; relativeIndex < newTablets.size(); ++relativeIndex) {
                auto* tablet = newTablets[relativeIndex];
                const auto& lowerPivot = tablet->GetPivotKey();
                const auto& upperPivot = tablet->GetIndex() == tablets.size() - 1
                    ? MaxKey()
                    : tablets[tablet->GetIndex() + 1]->GetPivotKey();

                std::vector<TChunkTree*> mergedChunksOrViews;
                try {
                    mergedChunksOrViews = MergeChunkViewRanges(newTabletChildrenToBeMerged[relativeIndex], lowerPivot, upperPivot);
                } catch (const std::exception& ex) {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), ex, "Failed to merge chunk view ranges");
                    mergedChunksOrViews = {};
                }

                auto* newTabletChunkList = newTabletChunkTrees[relativeIndex]->AsChunkList();
                chunkManager->AttachToChunkList(newTabletChunkList, mergedChunksOrViews);

                std::vector<TChunkTree*> hunkChunks;
                for (auto* chunkOrView : mergedChunksOrViews) {
                    if (oldEdenStoreIds.contains(chunkOrView->AsChunkView()->GetUnderlyingChunk()->GetId())) {
                        newEdenStoreIds[relativeIndex].push_back(chunkOrView->GetId());
                    }

                    if (chunkOrView->GetType() == EObjectType::Chunk) {
                        const auto* chunk = chunkOrView->AsChunk();
                        if (auto hunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunk->ChunkMeta().extensions())) {
                            for (const auto& protoRef : hunkRefsExt->refs()) {
                                auto hunkChunkId = FromProto<TChunkId>(protoRef.chunk_id());
                                auto* hunkChunk = chunkManager->FindChunk(hunkChunkId);
                                if (!IsObjectAlive(hunkChunk)) {
                                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Store references a non-existing hunk chunk (StoreId: %v, HunkChunkId: %v)",
                                        chunk->GetId(),
                                        hunkChunkId);
                                    continue;
                                }
                                hunkChunks.push_back(hunkChunk);
                            }
                        }
                    }
                }

                if (!hunkChunks.empty()) {
                    SortUniqueBy(hunkChunks, [] (const auto* chunkTree) { return chunkTree->GetId(); });
                    auto* hunkChunkList = chunkManager->GetOrCreateHunkChunkList(newTabletChunkList);
                    chunkManager->AttachToChunkList(hunkChunkList, hunkChunks);
                }
            }

            for (int relativeIndex = 0; relativeIndex < newTablets.size(); ++relativeIndex) {
                SetTabletEdenStoreIds(newTablets[relativeIndex], newEdenStoreIds[relativeIndex]);
            }
        } else {
            // If the number of tablets increases, just leave the new trailing ones empty.
            // If the number of tablets decreases, merge the original trailing ones.
            auto attachChunksToChunkList = [&] (TChunkList* chunkList, int firstTabletIndex, int lastTabletIndex) {
                std::vector<TChunk*> chunks;
                for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                    EnumerateChunksInChunkTree(oldTabletChunkTrees[index]->AsChunkList(), &chunks);
                }
                for (auto* chunk : chunks) {
                    chunkManager->AttachToChunkList(chunkList, chunk);
                }
            };
            for (int index = firstTabletIndex; index < firstTabletIndex + std::min(oldTabletCount, newTabletCount); ++index) {
                newTabletChunkTrees.push_back(chunkManager->CloneTabletChunkList(oldTabletChunkTrees[index]->AsChunkList()));
            }
            if (oldTabletCount > newTabletCount) {
                auto* chunkList = newTabletChunkTrees[newTabletCount - 1]->AsChunkList();
                attachChunksToChunkList(chunkList, firstTabletIndex + newTabletCount, lastTabletIndex);
            } else {
                for (int index = oldTabletCount; index < newTabletCount; ++index) {
                    newTabletChunkTrees.push_back(chunkManager->CreateChunkList(EChunkListKind::OrderedDynamicTablet));
                }
            }
            YT_ASSERT(newTabletChunkTrees.size() == newTabletCount);
        }

        // Update tablet chunk lists.
        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkTrees.data(),
            oldTabletChunkTrees.data() + firstTabletIndex);
        chunkManager->AttachToChunkList(
            newRootChunkList,
            newTabletChunkTrees);
        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkTrees.data() + lastTabletIndex + 1,
            oldTabletChunkTrees.data() + oldTabletChunkTrees.size());

        // Replace root chunk list.
        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);
        objectManager->RefObject(newRootChunkList);
        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        // Account new tablet statistics.
        for (auto* newTablet : newTablets) {
            table->AccountTabletStatistics(GetTabletStatistics(newTablet));
        }

        // TODO(savrus) Looks like this is unnecessary. Need to check.
        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();

        auto resourceUsageDelta = table->GetTabletResourceUsage() - resourceUsageBefore;
        UpdateResourceUsage(table, resourceUsageDelta);

        table->RecomputeTabletMasterMemoryUsage();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
    }


    void SetSyncTabletActionsKeepalive(const std::vector<TTabletActionId>& actionIds)
    {
        const auto* context = GetCurrentMutationContext();
        const auto expirationTime = context->GetTimestamp() + DefaultSyncTabletActionKeepalivePeriod;
        for (auto actionId : actionIds) {
            auto* action = GetTabletAction(actionId);
            action->SetExpirationTime(expirationTime);
        }
    }


    const TDynamicTabletManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig = nullptr)
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

        // COMPAT(ifsmirnov)
        if (oldConfig &&
            !oldConfig->TabletManager->AccumulatePreloadPendingStoreCountCorrectly &&
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
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
        TableReplicaMap_.LoadKeys(context);
        TabletActionMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadValues(context);
        TableReplicaMap_.LoadValues(context);
        TabletActionMap_.LoadValues(context);
        if (context.GetVersion() < EMasterReign::MoveTableStatisticsGossipToTableManager) {
            const auto& tableManager = context.GetBootstrap()->GetTableManager();
            tableManager->LoadStatisticsUpdateRequests(context);
        }

        // COMPAT(ifsmirnov)
        RecomputeAggregateTabletStatistics_ = (context.GetVersion() < EMasterReign::VersionedRemoteCopy);

        // COMPAT(ifsmirnov)
        RecomputeBundleResourceUsage_ = (context.GetVersion() < EMasterReign::BundleQuotas);
    }

    void RecomputeBundleResourceUsage()
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto [bundleId, bundleBase] : cellManager->CellBundles()) {
            if (!IsObjectAlive(bundleBase) || bundleBase->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

            auto* bundle = bundleBase->As<TTabletCellBundle>();
            bundle->ResourceUsage() = {};
            bundle->ResourceUsage().Initialize(Bootstrap_);
        }

        THashSet<TTableNode*> accountedTables;
        for (const auto& [id, tablet] : Tablets()) {
            auto* table = tablet->GetTable();
            if (!table) {
                continue;
            }
            if (!accountedTables.insert(table).second) {
                continue;
            }
            if (auto* bundle = table->GetTabletCellBundle()) {
                bundle->UpdateResourceUsage(table->GetTabletResourceUsage());
            }
        }
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        if (RecomputeAggregateTabletStatistics_) {
            THashSet<TTableNode*> resetTables;
            for (const auto& [id, tablet] : Tablets()) {
                if (auto* table = tablet->GetTable()) {
                    if (resetTables.insert(table).second) {
                        table->ResetTabletStatistics();
                    }
                    table->AccountTabletStatistics(GetTabletStatistics(tablet));
                }
            }
        }

        InitBuiltins();
    }

    void OnAfterCellManagerSnapshotLoaded()
    {
        InitBuiltins();

        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        for (auto [cellId, cellBase] : cellManager->Cells()) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            cell->GossipStatistics().Initialize(Bootstrap_);
        }

        for (auto [bundleId, bundleBase] : cellManager->CellBundles()) {
            if (bundleBase->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

            auto* bundle = bundleBase->As<TTabletCellBundle>();
            bundle->ResourceUsage().Initialize(Bootstrap_);
        }

        // COMPAT(ifsmirnov)
        if (RecomputeBundleResourceUsage_) {
            RecomputeBundleResourceUsage();
        }

        for (auto [actionId, action] : TabletActionMap_) {
            // NB: Process non-alive objects to pair with DestroyTabletAction.
            auto bundle = action->GetTabletCellBundle();
            bundle->TabletActions().insert(action);
            if (!action->IsFinished()) {
                bundle->IncreaseActiveTabletActionCount();
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TabletMap_.Clear();
        TableReplicaMap_.Clear();
        TabletActionMap_.Clear();

        DefaultTabletCellBundle_ = nullptr;
    }

    virtual void SetZeroState() override
    {
        InitBuiltins();
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
            DefaultTabletCellBundle_->ResourceLimits().TabletCount = 100000;
            DefaultTabletCellBundle_->ResourceLimits().TabletStaticMemory = 1_TB;
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

        auto holder = std::make_unique<TTabletCellBundle>(id);
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
        request.set_cell_tag(multicellManager->GetCellTag());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto [cellId, cellBase] : cellManager->Cells()) {
            if (!IsObjectAlive(cellBase) || cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

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
        auto cellTag = request->cell_tag();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), "Received tablet cell statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Received tablet cell statistics gossip message (CellTag: %v)",
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

            auto* tablet = FindTablet(tabletId);
            if (!IsObjectAlive(tablet) ||
                tablet->GetState() == ETabletState::Unmounted ||
                mountRevision != tablet->GetMountRevision())
            {
                continue;
            }

            auto* cell = tablet->GetCell();
            if (!IsObjectAlive(cell)){
                continue;
            }

            auto* slot = node->FindCellSlot(cell);
            if (!slot || (slot->PeerState != EPeerState::Leading && slot->PeerState != EPeerState::LeaderRecovery)) {
                continue;
            }

            auto tabletStatistics = GetTabletStatistics(tablet);
            tablet->GetTable()->DiscountTabletStatistics(tabletStatistics);
            cell->GossipStatistics().Local() -= tabletStatistics;

            tablet->NodeStatistics() = tabletInfo.statistics();

            tabletStatistics = GetTabletStatistics(tablet);
            tablet->GetTable()->AccountTabletStatistics(tabletStatistics);
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

            auto updatePerformanceCounter = [&] (TTabletPerformanceCounter* counter, i64 curValue) {
                i64 prevValue = counter->Count;
                auto timeDelta = std::max(1.0, (now - tablet->PerformanceCounters().Timestamp).SecondsFloat());
                auto valueDelta = std::max(curValue, prevValue) - prevValue;
                auto rate = valueDelta / timeDelta;
                counter->Count = curValue;
                counter->Rate = rate;
                auto exp10 = std::exp(-timeDelta / (60 * 10 / 2));
                counter->Rate10 = rate * (1 - exp10) + counter->Rate10 * exp10;
                auto exp60 = std::exp(-timeDelta / (60 * 60 / 2));
                counter->Rate60 = rate * (1 - exp60) + counter->Rate60 * exp60;
            };

            #define XX(name, Name) updatePerformanceCounter( \
                &tablet->PerformanceCounters().Name, \
                tabletInfo.performance_counters().name ## _count());
            ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
            #undef XX
            tablet->PerformanceCounters().Timestamp = now;

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
            ? std::make_optional(FromProto<ETabletState>(request->actual_tablet_state()))
            : std::nullopt;
        auto expectedState = request->has_expected_tablet_state()
            ? std::make_optional(FromProto<ETabletState>(request->expected_tablet_state()))
            : std::nullopt;

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
        if (!IsObjectAlive(node)) {
            return;
        }

        YT_VERIFY(IsTableType(node->GetType()));
        auto* table = node->As<TTableNode>();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Received update upstream tablet state request "
            "(TableId: %v, ActualTabletState: %v, ExpectedTabletState: %v, ExpectedLastMountTransactionId: %v, ActualLastMountTransactionId: %v)",
            tableId,
            actualState,
            expectedState,
            transactionId,
            table->GetLastMountTransactionId());

        if (actualState) {
            table->SetActualTabletState(*actualState);
        }

        if (transactionId == table->GetLastMountTransactionId()) {
            if (expectedState) {
                table->SetExpectedTabletState(*expectedState);
            }
            table->SetLastMountTransactionId(TTransactionId());
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

        YT_VERIFY(IsTableType(node->GetType()));
        auto* table = node->As<TTableNode>();
        auto transactionId = FromProto<TTransactionId>(request->last_mount_transaction_id());
        table->SetPrimaryLastMountTransactionId(transactionId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table tablet state check request received (TableId: %v, LastMountTransactionId %v, PrimaryLastMountTransactionId %v)",
            table->GetId(),
            table->GetLastMountTransactionId(),
            table->GetPrimaryLastMountTransactionId());

        UpdateTabletState(table);
    }

    void UpdateTabletState(TTableNode* table)
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

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table tablet state check requested (TableId: %v, LastMountTransactionId %v)",
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
            YT_VERIFY(tabletCount == table->Tablets().size());
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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table tablet state updated "
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

    void HydraOnTabletMounted(NProto::TRspMountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounting && state != ETabletState::FrozenMounting) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Mounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        bool frozen = response->frozen();
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        // FIXME(savrus) Remove this.
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet mounted (TableId: %v, TabletId: %v, MountRevision: %llx, CellId: %v, Frozen: %v)",
            table->GetId(),
            tablet->GetId(),
            tablet->GetMountRevision(),
            cell->GetId(),
            frozen);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet static memory usage (TableId: %v, TabletMemorySize: %v, TabletStaticUsage: %v, ExternalTabletResourceUsage: %v)",
            table->GetId(),
            tablet->GetTabletStaticMemorySize(),
            table->GetTabletResourceUsage().TabletStaticMemory,
            table->GetExternalTabletResourceUsage().TabletStaticMemory);

        tablet->SetState(frozen ? ETabletState::Frozen : ETabletState::Mounted);

        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnmounted(NProto::TRspUnmountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Unmounting) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Unmounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        SetTabletEdenStoreIds(tablet, FromProto<std::vector<TStoreId>>(response->mount_hint().eden_store_ids()));

        DiscardDynamicStores(tablet);
        DoTabletUnmounted(tablet, /*force*/ false);
        OnTabletActionStateChanged(tablet->GetAction());
    }

    void HydraOnTabletFrozen(NProto::TRspFreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Freezing) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Frozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        SetTabletEdenStoreIds(tablet, FromProto<std::vector<TStoreId>>(response->mount_hint().eden_store_ids()));

        DiscardDynamicStores(tablet);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet frozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Frozen);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnfrozen(NProto::TRspUnfreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Unfreezing) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Unfrozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet unfrozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Mounted);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraUpdateTableReplicaStatistics(NProto::TReqUpdateTableReplicaStatistics* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->GetReplicaInfo(replica);
        PopulateTableReplicaInfoFromStatistics(replicaInfo, request->statistics());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica statistics updated (TabletId: %v, ReplicaId: %v, "
            "CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tabletId,
            replicaId,
            replicaInfo->GetCurrentReplicationRowIndex(),
            replicaInfo->GetCurrentReplicationTimestamp());
    }

    void HydraOnTableReplicaEnabled(NProto::TRspEnableTableReplica* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(response->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = response->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->GetReplicaInfo(replica);
        if (replicaInfo->GetState() != ETableReplicaState::Enabling) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Enabled replica notification received for a replica in a wrong state, "
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
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(response->replica_id());
        auto* replica = FindTableReplica(replicaId);
        if (!IsObjectAlive(replica)) {
            return;
        }

        auto mountRevision = response->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto* replicaInfo = tablet->GetReplicaInfo(replica);
        if (replicaInfo->GetState() != ETableReplicaState::Disabling) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Disabled replica notification received for a replica in a wrong state, "
                "ignored (TabletId: %v, ReplicaId: %v, State: %v)",
                tabletId,
                replicaId,
                replicaInfo->GetState());
            return;
        }

        StopReplicaTransition(tablet, replica, replicaInfo, ETableReplicaState::Disabled);
        CheckTransitioningReplicaTablets(replica);
    }

    void StartReplicaTransition(TTablet* tablet, TTableReplica* replica, TTableReplicaInfo* replicaInfo, ETableReplicaState newState)
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica is now transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica is no longer transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica enabled (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabled);
                break;

            case ETableReplicaState::Disabling:
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica disabled (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Disabled);
                break;

            default:
                YT_ABORT();
        }
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
        CopyChunkListIfShared(tablet->GetTable(), tablet->GetIndex(), tablet->GetIndex(), /*force*/ false);

        DetachChunksFromTablet(tablet, dynamicStores);

        auto* table = tablet->GetTable();
        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            table,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false);

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = -static_cast<int>(dynamicStores.size());
        tablet->GetCell()->GossipStatistics().Local() += statisticsDelta;
        table->AccountTabletStatisticsDelta(statisticsDelta);
    }

    void AbandonDynamicStores(TTablet* tablet)
    {
        auto stores = EnumerateStoresInChunkTree(tablet->GetChunkList());
        std::vector<TChunkTree*> dynamicStores;
        for (auto* store : stores) {
            if (IsDynamicTabletStoreType(store->GetType())) {
                dynamicStores.push_back(store);
                store->AsDynamicStore()->Abandon();
            }
        }
    }

    void DoTabletUnmounted(TTablet* tablet, bool force)
    {
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet unmounted (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        auto tabletStatistics = GetTabletStatistics(tablet);
        cell->GossipStatistics().Local() -= tabletStatistics;
        table->DiscountTabletStatistics(tabletStatistics);
        CheckIfFullyUnmounted(cell);

        auto resourceUsageBefore = table->GetTabletResourceUsage();

        tablet->NodeStatistics().Clear();
        tablet->PerformanceCounters() = TTabletPerformanceCounters();
        tablet->SetInMemoryMode(EInMemoryMode::None);
        tablet->SetState(ETabletState::Unmounted);
        tablet->SetCell(nullptr);
        tablet->SetStoresUpdatePreparedTransaction(nullptr);
        tablet->SetMountRevision(NullRevision);

        UpdateResourceUsage(table, table->GetTabletResourceUsage() - resourceUsageBefore);
        UpdateTabletState(table);

        if (!table->IsPhysicallySorted()) {
            const auto& chunkListStatistics = tablet->GetChunkList()->Statistics();
            if (tablet->GetTrimmedRowCount() > chunkListStatistics.LogicalRowCount) {
                auto message = Format(
                    "Trimmed row count exceeds total row count of the tablet "
                    "and will be rolled back (TableId: %v, TabletId: %v, CellId: %v, "
                    "TrimmedRowCount: %v, LogicalRowCount: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId(),
                    tablet->GetTrimmedRowCount(),
                    chunkListStatistics.LogicalRowCount);
                if (force) {
                    YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), message);
                    tablet->SetTrimmedRowCount(chunkListStatistics.LogicalRowCount);
                } else {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), message);
                }
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        YT_VERIFY(cell->Tablets().erase(tablet) == 1);
        objectManager->UnrefObject(cell);

        for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
            auto* replica = it->first;
            auto& replicaInfo = it->second;
            if (replica->TransitioningTablets().erase(tablet) == 1) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Table replica is still transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v)",
                    tablet->GetTable()->GetId(),
                    tablet->GetId(),
                    replica->GetId(),
                    replicaInfo.GetState());
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Table replica state updated (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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

        table->AccountTabletStatistics(GetTabletStatistics(tablet));
    }

    TDynamicStoreId GenerateDynamicStoreId(const TTablet* tablet, TDynamicStoreId hintId = NullObjectId)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto type = tablet->GetTable()->IsPhysicallySorted()
            ? EObjectType::SortedDynamicTabletStore
            : EObjectType::OrderedDynamicTabletStore;
        return objectManager->GenerateId(type, hintId);
    }

    TDynamicStore* CreateDynamicStore(const TTablet* tablet, TDynamicStoreId hintId = NullObjectId)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto id = GenerateDynamicStoreId(tablet, hintId);
        return chunkManager->CreateDynamicStore(id, tablet);
    }

    void AttachDynamicStoreToTablet(TTablet* tablet, TDynamicStore* dynamicStore)
    {
        auto* table = tablet->GetTable();

        CopyChunkListIfShared(table, tablet->GetIndex(), tablet->GetIndex());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->AttachToChunkList(tablet->GetChunkList(), dynamicStore);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store attached to tablet (TabletId: %v, DynamicStoreId: %v)",
            tablet->GetId(),
            dynamicStore->GetId());

        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            table,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false);

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = 1;
        tablet->GetCell()->GossipStatistics().Local() += statisticsDelta;
        tablet->GetTable()->AccountTabletStatisticsDelta(statisticsDelta);
    }

    template <class TRequest>
    void CreateAndAttachDynamicStores(TTablet* tablet, TRequest* request)
    {
        for (int index = 0; index < DynamicStoreIdPoolSize; ++index) {
            auto* dynamicStore = CreateDynamicStore(tablet);
            AttachDynamicStoreToTablet(tablet, dynamicStore);
            ToProto(request->add_dynamic_store_ids(), dynamicStore->GetId());
        }
    }

    void CopyChunkListIfShared(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        bool force = false)
    {
        TWallTimer timer;
        auto counters = GetCounters({}, table);
        auto reportTime = Finally([&] {
            counters->CopyChunkListTime.Add(timer.GetElapsedTime());
        });

        int actionCount = 0;

        auto* oldRootChunkList = table->GetChunkList();
        auto& chunkLists = oldRootChunkList->Children();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto checkStatisticsMatch = [] (const TChunkTreeStatistics& lhs, TChunkTreeStatistics rhs) {
            rhs.ChunkListCount = lhs.ChunkListCount;
            rhs.Rank = lhs.Rank;
            return lhs == rhs;
        };

        if (objectManager->GetObjectRefCounter(oldRootChunkList) > 1) {
            auto statistics = oldRootChunkList->Statistics();
            auto* newRootChunkList = chunkManager->CreateChunkList(oldRootChunkList->GetKind());
            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data(),
                chunkLists.data() + firstTabletIndex);

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* newTabletChunkList = chunkManager->CloneTabletChunkList(chunkLists[index]->AsChunkList());
                chunkManager->AttachToChunkList(newRootChunkList, newTabletChunkList);

                actionCount += newTabletChunkList->Statistics().ChunkCount;
            }

            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data() + lastTabletIndex + 1,
                chunkLists.data() + chunkLists.size());

            actionCount += newRootChunkList->Children().size();

            // Replace root chunk list.
            table->SetChunkList(newRootChunkList);
            newRootChunkList->AddOwningNode(table);
            objectManager->RefObject(newRootChunkList);
            oldRootChunkList->RemoveOwningNode(table);
            objectManager->UnrefObject(oldRootChunkList);
            if (!checkStatisticsMatch(newRootChunkList->Statistics(), statistics)) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                    "Invalid new root chunk list statistics "
                    "(TableId: %v, NewRootChunkListStatistics: %v, Statistics: %v)",
                    table->GetId(),
                    newRootChunkList->Statistics(),
                    statistics);
            }
        } else {
            auto statistics = oldRootChunkList->Statistics();

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* oldTabletChunkList = chunkLists[index]->AsChunkList();
                if (force || objectManager->GetObjectRefCounter(oldTabletChunkList) > 1) {
                    auto* newTabletChunkList = chunkManager->CloneTabletChunkList(oldTabletChunkList);
                    chunkManager->ReplaceChunkListChild(oldRootChunkList, index, newTabletChunkList);

                    actionCount += newTabletChunkList->Statistics().ChunkCount;

                    // ReplaceChunkListChild assumes that statistics are updated by caller.
                    // Here everything remains the same except for missing subtablet chunk lists.
                    int subtabletChunkListCount = oldTabletChunkList->Statistics().ChunkListCount - 1;
                    if (subtabletChunkListCount > 0) {
                        TChunkTreeStatistics delta{};
                        delta.ChunkListCount = -subtabletChunkListCount;
                        NChunkServer::AccumulateUniqueAncestorsStatistics(newTabletChunkList, delta);
                    }
                }
            }

            if (!checkStatisticsMatch(oldRootChunkList->Statistics(), statistics)) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                    "Invalid old root chunk list statistics "
                    "(TableId: %v, OldRootChunkListStatistics: %v, Statistics: %v)",
                    table->GetId(),
                    oldRootChunkList->Statistics(),
                    statistics);
            }
        }

        if (actionCount > 0) {
            counters->CopyChunkListIfSharedActionCount.Increment(actionCount);
        }
    }

    static int GetFirstDynamicStoreIndex(const TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

        const auto& children = chunkList->Children();
        int firstDynamicStoreIndex = static_cast<int>(children.size()) - 1;
        YT_VERIFY(IsDynamicTabletStoreType(children[firstDynamicStoreIndex]->GetType()));
        while (firstDynamicStoreIndex > chunkList->GetTrimmedChildCount() &&
            IsDynamicTabletStoreType(children[firstDynamicStoreIndex - 1]->GetType()))
        {
            --firstDynamicStoreIndex;
        }

        return firstDynamicStoreIndex;
    }

    void HydraPrepareUpdateTabletStores(TTransaction* transaction, NProto::TReqUpdateTabletStores* request, bool persistent)
    {
        YT_VERIFY(persistent);

        const auto& dynamicConfig = GetDynamicConfig();
        if ((request->hunk_chunks_to_add_size() > 0 || request->hunk_chunks_to_remove_size() > 0) && !dynamicConfig->EnableHunks) {
            THROW_ERROR_EXCEPTION("Hunks are not enabled");
        }

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        if (tablet->GetStoresUpdatePreparedTransaction()) {
            THROW_ERROR_EXCEPTION("Stores update for tablet %v is already prepared by transaction %v",
                tabletId,
                tablet->GetStoresUpdatePreparedTransaction()->GetId());
        }

        auto validateStoreType = [&] (TObjectId id, TStringBuf action) {
            auto type = TypeFromId(id);
            if (!IsChunkTabletStoreType(type) &&
                !IsDynamicTabletStoreType(type) &&
                type != EObjectType::ChunkView)
            {
                THROW_ERROR_EXCEPTION("Cannot %v store %v of type %Qlv",
                    action,
                    id,
                    type);
            }
        };

        auto validateHunkChunkType = [&] (TChunkId id, TStringBuf action) {
            auto type = TypeFromId(id);
            if (!IsBlobChunkType(type)) {
                THROW_ERROR_EXCEPTION("Cannot %v hunk chunk %v of type %Qlv",
                    action,
                    id,
                    type);
            }
        };

        for (const auto& descriptor : request->stores_to_add()) {
            validateStoreType(FromProto<TObjectId>(descriptor.store_id()), "attach");
        }

        for (const auto& descriptor : request->stores_to_remove()) {
            validateStoreType(FromProto<TObjectId>(descriptor.store_id()), "detach");
        }

        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            validateHunkChunkType(FromProto<TChunkId>(descriptor.chunk_id()), "attach");
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            validateHunkChunkType(FromProto<TChunkId>(descriptor.chunk_id()), "detach");
        }

        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        auto state = tablet->GetState();
        if (state != ETabletState::Mounted &&
            state != ETabletState::Unmounting &&
            state != ETabletState::Freezing)
        {
            THROW_ERROR_EXCEPTION("Cannot update stores while tablet %v is in %Qlv state",
                tabletId,
                state);
        }

        const auto* table = tablet->GetTable();
        if (!table->IsPhysicallySorted()) {
            auto* tabletChunkList = tablet->GetChunkList();

            if (request->stores_to_add_size() > 0) {
                if (request->stores_to_add_size() > 1) {
                    THROW_ERROR_EXCEPTION("Cannot attach more than one store to an ordered tablet %v at once",
                        tabletId);
                }

                const auto& descriptor = request->stores_to_add(0);
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                YT_VERIFY(descriptor.has_starting_row_index());
                if (tabletChunkList->Statistics().LogicalRowCount != descriptor.starting_row_index()) {
                    THROW_ERROR_EXCEPTION("Invalid starting row index of store %v in tablet %v: expected %v, got %v",
                        storeId,
                        tabletId,
                        tabletChunkList->Statistics().LogicalRowCount,
                        descriptor.starting_row_index());
                }
            }

            auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

            if (updateReason == ETabletStoresUpdateReason::Trim) {
                int childIndex = tabletChunkList->GetTrimmedChildCount();
                const auto& children = tabletChunkList->Children();
                for (const auto& descriptor : request->stores_to_remove()) {
                    auto storeId = FromProto<TStoreId>(descriptor.store_id());
                    if (TypeFromId(storeId) == EObjectType::OrderedDynamicTabletStore) {
                        continue;
                    }

                    if (childIndex >= children.size()) {
                        THROW_ERROR_EXCEPTION("Attempted to trim store %v which is not part of tablet %v",
                            storeId,
                            tabletId);
                    }
                    if (children[childIndex]->GetId() != storeId) {
                        THROW_ERROR_EXCEPTION("Invalid store to trim in tablet %v: expected %v, got %v",
                            tabletId,
                            children[childIndex]->GetId(),
                            storeId);
                    }
                    ++childIndex;
                }
            }

            if (updateReason == ETabletStoresUpdateReason::Flush &&
                IsDynamicStoreReadEnabled(table) &&
                !request->stores_to_remove().empty())
            {
                auto storeId = FromProto<TStoreId>(request->stores_to_remove(0).store_id());
                int firstDynamicStoreIndex = GetFirstDynamicStoreIndex(tabletChunkList);
                const auto* firstDynamicStore = tabletChunkList->Children()[firstDynamicStoreIndex];
                if (firstDynamicStore->GetId() != storeId) {
                    THROW_ERROR_EXCEPTION("Attempted to flush ordered dynamic store out of order")
                        << TErrorAttribute("first_dynamic_store_id", firstDynamicStore->GetId())
                        << TErrorAttribute("flushed_store_id", storeId);
                }
            }
        }

        tablet->SetStoresUpdatePreparedTransaction(transaction);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet stores update prepared (TransactionId: %v, TableId: %v, TabletId: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId);
    }

    void AttachChunksToTablet(TTablet* tablet, const std::vector<TChunkTree*>& chunkTrees)
    {
        auto* tabletChunkList = tablet->GetChunkList();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->AttachToTabletChunkList(tabletChunkList, chunkTrees);
    }

    void DetachChunksFromTablet(TTablet* tablet, const std::vector<TChunkTree*>& chunkTrees)
    {
        auto* tabletChunkList = tablet->GetChunkList();

        // Ensure deteministic ordering of keys.
        std::map<TChunkList*, std::vector<TChunkTree*>, TObjectRefComparer> childrenByParent;
        for (auto* child : chunkTrees) {
            int parentCount = GetParentCount(child);
            if (parentCount == 1) {
                auto* parent = GetUniqueParent(child);
                YT_VERIFY(parent->GetType() == EObjectType::ChunkList);
                childrenByParent[parent].push_back(child);
            } else {
                YT_VERIFY(HasParent(child, tabletChunkList));
                childrenByParent[tabletChunkList].push_back(child);
            }
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto pruneEmptyChunkList = [&] (TChunkList* chunkList) {
            while (chunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet && chunkList->Children().empty()) {
                auto* parent = GetUniqueParent(chunkList);
                chunkManager->DetachFromChunkList(parent, chunkList);
                chunkList = parent;
            }
        };

        for (const auto& [parent, children] : childrenByParent) {
            chunkManager->DetachFromChunkList(parent, children);
            pruneEmptyChunkList(parent);
        }
    }

    void HydraCommitUpdateTabletStores(TTransaction* transaction, NProto::TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        TWallTimer timer;
        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());
        auto counters = GetCounters(updateReason, tablet->GetTable());
        auto updateTime = Finally([&] {
            counters->UpdateTabletStoreTime.Add(timer.GetElapsedTime());
        });

        if (tablet->GetStoresUpdatePreparedTransaction() != transaction) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet stores update commit for an improperly prepared tablet; ignored "
                "(TabletId: %v, ExpectedTransactionId: %v, ActualTransactionId: %v)",
                tabletId,
                transaction->GetId(),
                GetObjectId(tablet->GetStoresUpdatePreparedTransaction()));
            return;
        }

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Invalid mount revision on tablet stores update commit; ignored "
                "(TabletId: %v, TransactionId: %v, ExpectedMountRevision: %llx, ActualMountRevision: %llx)",
                tabletId,
                transaction->GetId(),
                mountRevision,
                tablet->GetMountRevision());
            return;
        }

        auto* table = tablet->GetTable();
        if (!IsObjectAlive(table)) {
            return;
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetModified(table, EModificationType::Content);

        // Collect all changes first.
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        // Dynamic stores are also possible.
        std::vector<TChunkTree*> chunksToAttach;
        i64 attachedRowCount = 0;
        auto lastCommitTimestamp = table->GetLastCommitTimestamp();

        TChunk* flushedChunk = nullptr;

        auto validateChunkAttach = [&] (TChunk* chunk) {
            if (!IsObjectAlive(chunk)) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Attempt to attach a zombie chunk (ChunkId: %v)",
                    chunk->GetId());
            }
            if (chunk->HasParents()) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Attempt to attach a chunk that already has a parent (ChunkId: %v)",
                    chunk->GetId());
            }
        };

        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto type = TypeFromId(storeId);
            if (IsChunkTabletStoreType(type)) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                validateChunkAttach(chunk);
                const auto& miscExt = chunk->MiscExt();
                if (miscExt.has_max_timestamp()) {
                    lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt.max_timestamp()));
                }
                attachedRowCount += miscExt.row_count();
                chunksToAttach.push_back(chunk);
            } else if (IsDynamicTabletStoreType(type)) {
                if (IsDynamicStoreReadEnabled(table)) {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Attempt to attach dynamic store to a table "
                        "with readable dynamic stores (TableId: %v, TabletId: %v, StoreId: %v, Reason: %v)",
                        table->GetId(),
                        tablet->GetId(),
                        storeId,
                        updateReason);
                }
            } else {
                YT_ABORT();
            }
        }

        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            auto* chunk = chunkManager->GetChunkOrThrow(chunkId);
            validateChunkAttach(chunk);
            chunksToAttach.push_back(chunk);
        }

        if (updateReason == ETabletStoresUpdateReason::Flush) {
            YT_VERIFY(request->stores_to_add_size() <= 1);
            if (request->stores_to_add_size() == 1) {
                flushedChunk = chunksToAttach[0]->AsChunk();
            }

            if (request->request_dynamic_store_id()) {
                auto storeId = ReplaceTypeInId(
                    transaction->GetId(),
                    table->IsPhysicallySorted()
                        ? EObjectType::SortedDynamicTabletStore
                        : EObjectType::OrderedDynamicTabletStore);
                auto* dynamicStore = CreateDynamicStore(tablet, storeId);
                chunksToAttach.push_back(dynamicStore);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store attached to tablet during flush (TableId: %v, TabletId: %v, StoreId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    storeId);
            }
        }

        // Chunk views are also possible.
        std::vector<TChunkTree*> chunksOrViewsToDetach;

        i64 detachedRowCount = 0;
        bool flatteningRequired = false;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (IsChunkTabletStoreType(TypeFromId(storeId))) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                const auto& miscExt = chunk->MiscExt();
                detachedRowCount += miscExt.row_count();
                chunksOrViewsToDetach.push_back(chunk);
                flatteningRequired |= !CanUnambiguouslyDetachChild(tablet->GetChunkList(), chunk);
            } else if (TypeFromId(storeId) == EObjectType::ChunkView) {
                auto* chunkView = chunkManager->GetChunkViewOrThrow(storeId);
                auto* chunk = chunkView->GetUnderlyingChunk();
                const auto& miscExt = chunk->MiscExt();
                detachedRowCount += miscExt.row_count();
                chunksOrViewsToDetach.push_back(chunkView);
                flatteningRequired |= !CanUnambiguouslyDetachChild(tablet->GetChunkList(), chunkView);
            } else if (IsDynamicTabletStoreType(TypeFromId(storeId))) {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                auto* dynamicStore = chunkManager->FindDynamicStore(storeId);
                if (dynamicStore) {
                    YT_VERIFY(updateReason == ETabletStoresUpdateReason::Flush);
                    dynamicStore->SetFlushedChunk(flushedChunk);
                    if (!table->IsSorted()) {
                        // NB: Dynamic stores at the end of the chunk list do not contribute to row count,
                        // so the logical row count of the chunk list is exactly the number of rows
                        // in all tablet chunks.
                        dynamicStore->SetTableRowIndex(tablet->GetChunkList()->Statistics().LogicalRowCount);
                    }
                    chunksOrViewsToDetach.push_back(dynamicStore);
                }
            } else {
                YT_ABORT();
            }
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto* chunk = chunkManager->GetChunkOrThrow(chunkId);
            chunksOrViewsToDetach.push_back(chunk);
        }

        // Update last commit timestamp.
        table->SetLastCommitTimestamp(lastCommitTimestamp);

        // Update retained timestamp.
        auto retainedTimestamp = std::max(
            tablet->GetRetainedTimestamp(),
            static_cast<TTimestamp>(request->retained_timestamp()));
        tablet->SetRetainedTimestamp(retainedTimestamp);

        // Copy chunk tree if somebody holds a reference or if children cannot be detached unambiguously.
        CopyChunkListIfShared(table, tablet->GetIndex(), tablet->GetIndex(), flatteningRequired);

        // Save old tablet resource usage.
        auto oldMemorySize = tablet->GetTabletStaticMemorySize();
        auto oldStatistics = GetTabletStatistics(tablet);

        // Apply all requested changes.
        auto* tabletChunkList = tablet->GetChunkList();
        auto* cell = tablet->GetCell();

        if (!table->IsSorted() && IsDynamicStoreReadEnabled(table) && updateReason == ETabletStoresUpdateReason::Flush) {
            // NB: Flushing ordered tablet requires putting a certain chunk in place of a certain dynamic store.

            const auto& children = tabletChunkList->Children();
            YT_VERIFY(!children.empty());

            auto* dynamicStoreToRemove = chunksOrViewsToDetach[0]->AsDynamicStore();
            int firstDynamicStoreIndex = GetFirstDynamicStoreIndex(tabletChunkList);
            YT_VERIFY(dynamicStoreToRemove == children[firstDynamicStoreIndex]);

            std::vector<TChunkTree*> allDynamicStores(
                children.begin() + firstDynamicStoreIndex,
                children.end());

            if (allDynamicStores.size() > NTabletNode::DynamicStoreCountLimit) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Too many dynamic stores in ordered tablet chunk list "
                    "(TableId: %v, TabletId: %v, ChunkListId: %v, DynamicStoreCount: %v, "
                    "Limit: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    tabletChunkList->GetId(),
                    allDynamicStores.size(),
                    NTabletNode::DynamicStoreCountLimit);
            }

            chunkManager->DetachFromChunkList(tabletChunkList, allDynamicStores);

            if (flushedChunk) {
                chunkManager->AttachToChunkList(tabletChunkList, flushedChunk);
            }

            allDynamicStores.erase(allDynamicStores.begin());
            chunkManager->AttachToChunkList(tabletChunkList, allDynamicStores);

            if (request->request_dynamic_store_id()) {
                auto* dynamicStoreToAdd = chunksToAttach.back()->AsDynamicStore();
                chunkManager->AttachToChunkList(tabletChunkList, dynamicStoreToAdd);
            }
        } else {
            AttachChunksToTablet(tablet, chunksToAttach);
            DetachChunksFromTablet(tablet, chunksOrViewsToDetach);
        }

        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();

        // Get new tablet resource usage.
        auto newMemorySize = tablet->GetTabletStaticMemorySize();
        auto newStatistics = GetTabletStatistics(tablet);
        auto deltaStatistics = newStatistics - oldStatistics;

        // Update cell and table statistics.
        cell->GossipStatistics().Local() += deltaStatistics;
        table->DiscountTabletStatistics(oldStatistics);
        table->AccountTabletStatistics(newStatistics);

        // Update table resource usage.

        // Unstage just attached chunks.
        for (auto* chunk : chunksToAttach) {
            if (IsChunkTabletStoreType(chunk->GetType())) {
                chunkManager->UnstageChunk(chunk->AsChunk());
            }
        }

        // Requisition update pursues two goals: updating resource usage and
        // setting requisitions to correct values. The latter is required both
        // for detached chunks (for obvious reasons) and attached chunks
        // (because the protocol doesn't allow for creating chunks with correct
        // requisitions from the start).
        for (auto* chunk : chunksToAttach) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }
        for (auto* chunk : chunksOrViewsToDetach) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }

        if (tablet->GetStoresUpdatePreparedTransaction() == transaction) {
            tablet->SetStoresUpdatePreparedTransaction(nullptr);
        }

        UpdateResourceUsage(
            table,
            TTabletResources().SetTabletStaticMemory(newMemorySize - oldMemorySize));

        counters->UpdateTabletStoresStoreCount.Increment(chunksToAttach.size() + chunksOrViewsToDetach.size());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet stores update committed (TransactionId: %v, TableId: %v, TabletId: %v, "
            "AttachedChunkIds: %v, DetachedChunkOrViewIds: %v, "
            "AttachedRowCount: %v, DetachedRowCount: %v, RetainedTimestamp: %llx, UpdateReason: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId,
            MakeFormattableView(chunksToAttach, TObjectIdFormatter()),
            MakeFormattableView(chunksOrViewsToDetach, TObjectIdFormatter()),
            attachedRowCount,
            detachedRowCount,
            retainedTimestamp,
            updateReason);
    }

    void HydraAbortUpdateTabletStores(TTransaction* transaction, NProto::TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        if (tablet->GetStoresUpdatePreparedTransaction() != transaction) {
            return;
        }

        const auto* table = tablet->GetTable();

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet stores update aborted (TransactionId: %v, TableId: %v, TabletId: %v)",
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

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        if (tablet->GetState() == ETabletState::Unmounted) {
            return;
        }

        auto trimmedRowCount = request->trimmed_row_count();

        tablet->SetTrimmedRowCount(trimmedRowCount);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet trimmed row count updated (TabletId: %v, TrimmedRowCount: %v)",
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

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto* dynamicStore = CreateDynamicStore(tablet);
        AttachDynamicStoreToTablet(tablet, dynamicStore);

        TRspAllocateDynamicStore rsp;
        ToProto(rsp.mutable_dynamic_store_id(), dynamicStore->GetId());
        ToProto(rsp.mutable_tablet_id(), tabletId);
        rsp.set_mount_revision(tablet->GetMountRevision());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store allocated (StoreId: %v, TabletId: %v, TableId: %v)",
            dynamicStore->GetId(),
            tabletId,
            tablet->GetTable()->GetId());

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(tablet->GetCell()->GetId());
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
        std::optional<int> tabletCount = request->has_tablet_count()
            ? std::make_optional(request->tablet_count())
            : std::nullopt;

        TGuid correlationId;
        if (request->has_correlation_id()) {
            FromProto(&correlationId, request->correlation_id());
        }

        std::vector<TTablet*> tablets;
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
                false,
                correlationId,
                expirationTime);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), TError(ex), "Error creating tablet action (Kind: %v, "
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
        auto cellTag = request->cell_tag();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), "Received tablet cell bundle "
                "resource usage gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Received tablet cell bundle "
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

        for (auto [bundleId, bundleBase] : cellManager->CellBundles()) {
            if (!IsObjectAlive(bundleBase) || bundleBase->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

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
        request.set_cell_tag(multicellManager->GetCellTag());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto [bundleId, bundleBase] : cellManager->CellBundles()) {
            if (!IsObjectAlive(bundleBase) || bundleBase->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

            auto* bundle = bundleBase->As<TTabletCellBundle>();
            auto* entry = request.add_entries();
            ToProto(entry->mutable_bundle_id(), bundleId);

            if (multicellManager->IsPrimaryMaster()) {
                ToProto(entry->mutable_resource_usage(), bundle->ResourceUsage().Cluster());
            } else {
                ToProto(entry->mutable_resource_usage(), bundle->ResourceUsage().Local());
            }
        }

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }

        if (multicellManager->IsMulticell() && multicellManager->IsPrimaryMaster()) {
            NProto::TReqUpdateTabletCellBundleResourceUsage request;
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        }
    }

    void ValidateResourceUsageIncrease(
        const TTableNode* table,
        const TTabletResources& delta,
        TAccount* account = nullptr) const
    {
        // Old-fashioned account validation.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidateResourceUsageIncrease(
            account ? account : table->GetAccount(),
            ConvertToClusterResources(delta));

        // Brand-new bundle validation.
        if (GetDynamicConfig()->EnableTabletResourceValidation) {
            auto* bundle = table->GetTabletCellBundle();
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
        TTableNode* table,
        const TTabletResources& delta,
        bool scheduleTableDataStatisticsUpdate = true)
    {
        // Old-fashioned account accounting.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateTabletResourceUsage(
            table,
            ConvertToClusterResources(delta));

        // Brand-new bundle accounting.
        auto* bundle = table->GetTabletCellBundle();
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

        for (auto [bundleId, bundleBase] : cellManager->CellBundles()) {
            if (!IsObjectAlive(bundleBase) || bundleBase->GetType() != EObjectType::TabletCellBundle) {
                continue;
            }

            auto* bundle = bundleBase->As<TTabletCellBundle>();
            auto counters = GetOrCreateBundleProfilingCounters(bundle);
            bundle->OnProfiling(&counters);
        }
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        OnDynamicConfigChanged();

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
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        TabletCellDecommissioner_->Stop();
        TabletBalancer_->Stop();
        TabletActionManager_->Stop();

        if (TabletCellStatisticsGossipExecutor_) {
            TabletCellStatisticsGossipExecutor_->Stop();
            TabletCellStatisticsGossipExecutor_.Reset();
        }

        if (BundleResourceUsageGossipExecutor_) {
            BundleResourceUsageGossipExecutor_->Stop();
            BundleResourceUsageGossipExecutor_.Reset();
        }

        if (ProfilingExecutor_) {
            ProfilingExecutor_->Stop();
            ProfilingExecutor_.Reset();
        }

        BundleIdToProfilingCounters_.clear();
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


    std::vector<std::pair<TTablet*, TTabletCell*>> ComputeTabletAssignment(
        TTableNode* table,
        TTableMountConfigPtr mountConfig,
        TTabletCell* hintCell,
        std::vector<TTablet*> tabletsToMount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (IsCellActive(hintCell)) {
            std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
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
            switch (mountConfig->InMemoryMode) {
                case EInMemoryMode::None:
                    result = mutationContext->RandomGenerator().Generate<i64>();
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
        for (auto* cellBase : GetValuesSortedByKey(table->GetTabletCellBundle()->Cells())) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            if (!IsCellActive(cell)) {
                continue;
            }

            if (cell->GetCellBundle() == table->GetTabletCellBundle()) {
                cellKeys.push_back(TCellKey{getCellSize(cell), cell});
            }
        }
        if (cellKeys.empty()) {
            cellKeys.push_back(TCellKey{0, nullptr});
        }
        std::sort(cellKeys.begin(), cellKeys.end());

        auto getTabletSize = [&] (const TTablet* tablet) -> i64 {
            i64 result = 0;
            auto statistics = GetTabletStatistics(tablet);
            switch (mountConfig->InMemoryMode) {
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
            [&] (const TTablet* lhs, const TTablet* rhs) {
                return
                    std::make_tuple(getTabletSize(lhs), lhs->GetId()) >
                    std::make_tuple(getTabletSize(rhs), rhs->GetId());
            });

        // Assign tablets to cells iteratively looping over cell array.
        int cellIndex = 0;
        std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
        for (auto* tablet : tabletsToMount) {
            assignment.emplace_back(tablet, cellKeys[cellIndex].Cell);
            if (++cellIndex == cellKeys.size()) {
                cellIndex = 0;
            }
        }

        return assignment;
    }

    void DoUnmountTable(
        TTableNode* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex,
        bool onDestroy)
    {
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            DoUnmountTablet(tablet, force, onDestroy);
        }
    }

    void DoUnmountTablet(
        TTablet* tablet,
        bool force,
        bool onDestroy)
    {
        auto state = tablet->GetState();
        if (state == ETabletState::Unmounted) {
            return;
        }
        if (!force) {
            YT_VERIFY(state == ETabletState::Mounted ||
                state == ETabletState::Frozen ||
                state == ETabletState::Freezing ||
                state == ETabletState::Unmounting);
        }

        auto* table = tablet->GetTable();

        auto* cell = tablet->GetCell();
        YT_VERIFY(cell);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Unmounting tablet (TableId: %v, TabletId: %v, CellId: %v, Force: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId(),
            force);

        tablet->SetState(ETabletState::Unmounting);

        const auto& hiveManager = Bootstrap_->GetHiveManager();

        TReqUnmountTablet request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_force(force);
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
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
            DoTabletUnmounted(tablet, /*force*/ true);
        }
    }

    void ValidateTabletStaticMemoryUpdate(
        const TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const TTableMountConfigPtr& mountConfig)
    {
        i64 memorySize = 0;

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = table->Tablets()[index];
            if (tablet->GetState() != ETabletState::Unmounted) {
                continue;
            }
            memorySize += tablet->GetTabletStaticMemorySize(mountConfig->InMemoryMode);
        }

        ValidateResourceUsageIncrease(
            table,
            TTabletResources().SetTabletStaticMemory(memorySize));
    }

    void ValidateTableMountConfig(
        const TTableNode* table,
        const TTableMountConfigPtr& mountConfig)
    {
        if (table->IsReplicated() && mountConfig->InMemoryMode != EInMemoryMode::None) {
            THROW_ERROR_EXCEPTION("Cannot mount a replicated dynamic table in memory");
        }
        if (!table->IsPhysicallySorted() && mountConfig->EnableLookupHashTable) {
            THROW_ERROR_EXCEPTION("\"enable_lookup_hash_table\" can be \"true\" only for sorted dynamic table");
        }
    }

    bool IsDynamicStoreReadEnabled(const TTableNode* table)
    {
        if (table->IsReplicated()) {
            return false;
        }

        if (table->GetActualTabletState() == ETabletState::Unmounted) {
            return table->GetEnableDynamicStoreRead().value_or(
                GetDynamicConfig()->EnableDynamicStoreReadByDefault);
        } else {
            return table->GetMountedWithEnabledDynamicStoreRead();
        }
    }

    static TError TryParseTabletRange(
        TTableNode* table,
        int* first,
        int* last)
    {
        auto& tablets = table->Tablets();
        if (*first == -1 && *last == -1) {
            *first = 0;
            *last = static_cast<int>(tablets.size() - 1);
        } else {
            if (*first < 0 || *first >= tablets.size()) {
                return TError("First tablet index %v is out of range [%v, %v]",
                    *first,
                    0,
                    tablets.size() - 1);
            }
            if (*last < 0 || *last >= tablets.size()) {
                return TError("Last tablet index %v is out of range [%v, %v]",
                    *last,
                    0,
                    tablets.size() - 1);
            }
            if (*first > *last) {
               return TError("First tablet index is greater than last tablet index");
            }
        }

        return TError();
    }

    static void ParseTabletRangeOrThrow(
        TTableNode* table,
        int* first,
        int* last)
    {
        TryParseTabletRange(table, first, last)
            .ThrowOnError();
    }

    static void ParseTabletRange(
        TTableNode* table,
        int* first,
        int* last)
    {
        auto error = TryParseTabletRange(table, first, last);
        YT_VERIFY(error.IsOK());
    }

    std::pair<std::vector<TTablet*>::iterator, std::vector<TTablet*>::iterator> GetIntersectingTablets(
        std::vector<TTablet*>& tablets,
        const NChunkClient::TLegacyReadRange readRange)
    {
        YT_VERIFY(readRange.LowerLimit().HasLegacyKey());
        YT_VERIFY(readRange.UpperLimit().HasLegacyKey());
        const auto& minKey = readRange.LowerLimit().GetLegacyKey();
        const auto& maxKey = readRange.UpperLimit().GetLegacyKey();

        auto beginIt = std::upper_bound(
            tablets.begin(),
            tablets.end(),
            minKey,
            [] (const TLegacyOwningKey& key, const TTablet* tablet) {
                return key < tablet->GetPivotKey();
            });

        if (beginIt != tablets.begin()) {
            --beginIt;
        }

        auto endIt = beginIt;
        while (endIt != tablets.end() && maxKey > (*endIt)->GetPivotKey()) {
            ++endIt;
        }

        return std::make_pair(beginIt, endIt);
    }

    static EStoreType GetStoreType(
        const TTableNode* table,
        const TChunkTree* chunkOrView)
    {
        if (IsPhysicalChunkType(chunkOrView->GetType())) {
            const auto* chunk = chunkOrView->AsChunk();
            if (FromProto<EChunkType>(chunk->ChunkMeta().type()) == EChunkType::Hunk) {
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
            chunk = chunkView->GetUnderlyingChunk();
            auto* viewDescriptor = descriptor->mutable_chunk_view_descriptor();
            ToProto(viewDescriptor->mutable_chunk_view_id(), chunkView->GetId());
            ToProto(viewDescriptor->mutable_underlying_chunk_id(), chunk->GetId());
            ToProto(viewDescriptor->mutable_read_range(), chunkView->ReadRange());

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto timestamp = transactionManager->GetTimestampHolderTimestamp(chunkView->GetTransactionId());
            if (timestamp) {
                viewDescriptor->set_timestamp(timestamp);
            }
        } else {
            chunk = chunkOrView->AsChunk();
        }

        descriptor->mutable_chunk_meta()->CopyFrom(chunk->ChunkMeta());
        descriptor->set_starting_row_index(*startingRowIndex);
        *startingRowIndex += chunk->MiscExt().row_count();
    }

    void FillHunkChunkDescriptor(
        const TChunk* chunk,
        NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor)
    {
        ToProto(descriptor->mutable_chunk_id(), chunk->GetId());
        descriptor->mutable_chunk_meta()->CopyFrom(chunk->ChunkMeta());
    }

    void SetTabletEdenStoreIds(TTablet* tablet, std::vector<TStoreId> edenStoreIds)
    {
        i64 masterMemoryUsageDelta = -static_cast<i64>(tablet->EdenStoreIds().size() * sizeof(TStoreId));
        if (edenStoreIds.size() <= EdenStoreIdsSizeLimit) {
            tablet->EdenStoreIds() = std::move(edenStoreIds);
            tablet->EdenStoreIds().shrink_to_fit();
        } else {
            tablet->EdenStoreIds() = {};
        }
        masterMemoryUsageDelta += tablet->EdenStoreIds().size() * sizeof(TStoreId);

        auto* table = tablet->GetTable();
        table->SetTabletMasterMemoryUsage(
            table->GetTabletMasterMemoryUsage() + masterMemoryUsageDelta);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
    }

    void ValidateNodeCloneMode(TTableNode* trunkNode, ENodeCloneMode mode)
    {
        try {
            switch (mode) {
                case ENodeCloneMode::Copy:
                    trunkNode->ValidateAllTabletsFrozenOrUnmounted("Cannot copy dynamic table");
                    break;

                case ENodeCloneMode::Move:
                    if (trunkNode->IsReplicated()) {
                        THROW_ERROR_EXCEPTION("Cannot move a replicated table");
                    }
                    trunkNode->ValidateAllTabletsUnmounted("Cannot move dynamic table");
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


    static void PopulateTableReplicaDescriptor(TTableReplicaDescriptor* descriptor, const TTableReplica* replica, const TTableReplicaInfo& info)
    {
        ToProto(descriptor->mutable_replica_id(), replica->GetId());
        descriptor->set_cluster_name(replica->GetClusterName());
        descriptor->set_replica_path(replica->GetReplicaPath());
        descriptor->set_start_replication_timestamp(replica->GetStartReplicationTimestamp());
        descriptor->set_mode(ToProto<int>(replica->GetMode()));
        descriptor->set_preserve_timestamps(replica->GetPreserveTimestamps());
        descriptor->set_atomicity(ToProto<int>(replica->GetAtomicity()));
        PopulateTableReplicaStatisticsFromInfo(descriptor->mutable_statistics(), info);
    }

    static void PopulateTableReplicaStatisticsFromInfo(TTableReplicaStatistics* statistics, const TTableReplicaInfo& info)
    {
        statistics->set_current_replication_row_index(info.GetCurrentReplicationRowIndex());
        statistics->set_current_replication_timestamp(info.GetCurrentReplicationTimestamp());
    }

    static void PopulateTableReplicaInfoFromStatistics(TTableReplicaInfo* info, const TTableReplicaStatistics& statistics)
    {
        // Updates may be reordered but we can rely on monotonicity here.
        info->SetCurrentReplicationRowIndex(std::max(
            info->GetCurrentReplicationRowIndex(),
            statistics.current_replication_row_index()));
        info->SetCurrentReplicationTimestamp(std::max(
            info->GetCurrentReplicationTimestamp(),
            statistics.current_replication_timestamp()));
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TabletMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TableReplica, TTableReplica, TableReplicaMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletAction, TTabletAction, TabletActionMap_)

////////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTabletManager::~TTabletManager() = default;

void TTabletManager::Initialize()
{
    return Impl_->Initialize();
}

TTabletStatistics TTabletManager::GetTabletStatistics(const TTablet* tablet)
{
    return Impl_->GetTabletStatistics(tablet);
}

void TTabletManager::PrepareMountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCellId hintCellId,
    const std::vector<TTabletCellId>& targetCellIds,
    bool freeze)
{
    Impl_->PrepareMountTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        hintCellId,
        targetCellIds,
        freeze);
}

void TTabletManager::PrepareUnmountTable(
    TTableNode* table,
    bool force,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareUnmountTable(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareRemountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareRemountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareFreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareFreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareUnfreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->PrepareUnfreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::PrepareReshardTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<TLegacyOwningKey>& pivotKeys,
    bool create)
{
    Impl_->PrepareReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys,
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

void TTabletManager::MountTable(
    TTableNode* table,
    const TString& path,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCellId hintCellId,
    const std::vector<TTabletCellId>& targetCellIds,
    bool freeze,
    TTimestamp mountTimestamp)
{
    Impl_->MountTable(
        table,
        path,
        firstTabletIndex,
        lastTabletIndex,
        hintCellId,
        targetCellIds,
        freeze,
        mountTimestamp);
}

void TTabletManager::UnmountTable(
    TTableNode* table,
    bool force,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->UnmountTable(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::RemountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->RemountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::FreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->FreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::UnfreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->UnfreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::DestroyTable(TTableNode* table)
{
    Impl_->DestroyTable(table);
}

void TTabletManager::ReshardTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<TLegacyOwningKey>& pivotKeys)
{
    Impl_->ReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys);
}

void TTabletManager::ValidateCloneTable(
    TTableNode* sourceTable,
    ENodeCloneMode mode,
    TAccount* account)
{
    return Impl_->ValidateCloneTable(
        sourceTable,
        mode,
        account);
}

void TTabletManager::ValidateBeginCopyTable(
    TTableNode* sourceTable,
    ENodeCloneMode mode)
{
    return Impl_->ValidateBeginCopyTable(
        sourceTable,
        mode);
}

void TTabletManager::CloneTable(
    TTableNode* sourceTable,
    TTableNode* clonedTable,
    ENodeCloneMode mode)
{
    return Impl_->CloneTable(
        sourceTable,
        clonedTable,
        mode);
}

void TTabletManager::MakeTableDynamic(TTableNode* table)
{
    Impl_->MakeTableDynamic(table);
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

TTablet* TTabletManager::GetTabletOrThrow(TTabletId id)
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

void TTabletManager::SetTabletCellBundle(TTableNode* table, TTabletCellBundle* cellBundle)
{
    Impl_->SetTabletCellBundle(table, cellBundle);
}

void TTabletManager::DestroyTablet(TTablet* tablet)
{
    Impl_->DestroyTablet(tablet);
}

void TTabletManager::ZombifyTabletCell(TTabletCell* cell)
{
    Impl_->ZombifyTabletCell(cell);
}

TNode* TTabletManager::FindTabletLeaderNode(const TTablet* tablet) const
{
    return Impl_->FindTabletLeaderNode(tablet);
}

TTableReplica* TTabletManager::CreateTableReplica(
    TReplicatedTableNode* table,
    const TString& clusterName,
    const TYPath& replicaPath,
    ETableReplicaMode mode,
    bool preserveTimestamps,
    NTransactionClient::EAtomicity atomicity,
    TTimestamp startReplicationTimestamp,
    const  std::optional<std::vector<i64>>& startReplicationRowIndexes)
{
    return Impl_->CreateTableReplica(
        table,
        clusterName,
        replicaPath,
        mode,
        preserveTimestamps,
        atomicity,
        startReplicationTimestamp,
        startReplicationRowIndexes);
}

void TTabletManager::DestroyTableReplica(TTableReplica* replica)
{
    Impl_->DestroyTableReplica(replica);
}

void TTabletManager::AlterTableReplica(
    TTableReplica* replica,
    std::optional<bool> enabled,
    std::optional<ETableReplicaMode> mode,
    std::optional<NTransactionClient::EAtomicity> atomicity,
    std::optional<bool> preserveTimestamps)
{
    Impl_->AlterTableReplica(
        replica,
        std::move(enabled),
        std::move(mode),
        std::move(atomicity),
        std::move(preserveTimestamps));
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
    const std::vector<TTablet*>& tablets,
    const std::vector<TTabletCell*>& cells,
    const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
    const std::optional<int>& tabletCount,
    bool skipFreezing,
    TGuid correlationId,
    TInstant expirationTime)
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
        expirationTime);
}

void TTabletManager::DestroyTabletAction(TTabletAction* action)
{
    Impl_->DestroyTabletAction(action);
}

void TTabletManager::MergeTable(TTableNode* originatingNode, NTableServer::TTableNode* branchedNode)
{
    Impl_->MergeTable(originatingNode, branchedNode);
}

void TTabletManager::RecomputeTabletCellStatistics(TCellBase* cellBase)
{
    return Impl_->RecomputeTabletCellStatistics(cellBase);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TableReplica, TTableReplica, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletAction, TTabletAction, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
