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
#include "tablet_service.h"
#include "tablet_type_handler.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_view.h>
#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/server/master/chunk_server/dynamic_store.h>
#include <yt/server/master/chunk_server/helpers.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/helpers.h>

#include <yt/server/lib/misc/interned_attributes.h>
#include <yt/server/lib/misc/profiling_helpers.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/subject.h>

#include <yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/server/master/table_server/table_node.h>
#include <yt/server/master/table_server/replicated_table_node.h>

#include <yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/server/master/table_server/shared_table_schema.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/tablet_client/config.h>
#include <yt/ytlib/tablet_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/random_access_queue.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/tls_cache.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ypath/token.h>

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
using namespace NTabletServer::NProto;
using namespace NTransactionServer;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCellServer;
using namespace NProfiling;

using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerServer::NProto::TReqIncrementalHeartbeat;
using NTabletNode::EStoreType;
using NTabletNode::TTableMountConfigPtr;
using NTabletNode::DynamicStoreIdPoolSize;
using NTransactionServer::TTransaction;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;
static const auto& Profiler = TabletServerProfiler;

////////////////////////////////////////////////////////////////////////////////

struct TProfilingCounters
{
    TProfilingCounters(const TTagIdList& list)
        : CopyChunkListIfSharedActionCount("/copy_chunk_list_if_shared/action_count", list)
        , UpdateTabletStoresStoreCount("/update_tablet_stores/store_count", list)
    { }

    TMonotonicCounter CopyChunkListIfSharedActionCount;
    TMonotonicCounter UpdateTabletStoresStoreCount;
};

using TTabletManagerProfilerTrait = TTagListProfilerTrait<TProfilingCounters>;

static TMonotonicCounter IncrementalHeartbeatCounter("/incremental_heartbeat");

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap,  NCellMaster::EAutomatonThreadQueue::TabletManager)
        , Config_(config)
        , TabletService_(New<TTabletService>(Bootstrap_))
        , TabletBalancer_(New<TTabletBalancer>(Bootstrap_))
        , TabletCellDecommissioner_(New<TTabletCellDecommissioner>(Bootstrap_))
        , TabletActionManager_(New<TTabletActionManager>(Bootstrap_))
        , TableStatisticsGossipThrottler_(CreateReconfigurableThroughputThrottler(
            New<TThroughputThrottlerConfig>(),
            TabletServerLogger,
            TabletServerProfiler.AppendPath("/table_statistics_gossip_throttler")))
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
        RegisterMethod(BIND(&TImpl::HydraSendTableStatisticsUpdates, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTableStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateUpstreamTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAllocateDynamicStore, Unretained(this)));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
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

        FillProfilerTags();

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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet created (TableId: %v, TabletId: %v)",
            table->GetId(),
            tablet->GetId());

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // XXX(savrus): this is a workaround for YTINCIDENTS-42
        if (auto* cell = tablet->GetCell()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Destroying tablet with non-null tablet cell (TabletId: %v, CellId: %v)",
                tablet->GetId(),
                cell->GetId());

            // Replicated table not supported.
            YT_VERIFY(tablet->Replicas().empty());

            if (cell->Tablets().erase(tablet) > 0) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Unbinding tablet from tablet cell since tablet is destroyed (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    cell->GetId());
            }

            if (tablet->GetState() == ETabletState::Mounted) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Sending force unmount request to node since tablet is destroyed (TabletId: %v, CellId: %v)",
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica created (TableId: %v, ReplicaId: %v, Mode: %v, StartReplicationTimestamp: %llx)",
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica updated (TableId: %v, ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v)",
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
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Enabling table replica (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabling);
            } else {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Disabling table replica (TableId: %v, ReplicaId: %v)",
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
                req.set_mode(static_cast<int>(*mode));
            }
            if (atomicity) {
                req.set_atomicity(static_cast<int>(*atomicity));
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
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
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
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->LockNode(table, nullptr, ELockMode::Exclusive);

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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet action destroyed (ActionId: %v, TabletBalancerCorrelationId: %v)",
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
        bool freeze,
        TTimestamp mountTimestamp)
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

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
        NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);
        ValidateTableMountConfig(table, mountConfig);
        ValidateTabletStaticMemoryUpdate(
            table,
            firstTabletIndex,
            lastTabletIndex,
            mountConfig);

        if (mountConfig->InMemoryMode != EInMemoryMode::None &&
            writerOptions->ErasureCodec != NErasure::ECodec::None)
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

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
        NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;

        GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex);

        auto serializedMountConfig = ConvertToYsonString(mountConfig);
        auto serializedReaderConfig = ConvertToYsonString(readerConfig);
        auto serializedWriterConfig = ConvertToYsonString(writerConfig);
        auto serializedWriterOptions = ConvertToYsonString(writerOptions);

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
                mountConfig,
                hintCell,
                std::move(tabletsToMount));
        }

        table->SetMountedWithEnabledDynamicStoreRead(IsDynamicStoreReadEnabled(table));

        DoMountTablets(
            table,
            assignment,
            mountConfig->InMemoryMode,
            freeze,
            serializedMountConfig,
            serializedReaderConfig,
            serializedWriterConfig,
            serializedWriterOptions,
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

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
        NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);
        ValidateTableMountConfig(table, mountConfig);

        if (mountConfig->InMemoryMode != EInMemoryMode::None &&
            writerOptions->ErasureCodec != NErasure::ECodec::None)
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

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
        NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);
        auto serializedMountConfig = ConvertToYsonString(mountConfig);
        auto serializedReaderConfig = ConvertToYsonString(readerConfig);
        auto serializedWriterConfig = ConvertToYsonString(writerConfig);
        auto serializedWriterOptions = ConvertToYsonString(writerOptions);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();
            auto state = tablet->GetState();

            if (state != ETabletState::Unmounted) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Remounting tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                cell->GossipStatistics().Local() -= GetTabletStatistics(tablet);
                tablet->SetInMemoryMode(mountConfig->InMemoryMode);
                cell->GossipStatistics().Local() += GetTabletStatistics(tablet);

                const auto& hiveManager = Bootstrap_->GetHiveManager();

                TReqRemountTablet request;
                request.set_mount_config(serializedMountConfig.GetData());
                request.set_reader_config(serializedReaderConfig.GetData());
                request.set_writer_config(serializedWriterConfig.GetData());
                request.set_writer_options(serializedWriterOptions.GetData());
                ToProto(request.mutable_tablet_id(), tablet->GetId());

                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }

        CommitTabletStaticMemoryUpdate(table, resourceUsageBefore, table->GetTabletResourceUsage());
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
        const std::vector<TOwningKey>& pivotKeys,
        bool create = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        // First, check parameters with little knowledge of the table.
        // Primary master must ensure that the table could be created.

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        if (table->IsReplicated() && !table->IsEmpty()) {
            THROW_ERROR_EXCEPTION("Cannot reshard non-empty replicated table");
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
            securityManager->ValidateResourceUsageIncrease(
                table->GetAccount(),
                TClusterResources().SetTabletCount(newTabletCount - oldTabletCount));
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
                THROW_ERROR_EXCEPTION("Pivot keys must be porovided to reshard a replicated table");
            }
        } else {
            if (!pivotKeys.empty()) {
                THROW_ERROR_EXCEPTION("Table is ordered; must provide tablet count");
            }
        }

        if (table->IsExternal()) {
            return;
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

        securityManager->ValidateResourceUsageIncrease(
            table->GetAccount(),
            TClusterResources().SetTabletCount(newTabletCount - oldTabletCount));

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
        const std::vector<TOwningKey>& pivotKeys)
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

            if (tablet->GetState() != ETabletState::Unmounted) {
                totalMemorySizeDelta -= tablet->GetTabletStaticMemorySize();

                auto* cell = tablet->GetCell();
                cell->GossipStatistics().Local() -= GetTabletStatistics(tablet);
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
            auto* appendChunkList = branchedChunkList->Children()[index];
            auto* tabletChunkList = originatingChunkList->Children()[index]->AsChunkList();
            auto* tablet = originatingNode->Tablets()[index];

            if (updateMode == EUpdateMode::Append) {
                if (!appendChunkList->AsChunkList()->Children().empty()) {
                    chunkManager->AttachToChunkList(tabletChunkList, appendChunkList);
                }
            }

            if (tablet->GetState() == ETabletState::Unmounted) {
                continue;
            }

            auto newMemorySize = tablet->GetTabletStaticMemorySize();
            auto newStatistics = GetTabletStatistics(tablet);

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

            auto stores = EnumerateStoresInChunkTree(appendChunkList->AsChunkList());
            auto storeType = originatingNode->IsPhysicallySorted() ? EStoreType::SortedChunk : EStoreType::OrderedChunk;
            i64 startingRowIndex = 0;

            for (const auto* store : stores) {
                auto* descriptor = req.add_stores_to_add();
                FillStoreDescriptor(store, storeType, descriptor, &startingRowIndex);
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

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto resourceUsageDelta = TClusterResources()
            .SetTabletStaticMemory(totalMemorySizeDelta);
        securityManager->UpdateTabletResourceUsage(originatingNode, resourceUsageDelta);
        ScheduleTableStatisticsUpdate(originatingNode);

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
            &context,
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

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        auto* trunkSourceTable = sourceTable->GetTrunkNode();
        securityManager->ValidateResourceUsageIncrease(
            account,
            TClusterResources().SetTabletCount(trunkSourceTable->GetTabletResourceUsage().TabletCount));

        ValidateNodeCloneMode(trunkSourceTable, mode);

        if (auto* cellBundle = trunkSourceTable->GetTabletCellBundle()) {
            cellBundle->ValidateActiveLifeStage();
        }
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
            cellBundle->ValidateActiveLifeStage();
        }
    }

    void ValidateEndCopyTable(TAccount* account)
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
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            YT_LOG_ALERT_UNLESS(IsRecovery(), ex, "Error cloning table (TableId: %v, User: %v)",
                sourceTable->GetId(),
                securityManager->GetAuthenticatedUserName());
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

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        securityManager->UpdateTabletResourceUsage(
            trunkClonedTable,
            trunkClonedTable->GetTabletResourceUsage());
        ScheduleTableStatisticsUpdate(trunkClonedTable, false);
    }


    void ValidateMakeTableDynamic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(table->IsTrunk());

        if (table->IsDynamic()) {
            return;
        }

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        securityManager->ValidateResourceUsageIncrease(table->GetAccount(), TClusterResources().SetTabletCount(1));
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

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();

        auto* oldRootChunkList = table->GetChunkList();

        std::vector<TChunk*> chunks;
        EnumerateChunksInChunkTree(oldRootChunkList, &chunks);

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

        auto tabletResourceUsage = table->GetTabletResourceUsage();
        securityManager->UpdateTabletResourceUsage(table, tabletResourceUsage);
        securityManager->UpdateMasterMemoryUsage(table);
        ScheduleTableStatisticsUpdate(table, false);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table is switched to dynamic mode (TableId: %v)",
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

        auto tabletResourceUsage = table->GetTabletResourceUsage();

        auto* oldRootChunkList = table->GetChunkList();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto newRootChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(newRootChunkList);

        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);

        std::vector<TChunk*> chunks;
        EnumerateChunksInChunkTree(oldRootChunkList, &chunks);
        std::vector<TChunkTree*> chunkTrees(chunks.begin(), chunks.end());
        chunkManager->AttachToChunkList(newRootChunkList, chunkTrees);

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
        securityManager->UpdateTabletResourceUsage(table, -tabletResourceUsage);
        securityManager->UpdateMasterMemoryUsage(table);
        ScheduleTableStatisticsUpdate(table, false);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table is switched to static mode (TableId: %v)",
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Waiting for tablet lock confirmation (TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
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

    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const TString& name)
    {
        auto* cellBundle = FindTabletCellBundleByName(name);
        if (!cellBundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell bundle %Qv",
                name);
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

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            if (table->GetTabletCellBundle() && table->IsDynamic()) {
                table->ValidateAllTabletsUnmounted("Cannot change tablet cell bundle");
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        if (oldBundle) {
            objectManager->UnrefObject(oldBundle);
        }

        table->SetTabletCellBundle(newBundle);
        objectManager->RefObject(newBundle);
    }

    void SetTabletCellBundle(TCompositeNodeBase* node, TTabletCellBundle* newBundle)
    {
        auto* oldBundle = node->GetTabletCellBundle();
        if (oldBundle == newBundle) {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();

        if (oldBundle) {
            objectManager->UnrefObject(oldBundle);
        }

        node->SetTabletCellBundle(newBundle);
        objectManager->RefObject(newBundle);
    }


    void SendTableStatisticsUpdates(TChunkOwnerBase* chunkOwner)
    {
        if (chunkOwner->IsNative()) {
            return;
        }

        YT_VERIFY(chunkOwner->GetType() == EObjectType::Table);
        auto* table = chunkOwner->As<TTableNode>();
        YT_VERIFY(table->IsDynamic());

        SendTableStatisticsUpdates(table);
    }


    TEntityMap<TTabletCellBundle>& CompatTabletCellBundleMap()
    {
        return CompatTabletCellBundleMap_;
    }

    TEntityMap<TTabletCell>& CompatTabletCellMap()
    {
        return CompatTabletCellMap_;
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

private:
    const TTabletManagerConfigPtr Config_;

    const TTabletServicePtr TabletService_;
    const TTabletBalancerPtr TabletBalancer_;
    const TTabletCellDecommissionerPtr TabletCellDecommissioner_;
    const TTabletActionManagerPtr TabletActionManager_;

    TEntityMap<TTabletCellBundle> CompatTabletCellBundleMap_;
    TEntityMap<TTabletCell> CompatTabletCellMap_;
    TEntityMap<TTablet> TabletMap_;
    TEntityMap<TTableReplica> TableReplicaMap_;
    TEntityMap<TTabletAction> TabletActionMap_;

    struct TTableStatisticsUpdateRequest
    {
        bool UpdateDataStatistics;
        bool UpdateTabletResourceUsage;
        bool UpdateModificationTime;
        bool UpdateAccessTime;

        void Persist(NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            // COMPAT(savrus)
            if ((context.GetVersion() >= EMasterReign::TuneTabletStatisticsUpdate_19_8 && context.GetVersion() <= EMasterReign::TruncateJournals) ||
                context.GetVersion() >= EMasterReign::TuneTabletStatisticsUpdate_20_2)
            {
                Persist(context, UpdateDataStatistics);
                Persist(context, UpdateTabletResourceUsage);
                Persist(context, UpdateModificationTime);
                Persist(context, UpdateAccessTime);
            } else {
                std::optional<TDataStatistics> dataStatistics;
                std::optional<TClusterResources> tabletResourceUsage;
                std::optional<TInstant> modificationTime;
                std::optional<TInstant> accessTime;

                Persist(context, dataStatistics);
                Persist(context, tabletResourceUsage);
                Persist(context, modificationTime);
                Persist(context, accessTime);

                UpdateDataStatistics = dataStatistics.has_value();
                UpdateTabletResourceUsage = tabletResourceUsage.has_value();
                UpdateModificationTime = modificationTime.has_value();
                UpdateAccessTime = accessTime.has_value();
            }
        }
    };

    TRandomAccessQueue<TTableId, TTableStatisticsUpdateRequest> TabletStatistisUpdateRequests_;

    TPeriodicExecutorPtr TabletCellStatisticsGossipExecutor_;
    TPeriodicExecutorPtr TableStatisticsGossipExecutor_;

    IReconfigurableThroughputThrottlerPtr TableStatisticsGossipThrottler_;

    TEnumIndexedVector<ETabletStoresUpdateReason, TTagId> UpdateTabletStoresProfilerTags_;
    TTagId SortedTableProfilerTag_;
    TTagId OrderedTableProfilerTag_;

    TTabletCellBundleId DefaultTabletCellBundleId_;
    TTabletCellBundle* DefaultTabletCellBundle_ = nullptr;

    bool RecomputeTabletCellStatistics_ = false;
    bool EnableUpdateStatisticsOnHeartbeat_ = true;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);



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
    }

    TTabletCellBundle* FindTabletCellBundleByName(const TString& name)
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* bundle = cellManager->FindCellBundleByName(name);
        if (!bundle) {
            return nullptr;
        }
        return bundle->GetType() == EObjectType::TabletCellBundle
            ? bundle->As<TTabletCellBundle>()
            : nullptr;
    }


    TTabletAction* DoCreateTabletAction(
        TObjectId hintId,
        ETabletActionKind kind,
        ETabletActionState state,
        const std::vector<TTablet*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet action created (%v)",
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

        action->Tablets().clear();
    }

    void UnbindTabletAction(TTabletAction* action)
    {
        UnbindTabletActionFromTablets(action);
        UnbindTabletActionFromCells(action);
    }


    std::vector<TOwningKey> CalculatePivotKeys(
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
            TOwningKey MinKey;
            TOwningKey MaxKey;
            i64 Size;

            bool operator<(const TEntry& other) const
            {
                return MinKey < other.MinKey;
            }
        };

        std::vector<TEntry> entries;
        i64 totalSize = 0;

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto chunksOrViews = EnumerateStoresInChunkTree(
                table->GetChunkList()->Children()[index]->AsChunkList());

            for (const auto* chunkOrView : chunksOrViews) {
                const auto* chunk = chunkOrView->GetType() == EObjectType::ChunkView
                    ? chunkOrView->AsChunkView()->GetUnderlyingChunk()
                    : chunkOrView->AsChunk();
                if (chunk->MiscExt().eden()) {
                    continue;
                }

                i64 size = chunk->MiscExt().uncompressed_data_size();
                entries.push_back({
                    GetMinKeyOrThrow(chunkOrView),
                    GetUpperBoundKeyOrThrow(chunkOrView),
                    size});
                totalSize += size;
            }
        }

        std::sort(entries.begin(), entries.end());

        i64 desired = totalSize / newTabletCount;
        std::vector<TOwningKey> pivotKeys{table->Tablets()[firstTabletIndex]->GetPivotKey()};
        TOwningKey lastKey;
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
                YT_LOG_ERROR_UNLESS(IsRecovery(), ex, "Error mounting missed in action tablet "
                    "(TabletId: %v, ActionId: %v, TabletBalancerCorrelationId: %v)",
                    tablet->GetId(),
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
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Change tablet action state (ActionId: %v, State: %v, TabletBalancerCorrelationId: %v)",
            action->GetId(),
            state,
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

                TTableMountConfigPtr mountConfig;
                NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
                NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
                NTabletNode::TTabletWriterOptionsPtr writerOptions;
                GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);
                auto serializedMountConfig = ConvertToYsonString(mountConfig);
                auto serializedReaderConfig = ConvertToYsonString(readerConfig);
                auto serializedWriterConfig = ConvertToYsonString(writerConfig);
                auto serializedWriterOptions = ConvertToYsonString(writerOptions);

                std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
                if (action->TabletCells().empty()) {
                    if (!CheckHasHealthyCells(table->GetTabletCellBundle())) {
                        ChangeTabletActionState(action, ETabletActionState::Orphaned, false);
                        break;
                    }

                    assignment = ComputeTabletAssignment(
                        table,
                        mountConfig,
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
                    mountConfig->InMemoryMode,
                    action->GetFreeze(),
                    serializedMountConfig,
                    serializedReaderConfig,
                    serializedWriterConfig,
                    serializedWriterOptions);

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
                YT_LOG_DEBUG_UNLESS(IsRecovery(), action->Error(), "Tablet action failed (ActionId: %v, TabletBalancerCorrelationId: %v)",
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

    void HydraKickOrphanedTabletActions(TReqKickOrphanedTabletActions* request)
    {

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        THashSet<TCellBundle*> healthyBundles;
        for (const auto [bundleId, bundle] : cellManager->CellBundles()) {
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


    void DoMountTablet(
        TTablet* tablet,
        TTabletCell* cell,
        bool freeze)
    {
        auto* table = tablet->GetTable();
        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletChunkReaderConfigPtr readerConfig;
        NTabletNode::TTabletChunkWriterConfigPtr writerConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &readerConfig, &writerConfig, &writerOptions);

        auto serializedMountConfig = ConvertToYsonString(mountConfig);
        auto serializedReaderConfig = ConvertToYsonString(readerConfig);
        auto serializedWriterConfig = ConvertToYsonString(writerConfig);
        auto serializedWriterOptions = ConvertToYsonString(writerOptions);

        auto assignment = ComputeTabletAssignment(
            table,
            mountConfig,
            cell,
            std::vector<TTablet*>{tablet});

        DoMountTablets(
            table,
            assignment,
            mountConfig->InMemoryMode,
            freeze,
            serializedMountConfig,
            serializedReaderConfig,
            serializedWriterConfig,
            serializedWriterOptions);
    }

    void DoMountTablets(
        TTableNode* table,
        const std::vector<std::pair<TTablet*, TTabletCell*>>& assignment,
        EInMemoryMode inMemoryMode,
        bool freeze,
        const TYsonString& serializedMountConfig,
        const TYsonString& serializedReaderConfig,
        const TYsonString& serializedWriterConfig,
        const TYsonString& serializedWriterOptions,
        TTimestamp mountTimestamp = NullTimestamp)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto resourceUsageBefore = table->GetTabletResourceUsage();
        const auto& allTablets = table->Tablets();
        for (const auto& pair : assignment) {
            auto* tablet = pair.first;
            auto* cell = pair.second;

            YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);

            if (!IsCellActive(cell)) {
                DoCreateTabletAction(
                    TObjectId(),
                    ETabletActionKind::Move,
                    ETabletActionState::Orphaned,
                    std::vector<TTablet*>{tablet},
                    std::vector<TTabletCell*>{},
                    std::vector<NTableClient::TOwningKey>{},
                    std::optional<int>(),
                    freeze,
                    false,
                    TGuid{},
                    TInstant::Zero());
                continue;
            }

            int tabletIndex = tablet->GetIndex();
            const auto& chunkLists = table->GetChunkList()->Children();
            YT_VERIFY(allTablets.size() == chunkLists.size());

            tablet->SetCell(cell);
            YT_VERIFY(cell->Tablets().insert(tablet).second);
            objectManager->RefObject(cell);

            tablet->SetState(freeze ? ETabletState::FrozenMounting : ETabletState::Mounting);
            tablet->SetInMemoryMode(inMemoryMode);

            cell->GossipStatistics().Local() += GetTabletStatistics(tablet);

            const auto* context = GetCurrentMutationContext();
            tablet->SetMountRevision(context->GetVersion().ToRevision());
            if (mountTimestamp != NullTimestamp) {
                tablet->NodeStatistics().set_unflushed_timestamp(mountTimestamp);
            }

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
                req.set_mount_config(serializedMountConfig.GetData());
                req.set_reader_config(serializedReaderConfig.GetData());
                req.set_writer_config(serializedWriterConfig.GetData());
                req.set_writer_options(serializedWriterOptions.GetData());
                req.set_atomicity(static_cast<int>(table->GetAtomicity()));
                req.set_commit_ordering(static_cast<int>(table->GetCommitOrdering()));
                req.set_freeze(freeze);
                ToProto(req.mutable_upstream_replica_id(), table->GetUpstreamReplicaId());
                if (table->IsReplicated()) {
                    auto* replicatedTable = table->As<TReplicatedTableNode>();
                    for (auto* replica : GetValuesSortedByKey(replicatedTable->Replicas())) {
                        const auto* replicaInfo = tablet->GetReplicaInfo(replica);
                        PopulateTableReplicaDescriptor(req.add_replicas(), replica, *replicaInfo);
                    }
                }

                auto* chunkList = chunkLists[tabletIndex]->AsChunkList();
                const auto& chunkListStatistics = chunkList->Statistics();
                auto chunksOrViews = EnumerateStoresInChunkTree(chunkList);
                auto storeType = table->IsPhysicallySorted() ? EStoreType::SortedChunk : EStoreType::OrderedChunk;
                i64 startingRowIndex = chunkListStatistics.LogicalRowCount - chunkListStatistics.RowCount;
                for (const auto* chunkOrView : chunksOrViews) {
                    auto* descriptor = req.add_stores();
                    FillStoreDescriptor(chunkOrView, storeType, descriptor, &startingRowIndex);
                }

                for (const auto& pair : table->DynamicTableLocks()) {
                    auto* lock = req.add_locks();
                    lock->mutable_transaction_id();
                    ToProto(lock->mutable_transaction_id(), pair.first);
                    lock->set_timestamp(static_cast<i64>(pair.second.Timestamp));
                }

                if (!freeze && IsDynamicStoreReadEnabled(table)) {
                    CreateAndAttachDynamicStores(tablet, &req);
                }

                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Mounting tablet (TableId: %v, TabletId: %v, CellId: %v, ChunkCount: %v, "
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

        CommitTabletStaticMemoryUpdate(table, resourceUsageBefore, table->GetTabletResourceUsage());
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Freezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Unfreezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
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


    void HydraOnTabletLocked(TRspLockTablet* response)
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

                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Confirmed tablet lock (TabletId: %v, TableId: %v, TransactionId: %v, PendingTabletCount: %v)",
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
        const std::vector<TOwningKey>& pivotKeys)
    {
        if (!pivotKeys.empty() || !table->IsPhysicallySorted()) {
            ReshardTableImpl(
                table,
                firstTabletIndex,
                lastTabletIndex,
                newTabletCount,
                pivotKeys);
            return newTabletCount;
        }

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

    // If there are several otherwise identical chunk views with adjacent read ranges
    // we merge them into one chunk view with the joint range.
    std::vector<TChunkTree*> MergeChunkViewRanges(
        std::vector<NChunkServer::TChunkView*> chunkViews,
        const TOwningKey& lowerPivot,
        const TOwningKey& upperPivot)
    {
        auto mergeResults = MergeAdjacentChunkViewRanges(std::move(chunkViews));
        std::vector<TChunkTree*> result;

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (const auto& mergeResult : mergeResults) {
            auto* firstChunkView = mergeResult.FirstChunkView;
            auto* lastChunkView = mergeResult.LastChunkView;
            const auto& lowerLimit = firstChunkView->ReadRange().LowerLimit().HasKey()
                ? firstChunkView->ReadRange().LowerLimit().GetKey()
                : EmptyKey();
            const auto& upperLimit = lastChunkView->ReadRange().UpperLimit().HasKey()
                ? lastChunkView->ReadRange().UpperLimit().GetKey()
                : MaxKey();

            if (firstChunkView == lastChunkView &&
                lowerPivot <= lowerLimit &&
                upperLimit <= upperPivot)
            {
                result.push_back(firstChunkView);
                continue;
            } else {
                NChunkClient::TReadRange readRange;
                const auto& adjustedLower = std::max(lowerLimit, lowerPivot);
                const auto& adjustedUpper = std::min(upperLimit, upperPivot);
                YT_VERIFY(adjustedLower < adjustedUpper);
                if (adjustedLower != EmptyKey()) {
                    readRange.LowerLimit().SetKey(adjustedLower);
                }
                if (adjustedUpper != MaxKey()) {
                    readRange.UpperLimit().SetKey(adjustedUpper);
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
        const std::vector<TOwningKey>& pivotKeys)
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Resharding table (TableId: %v, FirstTabletIndex: %v, LastTabletIndex: %v, "
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

        std::vector<TOwningKey> oldPivotKeys;

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            if (table->IsPhysicallySorted()) {
                oldPivotKeys.push_back(tablet->GetPivotKey());
            }
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
        table->RecomputeTabletMasterMemoryUsage();

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
            std::vector<TChunkTree*> chunksOrViews;

            const auto& tabletChunkTrees = table->GetChunkList()->Children();

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                std::vector<TChunkTree*> tabletStores;
                EnumerateStoresInChunkTree(tabletChunkTrees[index]->AsChunkList(), &tabletStores);

                const auto& lowerPivot = oldPivotKeys[index - firstTabletIndex];
                const auto& upperPivot = oldPivotKeys[index - firstTabletIndex + 1];

                for (auto* chunkTree : tabletStores) {
                    if (chunkTree->GetType() == EObjectType::ChunkView) {
                        auto* chunkView = chunkTree->AsChunkView();
                        auto readRange = chunkView->GetCompleteReadRange();

                        // Check if chunk view fits into the old tablet completely.
                        // This might not be the case if the chunk view comes from bulk insert and has no read range.
                        if (readRange.LowerLimit().GetKey() < lowerPivot ||
                            upperPivot < readRange.UpperLimit().GetKey())
                        {
                            if (!chunkView->GetTransactionId()) {
                                YT_LOG_ALERT("Chunk view without transaction id is not fully inside its tablet "
                                    "(ChunkViewId: %v, UnderlyingChunkId: %v, "
                                    "EffectiveLowerLimit: %v, EffectiveUpperLimit: %v, "
                                    "PivotKey: %v, NextPivotKey: %v)",
                                    chunkView->GetId(),
                                    chunkView->GetUnderlyingChunk()->GetId(),
                                    readRange.LowerLimit().GetKey(),
                                    readRange.UpperLimit().GetKey(),
                                    lowerPivot,
                                    upperPivot);
                            }

                            NChunkClient::TReadRange newReadRange;
                            if (readRange.LowerLimit().GetKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetKey()) {
                                newReadRange.UpperLimit().SetKey(upperPivot);
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

            for (TChunkTree* chunkOrView : chunksOrViews) {
                NChunkClient::TReadRange readRange;
                if (chunkOrView->GetType() == EObjectType::ChunkView) {
                    readRange = chunkOrView->AsChunkView()->GetCompleteReadRange();
                } else {
                    auto keyPair = GetChunkBoundaryKeys(chunkOrView->AsChunk()->ChunkMeta(), keyColumnCount);
                    readRange = {
                        NChunkClient::TReadLimit(keyPair.first),
                        NChunkClient::TReadLimit(GetKeySuccessor(keyPair.second))
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
                        if (lowerPivot <= readRange.LowerLimit().GetKey() &&
                            readRange.UpperLimit().GetKey() <= upperPivot)
                        {
                            // Chunk fits into the tablet.
                            chunkManager->AttachToChunkList(
                                newTabletChunkTrees[relativeIndex]->AsChunkList(),
                                chunkOrView);
                        } else {
                            // Chunk does not fit into the tablet, create chunk view.
                            NChunkClient::TReadRange newReadRange;
                            if (readRange.LowerLimit().GetKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetKey()) {
                                newReadRange.UpperLimit().SetKey(upperPivot);
                            }
                            auto* newChunkView = chunkManager->CreateChunkView(chunkOrView, newReadRange);
                            chunkManager->AttachToChunkList(
                                newTabletChunkTrees[relativeIndex]->AsChunkList(),
                                newChunkView);
                        }
                    }
                }
            }
            for (int i = 0; i < newTablets.size(); ++i) {
                auto* tablet = newTablets[i];
                const auto& lowerPivot = tablet->GetPivotKey();
                const auto& upperPivot = tablet->GetIndex() == tablets.size() - 1
                    ? MaxKey()
                    : tablets[tablet->GetIndex() + 1]->GetPivotKey();
                std::vector<TChunkTree*> mergedChunkViews;
                try {
                    mergedChunkViews = MergeChunkViewRanges(newTabletChildrenToBeMerged[i], lowerPivot, upperPivot);
                } catch (const std::exception& ex) {
                    YT_LOG_ALERT(ex, "Failed to merge chunk view ranges");
                    mergedChunkViews = {};
                }
                chunkManager->AttachToChunkList(newTabletChunkTrees[i]->AsChunkList(), mergedChunkViews);
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

        // TODO(savrus) Looks like this is unnecessary. Need to check.
        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto resourceUsageDelta = table->GetTabletResourceUsage() - resourceUsageBefore;
        securityManager->UpdateTabletResourceUsage(table, resourceUsageDelta);
        securityManager->UpdateMasterMemoryUsage(table);
        ScheduleTableStatisticsUpdate(table);
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


    const TDynamicTabletManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    void UpdateDynamicConfigAsync()
    {
        const auto& config = GetDynamicConfig();

        if (config->CompatibilityVersion == 1) {
            return;
        }

        auto newConfig = CloneYsonSerializable(config);

        newConfig->ChunkReader->BlockRpcTimeout = TDuration::Seconds(10);
        newConfig->ChunkReader->MinBackoffTime = TDuration::MilliSeconds(50);
        newConfig->ChunkReader->MaxBackoffTime = TDuration::Seconds(1);

        newConfig->ChunkReader->RetryTimeout = TDuration::Seconds(15);
        newConfig->ChunkReader->SessionTimeout = TDuration::Minutes(1);

        newConfig->ChunkReader->GroupSize = 16777216;
        newConfig->ChunkReader->WindowSize = 20971520;

        newConfig->ChunkWriter->BlockSize = 262144;
        newConfig->ChunkWriter->SampleRate = 0.0005;

        newConfig->CellScanPeriod = Config_->CellScanPeriod;
        newConfig->SafeOnlineNodeCount = Config_->SafeOnlineNodeCount;
        newConfig->LeaderReassignmentTimeout = Config_->LeaderReassignmentTimeout;
        newConfig->PeerRevocationTimeout = Config_->PeerRevocationTimeout;

        newConfig->MulticellGossip = Config_->MulticellGossip;

        newConfig->TabletActionManager = Config_->TabletActionManager;

        newConfig->TabletCellDecommissioner = Config_->TabletCellDecommissioner;

        newConfig->TabletBalancer->ConfigCheckPeriod = Config_->TabletBalancer->ConfigCheckPeriod;
        newConfig->TabletBalancer->BalancePeriod = Config_->TabletBalancer->BalancePeriod;

        newConfig->CompatibilityVersion = 1;

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Updating dynamic tablet config (NewConfig: %v)",
            ConvertToYsonString(newConfig, EYsonFormat::Text).GetData());

        auto req = TYPathProxy::Set("//sys/@config/tablet_manager");
        req->set_value(ConvertToYsonString(newConfig).GetData());
        auto rootService = Bootstrap_->GetObjectManager()->GetRootService();
        ExecuteVerb(rootService, req);
    }

    void OnDynamicConfigChanged()
    {
        const auto& config = GetDynamicConfig();

        // COMPAT(savrus)
        if (config->CompatibilityVersion == 0) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster() && IsLeader()) {
                BIND(&TImpl::UpdateDynamicConfigAsync, MakeWeak(this))
                    .Via(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
                    .Run();
            }
            return;
        }

        {
            const auto& gossipConfig = config->MulticellGossip;
            TableStatisticsGossipThrottler_->Reconfigure(gossipConfig->TableStatisticsGossipThrottler);
            if (TabletCellStatisticsGossipExecutor_) {
                TabletCellStatisticsGossipExecutor_->SetPeriod(gossipConfig->TabletCellStatisticsGossipPeriod);
            }
            if (TableStatisticsGossipExecutor_) {
                TableStatisticsGossipExecutor_->SetPeriod(gossipConfig->TableStatisticsGossipPeriod);
            }
            EnableUpdateStatisticsOnHeartbeat_ = gossipConfig->EnableUpdateStatisticsOnHeartbeat;
        }

        TabletCellDecommissioner_->Reconfigure(config->TabletCellDecommissioner);
        TabletActionManager_->Reconfigure(config->TabletActionManager);
        TabletBalancer_->Reconfigure(config->TabletBalancer);
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
        Save(context, TabletStatistisUpdateRequests_);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(savrus)
        if (context.GetVersion() < EMasterReign::CellServer) {
            CompatTabletCellBundleMap_.LoadKeys(context);
            CompatTabletCellMap_.LoadKeys(context);
        }
        TabletMap_.LoadKeys(context);
        TableReplicaMap_.LoadKeys(context);
        TabletActionMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(savrus)
        if (context.GetVersion() < EMasterReign::CellServer) {
            CompatTabletCellBundleMap_.LoadValues(context);
            CompatTabletCellMap_.LoadValues(context);
        }
        TabletMap_.LoadValues(context);
        TableReplicaMap_.LoadValues(context);
        TabletActionMap_.LoadValues(context);
        Load(context, TabletStatistisUpdateRequests_);

        // COMPAT(savrus)
        RecomputeTabletCellStatistics_ = (context.GetVersion() < EMasterReign::CellServer);
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

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

        // COMPAT(savrus)
        if (RecomputeTabletCellStatistics_) {
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

        CompatTabletCellBundleMap_.Clear();
        CompatTabletCellMap_.Clear();
        TabletMap_.Clear();
        TableReplicaMap_.Clear();
        TabletActionMap_.Clear();
        TabletStatistisUpdateRequests_.Clear();

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
        cellBundle = cellManager->CreateCellBundle(name, std::move(holder), std::move(options))
            ->As<TTabletCellBundle>();
        return true;
    }

    void ScheduleTableStatisticsUpdate(
        TTableNode* table,
        bool updateDataStatistics = true,
        bool updateTabletStatistics = true)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Schedule table statistics update (TableId: %v, UpdateDataStatistics: %v, UpdateTabletStatistics: %v)",
                table->GetId(),
                updateDataStatistics,
                updateTabletStatistics);

            auto& statistics = TabletStatistisUpdateRequests_[table->GetId()];
            statistics.UpdateTabletResourceUsage = updateTabletStatistics;
            statistics.UpdateDataStatistics = updateDataStatistics;
            statistics.UpdateModificationTime = true;
            statistics.UpdateAccessTime = true;
        }
    }

    void OnTableStatisticsGossip()
    {
        auto tableCount = TabletStatistisUpdateRequests_.Size();
        tableCount = TableStatisticsGossipThrottler_->TryAcquireAvailable(tableCount);
        if (tableCount == 0) {
            return;
        }

        NProto::TReqSendTableStatisticsUpdates request;
        request.set_table_count(tableCount);
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void SendTableStatisticsUpdates(TTableNode* table)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Sending table statistics update (TableId: %v)",
            table->GetId());

        NProto::TReqUpdateTableStatistics req;
        auto* entry = req.add_entries();
        ToProto(entry->mutable_table_id(), table->GetId());
        ToProto(entry->mutable_data_statistics(), table->SnapshotStatistics());
        ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
        entry->set_modification_time(ToProto<ui64>(table->GetModificationTime()));
        entry->set_access_time(ToProto<ui64>(table->GetAccessTime()));
        multicellManager->PostToMaster(req, table->GetNativeCellTag());

        TabletStatistisUpdateRequests_.Pop(table->GetId());
    }

    void HydraSendTableStatisticsUpdates(NProto::TReqSendTableStatisticsUpdates* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto remainingTableCount = request->table_count();

        std::vector<TTableId> tableIds;
        // NB: Ordered map is needed to make things deterministic.
        std::map<TCellTag, NProto::TReqUpdateTableStatistics> cellTagToRequest;
        while (remainingTableCount-- > 0 && !TabletStatistisUpdateRequests_.IsEmpty()) {
            const auto& [tableId, statistics] = TabletStatistisUpdateRequests_.Pop();

            auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
            if (!IsObjectAlive(node)) {
                continue;
            }
            YT_VERIFY(IsTableType(node->GetType()));
            auto* table = node->As<TTableNode>();

            auto cellTag = CellTagFromId(tableId);
            auto& request = cellTagToRequest[cellTag];
            auto* entry = request.add_entries();
            ToProto(entry->mutable_table_id(), tableId);
            if (statistics.UpdateDataStatistics) {
                ToProto(entry->mutable_data_statistics(), table->SnapshotStatistics());
            }
            if (statistics.UpdateTabletResourceUsage) {
                ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
            }
            if (statistics.UpdateModificationTime) {
                entry->set_modification_time(ToProto<ui64>(table->GetModificationTime()));
            }
            if (statistics.UpdateAccessTime) {
                entry->set_access_time(ToProto<ui64>(table->GetAccessTime()));
            }
            tableIds.push_back(tableId);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Sending table statistics update (RequestedTableCount: %v, TableIds: %v)",
            request->table_count(),
            tableIds);

        for  (const auto& [cellTag, request] : cellTagToRequest) {
            multicellManager->PostToMaster(request, cellTag);
        }
    }

    void HydraUpdateTableStatistics(NProto::TReqUpdateTableStatistics* request)
    {
        std::vector<TTableId> tableIds;
        tableIds.reserve(request->entries_size());
        for (const auto& entry : request->entries()) {
            tableIds.push_back(FromProto<TTableId>(entry.table_id()));
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Received table statistics update (TableIds: %v)",
            tableIds);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& entry : request->entries()) {
            auto tableId = FromProto<TTableId>(entry.table_id());
            auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            YT_VERIFY(IsTableType(node->GetType()));
            auto* table = node->As<TTableNode>();

            if (entry.has_tablet_resource_usage()) {
                table->SetExternalTabletResourceUsage(FromProto<TClusterResources>(entry.tablet_resource_usage()));
            }

            if (entry.has_data_statistics()) {
                YT_VERIFY(table->IsDynamic());
                table->SnapshotStatistics() = entry.data_statistics();
            }

            if (entry.has_modification_time()) {
                table->SetModificationTime(std::max(
                    table->GetModificationTime(),
                    FromProto<TInstant>(entry.modification_time())));
            }

            if (entry.has_access_time()) {
                table->SetAccessTime(std::max(
                    table->GetAccessTime(),
                    FromProto<TInstant>(entry.access_time())));
            }
        }
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
                cell->RecomputeClusterStatistics();
                ToProto(entry->mutable_statistics(), cell->GossipStatistics().Cluster());
            } else {
                ToProto(entry->mutable_statistics(), cell->GossipStatistics().Local());
            }
        }

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }
    }

    void HydraSetTabletCellStatistics(NProto::TReqSetTabletCellStatistics* request)
    {
        auto cellTag = request->cell_tag();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_UNLESS(IsRecovery(), "Received tablet cell statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_UNLESS(IsRecovery(), "Received tablet cell statistics gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto cellId = FromProto<TTabletCellId>(entry.tablet_cell_id());
            auto* cell = FindTabletCell(cellId);
            if (!IsObjectAlive(cell))
                continue;

            auto newStatistics = FromProto<NTabletServer::TTabletCellStatistics>(entry.statistics());

            if (multicellManager->IsPrimaryMaster()) {
                *cell->GossipStatistics().Remote(cellTag) = newStatistics;
            } else {
                cell->GossipStatistics().Cluster() = newStatistics;
            }
        }
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        // Copy tablet statistics, update performance counters and table replica statistics.
        auto now = TInstant::Now();

        for (auto& tabletInfo : request->tablets()) {
            auto tabletId = FromProto<TTabletId>(tabletInfo.tablet_id());
            auto* tablet = FindTablet(tabletId);

            if (!IsObjectAlive(tablet) || tablet->GetState() == ETabletState::Unmounted) {
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

            cell->GossipStatistics().Local() -= GetTabletStatistics(tablet);
            tablet->NodeStatistics() = tabletInfo.statistics();
            cell->GossipStatistics().Local() += GetTabletStatistics(tablet);

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
                    ScheduleTableStatisticsUpdate(table, true, false);
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

        TabletServerProfiler.Increment(IncrementalHeartbeatCounter, timer.GetElapsedValue());
    }

    void HydraUpdateUpstreamTabletState(TReqUpdateUpstreamTabletState* request)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        auto transactionId = FromProto<TTransactionId>(request->last_mount_transaction_id());
        auto actualState = request->has_actual_tablet_state()
            ? std::make_optional(static_cast<ETabletState>(request->actual_tablet_state()))
            : std::nullopt;
        auto expectedState = request->has_expected_tablet_state()
            ? std::make_optional(static_cast<ETabletState>(request->expected_tablet_state()))
            : std::nullopt;

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
        if (!IsObjectAlive(node)) {
            return;
        }

        YT_VERIFY(IsTableType(node->GetType()));
        auto* table = node->As<TTableNode>();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Received update upstream tablet state request "
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

    void HydraUpdateTabletState(TReqUpdateTabletState* request)
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table tablet state check request received (TableId: %v, LastMountTransactionId %v, PrimaryLastMountTransactionId %v)",
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

            TReqUpdateTabletState request;
            ToProto(request.mutable_table_id(), table->GetId());
            ToProto(request.mutable_last_mount_transaction_id(), table->GetLastMountTransactionId());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(request, table->GetExternalCellTag());

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table tablet state check requested (TableId: %v, LastMountTransactionId %v)",
                table->GetId(),
                table->GetLastMountTransactionId());
            return;
        }

        // TODO(savrus): Remove this after testing multicell on real cluster is done.
        YT_LOG_DEBUG("Table tablet state check started (TableId: %v, LastMountTransactionId: %v, PrimaryLastMountTransactionId: %v, TabletCountByState: %v, TabletCountByExpectedState: %v)",
            table->GetId(),
            table->GetLastMountTransactionId(),
            table->GetPrimaryLastMountTransactionId(),
            ConvertToYsonString(table->TabletCountByState(), EYsonFormat::Text).GetData(),
            ConvertToYsonString(table->TabletCountByExpectedState(), EYsonFormat::Text).GetData());


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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table tablet state updated "
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
            SendTableStatisticsUpdates(table);

            TReqUpdateUpstreamTabletState request;
            ToProto(request.mutable_table_id(), table->GetId());
            request.set_actual_tablet_state(static_cast<i32>(actualState));
            if (clearLastMountTransactionId) {
                ToProto(request.mutable_last_mount_transaction_id(), table->GetLastMountTransactionId());
            }
            if (expectedState) {
                request.set_expected_tablet_state(static_cast<i32>(*expectedState));
            }

            multicellManager->PostToMaster(request, table->GetNativeCellTag());

            if (clearLastMountTransactionId) {
                table->SetLastMountTransactionId(TTransactionId());
                table->SetPrimaryLastMountTransactionId(TTransactionId());
            }
        }
    }

    void HydraOnTabletMounted(TRspMountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounting && state != ETabletState::FrozenMounting) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Mounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        bool frozen = response->frozen();
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        // FIXME(savrus) Remove this.
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet mounted (TableId: %v, TabletId: %v, MountRevision: %llx, CellId: %v, Frozen: %v)",
            table->GetId(),
            tablet->GetId(),
            tablet->GetMountRevision(),
            cell->GetId(),
            frozen);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet static memory usage (TableId: %v, TabletMemorySize: %v, TabletStaticUsage: %v, ExternalTabletResourceUsage: %v)",
            table->GetId(),
            tablet->GetTabletStaticMemorySize(),
            table->GetTabletResourceUsage().TabletStaticMemory,
            table->GetExternalTabletResourceUsage().TabletStaticMemory);

        tablet->SetState(frozen ? ETabletState::Frozen : ETabletState::Mounted);

        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnmounted(TRspUnmountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Unmounting) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Unmounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        DiscardDynamicStores(tablet);
        DoTabletUnmounted(tablet);
        OnTabletActionStateChanged(tablet->GetAction());
    }

    void HydraOnTabletFrozen(TRspFreezeTablet* response)
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
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Frozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        DiscardDynamicStores(tablet);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet frozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Frozen);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraOnTabletUnfrozen(TRspUnfreezeTablet* response)
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
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Unfrozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet unfrozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Mounted);
        OnTabletActionStateChanged(tablet->GetAction());
        UpdateTabletState(table);
    }

    void HydraUpdateTableReplicaStatistics(TReqUpdateTableReplicaStatistics* request)
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica statistics updated (TabletId: %v, ReplicaId: %v, "
            "CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tabletId,
            replicaId,
            replicaInfo->GetCurrentReplicationRowIndex(),
            replicaInfo->GetCurrentReplicationTimestamp());
    }

    void HydraOnTableReplicaEnabled(TRspEnableTableReplica* response)
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
            YT_LOG_WARNING_UNLESS(IsRecovery(), "Enabled replica notification received for a replica in a wrong state, "
                "ignored (TabletId: %v, ReplicaId: %v, State: %v)",
                tabletId,
                replicaId,
                replicaInfo->GetState());
            return;
        }

        StopReplicaTransition(tablet, replica, replicaInfo, ETableReplicaState::Enabled);
        CheckTransitioningReplicaTablets(replica);
    }

    void HydraOnTableReplicaDisabled(TRspDisableTableReplica* response)
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
            YT_LOG_WARNING_UNLESS(IsRecovery(), "Disabled replica notification received for a replica in a wrong state, "
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
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica is now transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica is no longer transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica enabled (TableId: %v, ReplicaId: %v)",
                    table->GetId(),
                    replica->GetId());
                replica->SetState(ETableReplicaState::Enabled);
                break;

            case ETableReplicaState::Disabling:
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica disabled (TableId: %v, ReplicaId: %v)",
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

        DetachChunksFromTablet(tablet->GetChunkList(), dynamicStores);

        auto* table = tablet->GetTable();
        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();
        ScheduleTableStatisticsUpdate(table, /*updateDataStatistics*/ true, /*updateTabletStatistics*/ false);

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = -static_cast<int>(dynamicStores.size());
        tablet->GetCell()->GossipStatistics().Local() += statisticsDelta;
    }

    void DoTabletUnmounted(TTablet* tablet)
    {
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet unmounted (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        cell->GossipStatistics().Local() -= GetTabletStatistics(tablet);
        auto resourceUsageBefore = table->GetTabletResourceUsage();

        tablet->NodeStatistics().Clear();
        tablet->PerformanceCounters() = TTabletPerformanceCounters();
        tablet->SetInMemoryMode(EInMemoryMode::None);
        tablet->SetState(ETabletState::Unmounted);
        tablet->SetCell(nullptr);
        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        CommitTabletStaticMemoryUpdate(table, resourceUsageBefore, table->GetTabletResourceUsage());
        UpdateTabletState(table);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        YT_VERIFY(cell->Tablets().erase(tablet) == 1);
        objectManager->UnrefObject(cell);

        for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
            auto* replica = it->first;
            auto& replicaInfo = it->second;
            if (replica->TransitioningTablets().erase(tablet) == 1) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Table replica is still transitioning (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v)",
                    tablet->GetTable()->GetId(),
                    tablet->GetId(),
                    replica->GetId(),
                    replicaInfo.GetState());
            } else {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Table replica state updated (TableId: %v, TabletId: %v, ReplicaId: %v, State: %v -> %v)",
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
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Dynamic store attached to tablet (TabletId: %v, DynamicStoreId: %v)",
            tablet->GetId(),
            dynamicStore->GetId());

        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();
        ScheduleTableStatisticsUpdate(table, /*updateDataStatistics*/ true, /*updateTabletStatistics*/ false);

        TTabletStatistics statisticsDelta;
        statisticsDelta.ChunkCount = 1;
        tablet->GetCell()->GossipStatistics().Local() += statisticsDelta;
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
        TCumulativeServiceProfilerGuard profilerGuard(&Profiler, "/copy_chunk_list_if_shared");
        TTagIdList tagIds;

        // NB: When the table is resharded during creation it has no tablet cell bundle.
        if (IsRecovery() || !table->GetTabletCellBundle()) {
            profilerGuard.Disable();
        } else {
            tagIds = TTagIdList{
                table->GetTabletCellBundle()->GetProfilingTag(),
                table->IsPhysicallySorted()
                    ? SortedTableProfilerTag_
                    : OrderedTableProfilerTag_
            };
            profilerGuard.SetProfilerTags(tagIds);
        }

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
                YT_LOG_ALERT_UNLESS(IsRecovery(),
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
                YT_LOG_ALERT_UNLESS(IsRecovery(),
                    "Invalid old root chunk list statistics "
                    "(TableId: %v, OldRootChunkListStatistics: %v, Statistics: %v)",
                    table->GetId(),
                    oldRootChunkList->Statistics(),
                    statistics);
            }
        }

        if (actionCount > 0 && !IsRecovery()) {
            auto& counters = GetLocallyGloballyCachedValue<TTabletManagerProfilerTrait>(tagIds);
            Profiler.Increment(
                counters.CopyChunkListIfSharedActionCount,
                actionCount);
        }
    }

    void HydraPrepareUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        if (tablet->GetStoresUpdatePreparedTransaction()) {
            THROW_ERROR_EXCEPTION("Stores update for tablet %v is already prepared by transaction %v",
                tabletId,
                tablet->GetStoresUpdatePreparedTransaction()->GetId());
        }

        auto validateStoreType = [&] (TObjectId id, TStringBuf action) {
            auto type = TypeFromId(id);
            if (type != EObjectType::Chunk &&
                type != EObjectType::ErasureChunk &&
                type != EObjectType::ChunkView &&
                type != EObjectType::SortedDynamicTabletStore &&
                type != EObjectType::OrderedDynamicTabletStore)
            {
                THROW_ERROR_EXCEPTION("Cannot %v store %v of type %Qlv",
                    action,
                    id,
                    type);
            }
        };

        for (auto id : request->stores_to_add()) {
            validateStoreType(FromProto<TObjectId>(id.store_id()), "attach");
        }

        for (auto id : request->stores_to_remove()) {
            validateStoreType(FromProto<TObjectId>(id.store_id()), "detach");
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

            if (request->stores_to_remove_size() > 0) {
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
        }

        tablet->SetStoresUpdatePreparedTransaction(transaction);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet stores update prepared (TransactionId: %v, TableId: %v, TabletId: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId);
    }

    bool CanUnambiguouslyDetachChunk(TChunkList* tabletChunkList, const TChunkTree* child)
    {
        while (GetParentCount(child) == 1) {
            auto* parent = GetUniqueParent(child);
            if (parent == tabletChunkList) {
                return true;
            }
            if (parent->GetObjectRefCounter() > 1) {
                return false;
            }
            child = parent;
        }

        return HasParent(child, tabletChunkList);
    }

    void DetachChunksFromTablet(TChunkList* tabletChunkList, const std::vector<TChunkTree*>& chunksOrViews)
    {
        THashMap<TChunkListId, std::vector<TChunkTree*>> childrenByParent;

        for (auto* child : chunksOrViews) {
            int parentCount = GetParentCount(child);
            if (parentCount == 1) {
                auto* parent = GetUniqueParent(child);
                YT_VERIFY(parent->GetType() == EObjectType::ChunkList);
                childrenByParent[parent->GetId()].push_back(child);
            } else {
                auto foundParent = HasParent(child, tabletChunkList);
                YT_VERIFY(foundParent);
                childrenByParent[tabletChunkList->GetId()].push_back(child);
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

        for (const auto& [parentId, children] : childrenByParent) {
            auto* parent = chunkManager->GetChunkList(parentId);
            chunkManager->DetachFromChunkList(parent, children);
            pruneEmptyChunkList(parent);
        }
    }

    void HydraCommitUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        TCumulativeServiceProfilerGuard profilerGuard(&Profiler, "/update_tablet_stores");
        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());
        auto tagIds = TTagIdList{
            tablet->GetTable()->GetTabletCellBundle()->GetProfilingTag(),
            UpdateTabletStoresProfilerTags_[updateReason],
            tablet->GetTable()->IsPhysicallySorted()
                ? SortedTableProfilerTag_
                : OrderedTableProfilerTag_
        };
        profilerGuard.SetProfilerTags(tagIds);
        if (IsRecovery()) {
            profilerGuard.Disable();
        }

        if (tablet->GetStoresUpdatePreparedTransaction() != transaction) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet stores update commit for an improperly prepared tablet; ignored "
                "(TabletId: %v, ExpectedTransactionId: %v, ActualTransactionId: %v)",
                tabletId,
                transaction->GetId(),
                GetObjectId(tablet->GetStoresUpdatePreparedTransaction()));
            return;
        }

        tablet->SetStoresUpdatePreparedTransaction(nullptr);

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Invalid mount revision on tablet stores update commit; ignored "
                "(TabletId: %v, TransactionId: %v, ExpectedMountRevision: %v, ActualMountRevision: %v)",
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
        std::vector<TChunkTree*> chunksToAttach;
        i64 attachedRowCount = 0;
        auto lastCommitTimestamp = table->GetLastCommitTimestamp();

        TChunk* flushedChunk = nullptr;

        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto type = TypeFromId(storeId);
            YT_VERIFY(type != EObjectType::ChunkView);
            if (IsChunkTabletStoreType(type)) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                if (chunk->HasParents()) {
                    THROW_ERROR_EXCEPTION("Chunk %v cannot be attached since it already has a parent",
                        chunk->GetId());
                }
                const auto& miscExt = chunk->MiscExt();
                if (miscExt.has_max_timestamp()) {
                    lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt.max_timestamp()));
                }
                attachedRowCount += miscExt.row_count();
                chunksToAttach.push_back(chunk);
            } else if (IsDynamicTabletStoreType(type)) {
                if (IsDynamicStoreReadEnabled(table)) {
                    YT_LOG_ALERT_UNLESS(IsRecovery(), "Attempting to add dynamic store in UpdateTabletStores to a table "
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

        if (updateReason == ETabletStoresUpdateReason::Flush) {
            YT_VERIFY(chunksToAttach.size() <= 1);
            if (chunksToAttach.size() == 1) {
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
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Dynamic store attached to tablet during flush (TableId: %v, TabletId: %v, StoreId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    storeId);
            }
        }

        std::vector<TChunkTree*> chunksToDetach;
        i64 detachedRowCount = 0;
        bool flatteningRequired = false;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (IsChunkTabletStoreType(TypeFromId(storeId))) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                const auto& miscExt = chunk->MiscExt();
                detachedRowCount += miscExt.row_count();
                chunksToDetach.push_back(chunk);
                flatteningRequired = flatteningRequired ||
                    !CanUnambiguouslyDetachChunk(tablet->GetChunkList(), chunk);
            } else if (TypeFromId(storeId) == EObjectType::ChunkView) {
                auto* chunkView = chunkManager->GetChunkViewOrThrow(storeId);
                auto* chunk = chunkView->GetUnderlyingChunk();
                const auto& miscExt = chunk->MiscExt();
                detachedRowCount += miscExt.row_count();
                chunksToDetach.push_back(chunkView);
                flatteningRequired = flatteningRequired ||
                    !CanUnambiguouslyDetachChunk(tablet->GetChunkList(), chunkView);
            } else if (IsDynamicTabletStoreType(TypeFromId(storeId))) {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                auto* dynamicStore = chunkManager->FindDynamicStore(storeId);
                if (dynamicStore) {
                    YT_VERIFY(updateReason == ETabletStoresUpdateReason::Flush);
                    dynamicStore->SetFlushedChunk(flushedChunk);
                    chunksToDetach.push_back(dynamicStore);
                }
            } else {
                YT_ABORT();
            }
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
        chunkManager->AttachToChunkList(tabletChunkList, chunksToAttach);
        DetachChunksFromTablet(tabletChunkList, chunksToDetach);
        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();

        // Get new tablet resource usage.
        auto newMemorySize = tablet->GetTabletStaticMemorySize();
        auto newStatistics = GetTabletStatistics(tablet);
        auto deltaStatistics = newStatistics - oldStatistics;

        // Update cell statistics.
        cell->GossipStatistics().Local() += deltaStatistics;

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
        for (auto* chunk : chunksToDetach) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }

        if (tablet->GetStoresUpdatePreparedTransaction() == transaction) {
            tablet->SetStoresUpdatePreparedTransaction(nullptr);
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto resourceUsageDelta = TClusterResources()
            .SetTabletCount(0)
            .SetTabletStaticMemory(newMemorySize - oldMemorySize);
        securityManager->UpdateTabletResourceUsage(table, resourceUsageDelta);
        ScheduleTableStatisticsUpdate(table);

        if (!IsRecovery()) {
            int storeCount = chunksToAttach.size() + chunksToDetach.size();
            auto& counters = GetLocallyGloballyCachedValue<TTabletManagerProfilerTrait>(tagIds);
            Profiler.Increment(
                counters.UpdateTabletStoresStoreCount,
                storeCount);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet stores update committed (TransactionId: %v, TableId: %v, TabletId: %v, "
            "AttachedChunkIds: %v, DetachedChunkOrViewIds: %v, "
            "AttachedRowCount: %v, DetachedRowCount: %v, RetainedTimestamp: %llx, UpdateReason: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId,
            MakeFormattableView(chunksToAttach, TObjectIdFormatter()),
            MakeFormattableView(chunksToDetach, TObjectIdFormatter()),
            attachedRowCount,
            detachedRowCount,
            retainedTimestamp,
            updateReason);
    }

    void HydraAbortUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet stores update aborted (TransactionId: %v, TableId: %v, TabletId: %v)",
            transaction->GetId(),
            table->GetId(),
            tabletId);
    }

    void HydraUpdateTabletTrimmedRowCount(TReqUpdateTabletTrimmedRowCount* request)
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

        auto trimmedRowCount = request->trimmed_row_count();

        tablet->SetTrimmedRowCount(trimmedRowCount);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet trimmed row count updated (TabletId: %v, TrimmedRowCount: %v)",
            tabletId,
            trimmedRowCount);
    }

    void HydraAllocateDynamicStore(TReqAllocateDynamicStore* request)
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Dynamic store allocated (StoreId: %v, TabletId: %v, TableId: %v)",
            dynamicStore->GetId(),
            tabletId,
            tablet->GetTable()->GetId());

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(tablet->GetCell()->GetId());
        hiveManager->PostMessage(mailbox, rsp);
    }

    void HydraCreateTabletAction(TReqCreateTabletAction* request)
    {
        auto kind = ETabletActionKind(request->kind());
        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());
        auto cellIds = FromProto<std::vector<TTabletId>>(request->cell_ids());
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), TError(ex), "Error creating tablet action (Kind: %v, Tablets: %v, TabletCellsL %v, PivotKeys %v, TabletCount %v, TabletBalancerCorrelationId: %v)",
                kind,
                tablets,
                cells,
                pivotKeys,
                tabletCount,
                correlationId,
                TError(ex));
        }
    }

    void HydraDestroyTabletActions(TReqDestroyTabletActions* request)
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

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            TableStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
                BIND(&TImpl::OnTableStatisticsGossip, MakeWeak(this)),
                dynamicConfig->MulticellGossip->TableStatisticsGossipPeriod);
            TableStatisticsGossipExecutor_->Start();
        }
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

        if (TableStatisticsGossipExecutor_) {
            TableStatisticsGossipExecutor_->Stop();
        }
    }


    bool CheckHasHealthyCells(TTabletCellBundle* bundle)
    {
        for (auto* cellBase: bundle->Cells()) {
            if (cellBase->GetType() != EObjectType::TabletCell) {
                continue;
            }

            auto* cell = cellBase->As<TTabletCell>();
            if (!IsCellActive(cell)) {
                continue;
            }
            if (cell->GetHealth() == ETabletCellHealth::Good) {
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Unmounting tablet (TableId: %v, TabletId: %v, CellId: %v, Force: %v)",
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
            if (!onDestroy) {
                DiscardDynamicStores(tablet);
            }
            DoTabletUnmounted(tablet);
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

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidateResourceUsageIncrease(
            table->GetAccount(),
            TClusterResources().SetTabletStaticMemory(memorySize));
    }

    void CommitTabletStaticMemoryUpdate(
        TTableNode* table,
        const TClusterResources& resourceUsageBefore,
        const TClusterResources& resourceUsageAfter)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateTabletResourceUsage(table, resourceUsageAfter - resourceUsageBefore);
        ScheduleTableStatisticsUpdate(table);
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

        if (!table->IsSorted()) {
            return false;
        }

        if (table->GetActualTabletState() == ETabletState::Unmounted) {
            return table->GetEnableDynamicStoreRead().value_or(
                GetDynamicConfig()->EnableDynamicStoreReadByDefault);
        } else {
            return table->GetMountedWithEnabledDynamicStoreRead();
        }
    }

    void GetTableSettings(
        TTableNode* table,
        TTableMountConfigPtr* mountConfig,
        NTabletNode::TTabletChunkReaderConfigPtr* readerConfig,
        NTabletNode::TTabletChunkWriterConfigPtr* writerConfig,
        TTableWriterOptionsPtr* writerOptions)
    {
        const auto& dynamicConfig = GetDynamicConfig();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        const auto& tableAttributes = tableProxy->Attributes();

        // Parse and prepare mount config.
        try {
            *mountConfig = ConvertTo<TTableMountConfigPtr>(tableAttributes);
            // TODO(akozhikhov): avoid explicit string, when ProfilingMode attribute will be interned.
            if (!tableAttributes.Contains("profiling_mode")) {
                (*mountConfig)->ProfilingMode = dynamicConfig->DynamicTableProfilingMode;
            }
            (*mountConfig)->EnableDynamicStoreRead = IsDynamicStoreReadEnabled(table);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing table mount configuration")
                << ex;
        }

        // Parse and prepare table reader config.
        try {
            *readerConfig = UpdateYsonSerializable(
                GetDynamicConfig()->ChunkReader,
                tableAttributes.FindYson(EInternedAttributeKey::ChunkReader.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing chunk reader config")
                << ex;
        }

        // Prepare tablet writer options.
        const auto& chunkReplication = table->Replication();
        auto primaryMediumIndex = table->GetPrimaryMediumIndex();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);
        *writerOptions = New<TTableWriterOptions>();
        (*writerOptions)->ReplicationFactor = chunkReplication.Get(primaryMediumIndex).GetReplicationFactor();
        (*writerOptions)->MediumName = primaryMedium->GetName();
        (*writerOptions)->Account = table->GetAccount()->GetName();
        (*writerOptions)->CompressionCodec = table->GetCompressionCodec();
        (*writerOptions)->ErasureCodec = table->GetErasureCodec();
        (*writerOptions)->ChunksVital = chunkReplication.GetVital();
        (*writerOptions)->OptimizeFor = table->GetOptimizeFor();

        // Parse and prepare table writer config.
        try {
            auto config = CloneYsonSerializable(GetDynamicConfig()->ChunkWriter);
            config->PreferLocalHost = primaryMedium->Config()->PreferLocalHostForDynamicTables;

            *writerConfig = UpdateYsonSerializable(
                config,
                tableAttributes.FindYson(EInternedAttributeKey::ChunkWriter.Unintern()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing chunk writer config")
                << ex;
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
        const NChunkClient::TReadRange readRange)
    {
        YT_VERIFY(readRange.LowerLimit().HasKey());
        YT_VERIFY(readRange.UpperLimit().HasKey());
        const auto& minKey = readRange.LowerLimit().GetKey();
        const auto& maxKey = readRange.UpperLimit().GetKey();

        auto beginIt = std::upper_bound(
            tablets.begin(),
            tablets.end(),
            minKey,
            [] (const TOwningKey& key, const TTablet* tablet) {
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

    void FillStoreDescriptor(
        const TChunkTree* chunkOrView,
        EStoreType storeType,
        NTabletNode::NProto::TAddStoreDescriptor* descriptor,
        i64* startingRowIndex)
    {
        descriptor->set_store_type(static_cast<int>(storeType));
        ToProto(descriptor->mutable_store_id(), chunkOrView->GetId());

        const TChunk* chunk;
        if (chunkOrView->GetType() == EObjectType::ChunkView) {
            auto* chunkView = chunkOrView->AsChunkView();
            chunk = chunkView->GetUnderlyingChunk();
            auto* viewDescriptor = descriptor->mutable_chunk_view_descriptor();
            ToProto(viewDescriptor->mutable_chunk_view_id(), chunkView->GetId());
            ToProto(viewDescriptor->mutable_underlying_chunk_id(), chunk->GetId());
            ToProto(viewDescriptor->mutable_read_range(), chunkView->ReadRange());

            auto& transactionManager = Bootstrap_->GetTransactionManager();
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

    void FillProfilerTags()
    {
        const auto& profilerManager = NProfiling::TProfileManager::Get();
        SortedTableProfilerTag_ = profilerManager->RegisterTag("table_type", "sorted");
        OrderedTableProfilerTag_ = profilerManager->RegisterTag("table_type", "ordered");

        using TTraits = TEnumTraits<ETabletStoresUpdateReason>;
        for (int index = 0; index < TTraits::DomainSize; ++index) {
            auto value = TTraits::GetDomainValues()[index];
            auto name = TTraits::GetDomainNames()[index];
            UpdateTabletStoresProfilerTags_[value] = profilerManager->RegisterTag("update_reason", name);
        }
    }

    static void PopulateTableReplicaDescriptor(TTableReplicaDescriptor* descriptor, const TTableReplica* replica, const TTableReplicaInfo& info)
    {
        ToProto(descriptor->mutable_replica_id(), replica->GetId());
        descriptor->set_cluster_name(replica->GetClusterName());
        descriptor->set_replica_path(replica->GetReplicaPath());
        descriptor->set_start_replication_timestamp(replica->GetStartReplicationTimestamp());
        descriptor->set_mode(static_cast<int>(replica->GetMode()));
        descriptor->set_preserve_timestamps(replica->GetPreserveTimestamps());
        descriptor->set_atomicity(static_cast<int>(replica->GetAtomicity()));
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

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
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
    bool freeze,
    TTimestamp mountTimestamp)
{
    Impl_->PrepareMountTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        hintCellId,
        targetCellIds,
        freeze,
        mountTimestamp);
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
    const std::vector<TOwningKey>& pivotKeys,
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
    const std::vector<TOwningKey>& pivotKeys)
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

TTabletCellBundle* TTabletManager::GetTabletCellBundleByNameOrThrow(const TString& name)
{
    return Impl_->GetTabletCellBundleByNameOrThrow(name);
}

TTabletCellBundle* TTabletManager::GetDefaultTabletCellBundle()
{
    return Impl_->GetDefaultTabletCellBundle();
}

void TTabletManager::SetTabletCellBundle(TTableNode* table, TTabletCellBundle* cellBundle)
{
    Impl_->SetTabletCellBundle(table, cellBundle);
}

void TTabletManager::SetTabletCellBundle(TCompositeNodeBase* node, TTabletCellBundle* cellBundle)
{
    Impl_->SetTabletCellBundle(node, cellBundle);
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
    const std::vector<NTableClient::TOwningKey>& pivotKeys,
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

void TTabletManager::SendTableStatisticsUpdates(TChunkOwnerBase* chunkOwner)
{
    Impl_->SendTableStatisticsUpdates(chunkOwner);
}

TEntityMap<TTabletCellBundle>& TTabletManager::CompatTabletCellBundleMap()
{
    return Impl_->CompatTabletCellBundleMap();
}

TEntityMap<TTabletCell>& TTabletManager::CompatTabletCellMap()
{
    return Impl_->CompatTabletCellMap();
}


DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TableReplica, TTableReplica, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletAction, TTabletAction, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
