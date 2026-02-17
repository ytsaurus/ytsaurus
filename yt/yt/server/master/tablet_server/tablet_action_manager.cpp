#include "tablet_action_manager.h"

#include "config.h"
#include "helpers.h"
#include "private.h"
#include "public.h"
#include "table_settings.h"
#include "tablet_action.h"
#include "tablet_action_type_handler.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_cell.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NTableClient;
using namespace NTableServer;
using namespace NHydra;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTabletServer::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletActionManager
    : public ITabletActionManager
    , public TMasterAutomatonPart
{
public:
    explicit TTabletActionManager(
        NCellMaster::TBootstrap* bootstrap,
        ITabletActionManagerHostPtr host,
        ISimpleHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker)
        : TMasterAutomatonPart(
            bootstrap,
            NCellMaster::EAutomatonThreadQueue::TabletManager)
        , Host_(std::move(host))
        , HydraManager_(std::move(hydraManager))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , CleanupExecutor_(New<TPeriodicExecutor>(
            AutomatonInvoker_,
            BIND(&TTabletActionManager::RunCleanup, MakeWeak(this))))
    {
        RegisterLoader(
            "TabletActionManager.Keys",
            BIND_NO_PROPAGATE(&TTabletActionManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletActionManager.Values",
            BIND_NO_PROPAGATE(&TTabletActionManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletActionManager.Keys",
            BIND_NO_PROPAGATE(&TTabletActionManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletActionManager.Values",
            BIND_NO_PROPAGATE(&TTabletActionManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TTabletActionManager::HydraDestroyTabletActions, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletActionManager::HydraCreateTabletAction, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletActionManager::HydraKickOrphanedTabletActions, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateTabletActionTypeHandler(Bootstrap_, &TabletActionMap_));
    }

    void Start() override
    {
        DoReconfigure();
        CleanupExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(CleanupExecutor_->Stop());
    }

    void Reconfigure(TTabletActionManagerMasterConfigPtr config) override
    {
        Config_ = std::move(config);
        DoReconfigure();
    }

    void OnAfterCellManagerSnapshotLoaded() override
    {
        for (auto [actionId, action] : TabletActionMap_) {
            // COMPAT(ifsmirnov): EMasterReign::ZombifyTabletAction
            if (!IsObjectAlive(action)) {
                // Unbinding was earlier performed in Destroy instead of Zombify.
                // We take care of actions which had zero RC during the update
                // so neither handler was executed. Note that while UnbindTabletAction is
                // idempotent, ZombifyTabletActions() also operates with bundle state
                // which is not yet initialized, so we cannot call the method as-is.
                UnbindTabletAction(action);
                action->SetTabletCellBundle(nullptr);

                // NB: This is not a part of the compat and should be kept during cleanup.
                continue;
            }

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

    TTabletAction* CreateTabletAction(
        TObjectId hintId,
        ETabletActionKind kind,
        std::vector<TTabletBaseRawPtr> tablets,
        std::vector<TTabletCellRawPtr> cells,
        std::vector<NTableClient::TLegacyOwningKey> pivotKeys,
        const std::optional<int>& tabletCount,
        TCreateTabletActionOptions options) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

        THashSet<TTabletId> tabletIds;
        for (auto tablet : tablets) {
            if (tablet->GetOwner() != table) {
                THROW_ERROR_EXCEPTION("Tablets %v and %v belong to different tables",
                    tablets[0]->GetId(),
                    tablet->GetId());
            }
            if (auto action = tablet->GetAction()) {
                THROW_ERROR_EXCEPTION("Tablet %v already participating in action %v",
                    tablet->GetId(),
                    action->GetId());
            }
            if (tablet->GetState() != ETabletState::Mounted && tablet->GetState() != ETabletState::Frozen) {
                THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::TabletIsInIntermediateState,
                    "Tablet %v is in state %Qlv",
                    tablet->GetId(),
                    tablet->GetState());
            }
            if (tablet->GetTabletwiseAvenueEndpointId()) {
                THROW_ERROR_EXCEPTION("Tablet %v is recovering from smooth movement",
                    tablet->GetId());
            }
            if (auto [it, inserted] = tabletIds.emplace(tablet->GetId()); !inserted) {
                THROW_ERROR_EXCEPTION("Tablet %v cannot participate in one action twice",
                    tablet->GetId());
            }
        }

        bool freeze;
        {
            auto state = tablets[0]->GetState();
            for (auto tablet : tablets) {
                if (tablet->GetState() != state) {
                    THROW_ERROR_EXCEPTION("Tablets are in mixed state");
                }
            }
            freeze = state == ETabletState::Frozen;
        }

        const auto& bundle = table->TabletCellBundle();

        for (auto cell : cells) {
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

        Host_->ValidateBundleUsePermission(bundle.Get());

        switch (kind) {
            case ETabletActionKind::Move:
            case ETabletActionKind::SmoothMove:
                if (!cells.empty() && cells.size() != tablets.size()) {
                    THROW_ERROR_EXCEPTION("Number of destination cells and tablets mismatch: %v tablets, %v cells",
                        tablets.size(),
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
                if (options.InplaceReshard) {
                    THROW_ERROR_EXCEPTION("\"inplace_reshard\" can not be set together with move action");
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
                            THROW_ERROR_EXCEPTION("Number of destination cells and pivot keys mismatch: pivot keys %v, cells %v",
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

                    if (options.InplaceReshard && prev->GetCell() != cur->GetCell()) {
                        THROW_ERROR_EXCEPTION("All tablets must belong to the same cell");
                    }
                }

                if (options.InplaceReshard) {
                    auto cell = tablets[0]->GetCell();
                    THROW_ERROR_EXCEPTION_IF(!cells.empty(), "Destination cells cannot be specified with inplace reshard");

                    cells.assign(tabletCount.value_or(pivotKeys.size()), cell);
                }
                break;

            default:
                YT_ABORT();
        }

        auto tableSettings = Host_->GetTableSettings(table);
        ValidateTableMountConfig(
            table,
            tableSettings.EffectiveMountConfig,
            Host_->GetDynamicConfig());

        if (kind == ETabletActionKind::SmoothMove) {
            if (!Host_->GetDynamicConfig()->EnableSmoothTabletMovement) {
                THROW_ERROR_EXCEPTION("Smooth tablet movement is disabled in config");
            }

            if (tablets.size() != 1) {
                THROW_ERROR_EXCEPTION("Only one tablet can be moved at a time");
            }

            if (cells.size() != 1) {
                THROW_ERROR_EXCEPTION("Destination cell must be specified");
            }

            auto tablet = tablets[0];

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
        }

        auto* action = DoCreateTabletAction(
            hintId,
            kind,
            ETabletActionState::Preparing,
            std::move(tablets),
            std::move(cells),
            std::move(pivotKeys),
            tabletCount,
            freeze,
            std::move(options));

        OnTabletActionStateChanged(action);
        return action;
    }

    void CreateOrphanedTabletAction(TTabletBase* tablet, bool freeze) override
    {
        YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

        DoCreateTabletAction(
            TObjectId(),
            ETabletActionKind::Move,
            ETabletActionState::Orphaned,
            std::vector<TTabletBaseRawPtr>{tablet},
            std::vector<TTabletCellRawPtr>{},
            std::vector<NTableClient::TLegacyOwningKey>{},
            /*tabletCount*/ std::nullopt,
            freeze,
            /*options*/ {});
    }

    void ZombifyTabletAction(TTabletAction* action) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        UnbindTabletAction(action);
        if (auto bundle = action->GetTabletCellBundle()) {
            bundle->TabletActions().erase(action);
            if (!action->IsFinished()) {
                bundle->DecreaseActiveTabletActionCount();
            }
            action->SetTabletCellBundle(nullptr);
        }

        YT_LOG_DEBUG("Tablet action zombified (ActionId: %v, TabletBalancerCorrelationId: %v)",
            action->GetId(),
            action->GetCorrelationId());
    }

    void TouchAffectedTabletActions(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TStringBuf request) override
    {
        YT_VERIFY(firstTabletIndex >= 0 && firstTabletIndex <= lastTabletIndex && lastTabletIndex < std::ssize(table->Tablets()));

        auto error = TError("User request %Qv interfered with the action", request);
        THashSet<TTabletBase*> touchedTablets;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            touchedTablets.insert(table->Tablets()[index]);
        }
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            if (auto action = table->Tablets()[index]->GetAction()) {
                OnTabletActionTabletsTouched(action, touchedTablets, error);
            }
        }
    }

    void OnTabletActionStateChanged(TTabletAction* action) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

    void UnbindTabletActionFromCells(TTabletAction* action) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (auto cell : action->TabletCells()) {
            cell->Actions().erase(action);
        }

        action->TabletCells().clear();
    }

    void UnbindTabletActionFromTablets(TTabletAction* action)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (auto tablet : action->Tablets()) {
            YT_VERIFY(tablet->GetAction() == action);
            tablet->SetAction(nullptr);
        }

        action->SaveTabletIds();
        action->Tablets().clear();
    }

    void UnbindTabletAction(TTabletAction* action) override
    {
        UnbindTabletActionFromTablets(action);
        UnbindTabletActionFromCells(action);
    }

    void OnTabletActionDisturbed(TTabletAction* action, const TError& error) override
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

            case ETabletActionState::ProvisionallyFlushing:
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

            case ETabletActionState::ProvisionallyFlushed:
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

    void OnTabletActionTabletsTouched(
        TTabletAction* action,
        const THashSet<TTabletBase*>& touchedTablets,
        const TError& error) override
    {
        bool touched = false;
        for (auto tablet : action->Tablets()) {
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
                    [&] (auto tablet) {
                        return touchedTablets.find(tablet) != touchedTablets.end();
                    }),
                tablets.end());
        }

        UnbindTabletActionFromCells(action);
        OnTabletActionDisturbed(action, error);
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(TabletAction, TTabletAction);

private:
    const ITabletActionManagerHostPtr Host_;
    const ISimpleHydraManagerPtr HydraManager_;
    const IInvokerPtr AutomatonInvoker_;

    TTabletActionManagerMasterConfigPtr Config_ = New<TTabletActionManagerMasterConfig>();

    NConcurrency::TPeriodicExecutorPtr CleanupExecutor_;

    TEntityMap<TTabletAction> TabletActionMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        TabletActionMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        TabletActionMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= EMasterReign::TabletActionManager) {
            TabletActionMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= EMasterReign::TabletActionManager) {
            TabletActionMap_.LoadValues(context);
        }
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        TabletActionMap_.Clear();
    }

    void HydraDestroyTabletActions(NProto::TReqDestroyTabletActions* request)
    {
        auto actionIds = FromProto<std::vector<TTabletActionId>>(request->tablet_action_ids());
        for (const auto& id : actionIds) {
            auto* action = FindTabletAction(id);
            if (IsObjectAlive(action)) {
                UnbindTabletAction(action);
                const auto& objectManager = Bootstrap_->GetObjectManager();
                objectManager->UnrefObject(action);
            }
        }
    }

    void HydraCreateTabletAction(NProto::TReqCreateTabletAction* request)
    {
        auto kind = ETabletActionKind(request->kind());
        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());
        auto cellIds = FromProto<std::vector<TTabletId>>(request->cell_ids());
        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());
        TCreateTabletActionOptions options{.ExpirationTime = TInstant::Zero()};
        if (request->has_expiration_time()) {
            options.ExpirationTime = FromProto<TInstant>(request->expiration_time());
        }
        options.ExpirationTimeout = request->has_expiration_timeout()
            ? std::optional(FromProto<TDuration>(request->expiration_timeout()))
            : std::nullopt;
        std::optional<int> tabletCount = request->has_tablet_count()
            ? std::optional(request->tablet_count())
            : std::nullopt;
        options.InplaceReshard = request->inplace_reshard();
        if (request->has_correlation_id()) {
            FromProto(&options.CorrelationId, request->correlation_id());
        }

        std::vector<TTabletBaseRawPtr> tablets;
        std::vector<TTabletCellRawPtr> cells;

        for (auto tabletId : tabletIds) {
            tablets.push_back(Host_->GetTabletOrThrow(tabletId));
        }

        for (auto cellId : cellIds) {
            cells.push_back(Host_->GetTabletCellOrThrow(cellId));
        }

        try {
            CreateTabletAction(
                NullObjectId,
                kind,
                tablets,
                cells,
                pivotKeys,
                tabletCount,
                options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(TError(ex), "Error creating tablet action (Kind: %v, "
                "Tablets: %v, TabletCells: %v, PivotKeys: %v, TabletCount: %v, TabletBalancerCorrelationId: %v)",
                kind,
                tablets,
                cells,
                pivotKeys,
                tabletCount,
                options.CorrelationId);
        }
    }

    void HydraKickOrphanedTabletActions(NProto::TReqKickOrphanedTabletActions* request)
    {
        auto healthyBundles = Host_->ListHealthyTabletCellBundles();

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

    void DoReconfigure()
    {
        CleanupExecutor_->SetPeriod(Config_->TabletActionsCleanupPeriod);
    }

    void RunCleanup()
    {
        YT_LOG_DEBUG("Periodic tablet action cleanup started");

        const auto now = TInstant::Now();

        std::vector<TTabletActionId> actionIdsPendingRemoval;
        for (auto [actionId, action] : TabletActions()) {
            if (IsObjectAlive(action) && action->IsFinished() && action->GetExpirationTime() <= now) {
                actionIdsPendingRemoval.push_back(actionId);
            }
        }

        if (!actionIdsPendingRemoval.empty()) {
            YT_LOG_DEBUG("Destroying expired tablet actions (TabletActionIds: %v)",
                actionIdsPendingRemoval);

            TReqDestroyTabletActions request;
            ToProto(request.mutable_tablet_action_ids(), actionIdsPendingRemoval);

            YT_UNUSED_FUTURE(CreateMutation(HydraManager_, request)
                ->CommitAndLog(Logger()));
        }
    }

    TTabletAction* DoCreateTabletAction(
        TObjectId hintId,
        ETabletActionKind kind,
        ETabletActionState state,
        std::vector<TTabletBaseRawPtr> tablets,
        std::vector<TTabletCellRawPtr> cells,
        std::vector<NTableClient::TLegacyOwningKey> pivotKeys,
        std::optional<int> tabletCount,
        bool freeze,
        TCreateTabletActionOptions options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(state == ETabletActionState::Preparing || state == ETabletActionState::Orphaned);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto id = objectManager->GenerateId(EObjectType::TabletAction, hintId);
        auto actionHolder = TPoolAllocator::New<TTabletAction>(id);
        auto* action = TabletActionMap_.Insert(id, std::move(actionHolder));
        objectManager->RefObject(action);

        if (kind == ETabletActionKind::Reshard && options.InplaceReshard) {
            action->TabletMountRevisions().reserve(tablets.size());
        }

        for (auto tablet : tablets) {
            YT_VERIFY(tablet->GetType() == EObjectType::Tablet);

            tablet->SetAction(action);
            if (kind == ETabletActionKind::Reshard && options.InplaceReshard) {
                action->TabletMountRevisions().push_back(tablet->Servant().GetMountRevision());
            }

            if (state == ETabletActionState::Orphaned) {
                // Orphaned action can be created during mount if tablet cells are not available.
                // User can't create orphaned action directly because primary master need to know about mount.
                YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);
                tablet->SetExpectedState(freeze
                    ? ETabletState::Frozen
                    : ETabletState::Mounted);
            }
        }

        for (auto cell : cells) {
            cell->Actions().insert(action);
        }

        action->SetKind(kind);
        action->SetState(state);
        action->Tablets() = std::move(tablets);
        action->TabletCells() = std::move(cells);
        action->PivotKeys() = std::move(pivotKeys);
        action->SetTabletCount(tabletCount);
        action->SetSkipFreezing(options.SkipFreezing);
        action->SetFreeze(freeze);
        action->SetCorrelationId(options.CorrelationId);
        action->SetExpirationTime(options.ExpirationTime);
        action->SetExpirationTimeout(options.ExpirationTimeout);
        const auto& bundle = action->Tablets()[0]->GetOwner()->TabletCellBundle();
        action->SetTabletCellBundle(bundle.Get());
        action->SetInplaceReshard(options.InplaceReshard);
        const auto* table = action->Tablets()[0]->GetOwner()->As<TTableNode>();
        action->SetProvisionalFlushRequired(
            options.InplaceReshard &&
            !action->GetFreeze() &&
            table->IsPhysicallySorted());
        bundle->TabletActions().insert(action);
        bundle->IncreaseActiveTabletActionCount();

        YT_LOG_DEBUG("Tablet action created (%v)",
            *action);

        return action;
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
            "TableId: %v, Bundle: %v, TabletBalancerCorrelationId: %v)",
            action->GetId(),
            state,
            tableId,
            action->GetTabletCellBundle()->GetName(),
            action->GetCorrelationId());
        if (recursive) {
            OnTabletActionStateChanged(action);
        }
    }

    void DoTabletActionStateChanged(TTabletAction* action)
    {
        switch (action->GetState()) {
            case ETabletActionState::Preparing: {
                if (action->GetKind() == ETabletActionKind::SmoothMove) {
                    YT_VERIFY(action->Tablets().size() == 1);
                    YT_VERIFY(action->TabletCells().size() == 1);

                    auto cell = action->TabletCells()[0];
                    YT_VERIFY(action->Tablets()[0]->GetType() == EObjectType::Tablet);

                    auto* tablet = action->Tablets()[0]->As<TTablet>();

                    TTableSettings tableSettings;
                    try {
                        tableSettings = Host_->GetTableSettings(tablet->GetTable());
                    } catch (const std::exception& ex) {
                        YT_LOG_DEBUG(ex, "Failed to mount auxiliary servant "
                            "(TableId: %v, TabletId: %v, AuxiliaryCellId: %v)",
                            tablet->GetTable()->GetId(),
                            tablet->GetId(),
                            cell->GetId());
                        throw;
                    }

                    auto serializedTableSettings = SerializeTableSettings(tableSettings);
                    Host_->AllocateAuxiliaryServant(tablet, cell, serializedTableSettings);
                    ChangeTabletActionState(
                        action,
                        ETabletActionState::MountingAuxiliary,
                        /*recursive*/ false);

                    break;
                }

                if (action->IsProvisionalFlushRequired()) {
                    action->FlushingTablets().reserve(action->Tablets().size());
                    for (auto tablet : action->Tablets()) {
                        action->FlushingTablets().insert(tablet->GetId());
                        Host_->RequestProvisionalFlush(tablet);
                    }

                    ChangeTabletActionState(action, ETabletActionState::ProvisionallyFlushing);
                    break;
                }

                if (action->GetSkipFreezing()) {
                    ChangeTabletActionState(action, ETabletActionState::Frozen);
                    break;
                }

                for (auto tablet : action->Tablets()) {
                    Host_->DoFreezeTablet(tablet);
                }

                ChangeTabletActionState(action, ETabletActionState::Freezing);
                break;
            }

            case ETabletActionState::ProvisionallyFlushing: {
                if (action->FlushingTablets().empty()) {
                    auto state = action->Error().IsOK()
                        ? ETabletActionState::ProvisionallyFlushed
                        : ETabletActionState::Failing;
                    ChangeTabletActionState(action, state);
                }

                break;
            }

            case ETabletActionState::ProvisionallyFlushed: {
                if (action->GetSkipFreezing()) {
                    ChangeTabletActionState(action, ETabletActionState::Frozen);
                    break;
                }

                for (auto tablet : action->Tablets()) {
                    Host_->DoFreezeTablet(tablet);
                }

                ChangeTabletActionState(action, ETabletActionState::Freezing);
                break;
            }

            case ETabletActionState::Freezing: {
                int freezingCount = 0;
                for (auto tablet : action->Tablets()) {
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
                auto* table = action->Tablets().front()->GetOwner();
                bool inplaceReshard = action->IsInplaceReshard() &&
                    action->GetKind() == ETabletActionKind::Reshard;
                bool retainPreloadedChunks =
                    inplaceReshard &&
                    table->GetInMemoryMode() != NTabletClient::EInMemoryMode::None;
                for (auto tablet : action->Tablets()) {
                    YT_VERIFY(IsObjectAlive(tablet));
                    Host_->UnmountTablet(
                        tablet,
                        /*force*/ false,
                        /*onDestroy*/ false,
                        TUnmountTabletOptions{
                            .RetainPreloadedChunks = retainPreloadedChunks,
                            .UseExtendedSnapshotEvictionTimeout = inplaceReshard,
                        });
                }

                ChangeTabletActionState(action, ETabletActionState::Unmounting);
                break;
            }

            case ETabletActionState::Unmounting: {
                int unmountingCount = 0;
                for (auto tablet : action->Tablets()) {
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

                        std::vector<TTabletBaseRawPtr> oldTablets;
                        oldTablets.swap(action->Tablets());
                        for (auto tablet : oldTablets) {
                            tablet->SetAction(nullptr);
                        }
                        for (auto tablet : oldTablets) {
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
                            newTabletCount = Host_->TrySyncReshard(
                                table,
                                firstTabletIndex,
                                lastTabletIndex,
                                newTabletCount,
                                action->PivotKeys());
                        } catch (const std::exception& ex) {
                            for (auto tablet : oldTablets) {
                                YT_VERIFY(IsObjectAlive(tablet));
                                tablet->SetAction(action);
                            }
                            action->Tablets() = std::move(oldTablets);
                            throw;
                        }

                        action->Tablets() = std::vector<TTabletBaseRawPtr>(
                            table->Tablets().begin() + firstTabletIndex,
                            table->Tablets().begin() + firstTabletIndex + newTabletCount);
                        for (auto tablet : action->Tablets()) {
                            tablet->SetAction(action);
                            tablet->SetExpectedState(expectedState);
                        }

                        if (action->IsInplaceReshard()) {
                            Host_->SendReshardRedirectionHint(
                                action->TabletCells()[0]->GetId(),
                                action->Tablets(),
                                oldTablets,
                                action->TabletMountRevisions());
                        }

                        break;
                    }

                    default:
                        YT_ABORT();
                }

                TTableSettings tableSettings;
                try {
                    tableSettings = Host_->GetTableSettings(table->As<TTableNode>());

                    ValidateTableMountConfig(
                        table->As<TTableNode>(),
                        tableSettings.EffectiveMountConfig,
                        Host_->GetDynamicConfig());
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
                        action->Tablets(),
                        Host_->GetDynamicConfig()->TabletDataSizeFootprint);
                } else {
                    YT_VERIFY(action->TabletCells().size() >= action->Tablets().size());
                    for (int index = 0; index < std::ssize(action->Tablets()); ++index) {
                        assignment.emplace_back(
                            action->Tablets()[index],
                            action->TabletCells()[index]);
                    }
                }

                bool useRetainedPreloadedChunks = table->GetInMemoryMode() != NTabletClient::EInMemoryMode::None &&
                    action->GetKind() == ETabletActionKind::Reshard &&
                    action->IsInplaceReshard();

                Host_->DoMountTablets(
                    table,
                    serializedTableSettings,
                    assignment,
                    action->GetFreeze(),
                    useRetainedPreloadedChunks);

                ChangeTabletActionState(action, ETabletActionState::Mounting);
                break;
            }

            case ETabletActionState::Mounting: {
                int mountedCount = 0;
                for (auto tablet : action->Tablets()) {
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
                [[fallthrough]];

            case ETabletActionState::Failed: {
                UnbindTabletAction(action);
                if (auto bundle = action->GetTabletCellBundle()) {
                    bundle->DecreaseActiveTabletActionCount();
                }
                const auto now = GetCurrentMutationContext()->GetTimestamp();
                if (action->GetExpirationTime() <= now) {
                    const auto& objectManager = Bootstrap_->GetObjectManager();
                    objectManager->UnrefObject(action);
                }
                break;
            }

            case ETabletActionState::MountingAuxiliary: {
                auto tablet = action->Tablets()[0];
                if (tablet->AuxiliaryServant().GetState() != ETabletState::Mounted) {
                    break;
                }

                auto cell = action->TabletCells()[0];
                Host_->StartSmoothMovement(tablet, cell);

                ChangeTabletActionState(action, ETabletActionState::WaitingForSmoothMove);
                break;
            }

            case ETabletActionState::WaitingForSmoothMove: {
                auto cell = action->TabletCells()[0];
                auto tablet = action->Tablets()[0];
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
                auto tablet = action->Tablets()[0];
                YT_VERIFY(tablet->Servant());

                if (tablet->AuxiliaryServant()) {
                    if (auto state = tablet->AuxiliaryServant().GetState();
                        state == ETabletState::Mounting || state == ETabletState::FrozenMounting)
                    {
                        // Auxiliary servant has not yet been mounted. Main servant does not participate
                        // in movement yet.
                        Host_->DeallocateAuxiliaryServant(tablet);
                    } else {
                        // Main smooth movement phase is in progress, two-phase abort is required.
                        Host_->AbortSmoothMovement(tablet);
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

    void MountMissedInActionTablets(TTabletAction* action)
    {
        for (auto tablet : action->Tablets()) {
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
                        Host_->MountTablet(tablet, /*cell*/ nullptr, action->GetFreeze());
                        break;

                    case ETabletState::Frozen:
                        if (!action->GetFreeze()) {
                            Host_->DoUnfreezeTablet(tablet);
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

    NHydra::TEntityMap<TTabletAction>& MutableTabletActionMapCompat() override
    {
        return TabletActionMap_;
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletActionManager, TabletAction, TTabletAction, TabletActionMap_);

////////////////////////////////////////////////////////////////////////////////

ITabletActionManagerPtr CreateTabletActionManager(
    TBootstrap* bootstrap,
    ITabletActionManagerHostPtr host,
    ISimpleHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker)
{
    return New<TTabletActionManager>(
        bootstrap,
        std::move(host),
        std::move(hydraManager),
        std::move(automatonInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
