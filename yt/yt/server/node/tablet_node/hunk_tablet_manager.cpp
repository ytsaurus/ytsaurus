#include "hunk_tablet_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "hunk_store.h"
#include "hunk_tablet_scanner.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/hive/avenue_directory.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/journal_client/config.h>
#include <yt/yt/ytlib/journal_client/journal_hunk_chunk_writer.h>

#include <yt/yt/ytlib/tablet_client/proto/tablet_service.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHiveServer;
using namespace NHydra;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTransactionSupervisor;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class THunkTabletManager
    : public IHunkTabletManager
    , public IHunkTabletHost
    , public TTabletAutomatonPart
{
public:
    THunkTabletManager(
        IBootstrap* bootstrap,
        ITabletSlotPtr slot)
        : TTabletAutomatonPart(
            slot->GetCellId(),
            slot->GetSimpleHydraManager(),
            slot->GetAutomaton(),
            slot->GetAutomatonInvoker(),
            slot->GetMutationForwarder())
        , Bootstrap_(bootstrap)
        , Slot_(std::move(slot))
        , TabletMap_(TEntityMapTraits(this))
        , OrphanedTabletMap_(TEntityMapTraits(this))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "HunkTabletManager.Keys",
            BIND(&THunkTabletManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "HunkTabletManager.Values",
            BIND(&THunkTabletManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "HunkTabletManager.Keys",
            BIND(&THunkTabletManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "HunkTabletManager.Values",
            BIND(&THunkTabletManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&THunkTabletManager::HydraMountHunkTablet, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&THunkTabletManager::HydraUnmountHunkTablet, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqUpdateHunkTabletStores>({
            .Prepare = BIND_NO_PROPAGATE(&THunkTabletManager::HydraPrepareUpdateHunkTabletStores, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&THunkTabletManager::HydraCommitUpdateHunkTabletStores, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&THunkTabletManager::HydraAbortUpdateHunkTabletStores, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<NTabletClient::NProto::TReqToggleHunkTabletStoreLock>({
            .Prepare = BIND_NO_PROPAGATE(&THunkTabletManager::HydraPrepareToggleHunkTabletStoreLock, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&THunkTabletManager::HydraCommitToggleHunkTabletStoreLock, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&THunkTabletManager::HydraAbortToggleHunkTabletStoreLock, Unretained(this)),
        });
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Tablet, THunkTablet);

    THunkTablet* GetTabletOrThrow(TTabletId tabletId) override
    {
        if (auto* tablet = FindTablet(tabletId)) {
            return tablet;
        } else {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }
    }

    void CheckFullyUnlocked(THunkTablet* tablet) override
    {
        if (tablet->GetState() != ETabletState::Orphaned) {
            return;
        }

        if (tablet->IsFullyUnlocked(/*forceUnmount*/ true)) {
            auto tabletId = tablet->GetId();

            YT_LOG_DEBUG("Orphaned tablet is fully unlocked; destroying (TabletId: %v)",
                tabletId);

            Y_UNUSED(OrphanedTabletMap_.Release(tabletId));
        }
    }

    virtual IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    IBootstrap* const Bootstrap_;
    const ITabletSlotPtr Slot_;

    IHunkTabletScannerPtr HunkTabletScanner_;
    TPeriodicExecutorPtr ScannerExecutor_;

    class TEntityMapTraits
    {
    public:
        explicit TEntityMapTraits(IHunkTabletHost* host)
            : Host_(host)
        { }

        std::unique_ptr<THunkTablet> Create(TTabletId id) const
        {
            return std::make_unique<THunkTablet>(MakeStrong(Host_), id);
        }

    private:
        IHunkTabletHost* const Host_;
    };

    TEntityMap<THunkTablet, TEntityMapTraits> TabletMap_;
    TEntityMap<THunkTablet, TEntityMapTraits> OrphanedTabletMap_;

    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<THunkTabletManager> impl, IInvokerPtr invoker)
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
                    auto producer = BIND(&THunkTablet::BuildOrchidYson, tablet);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<THunkTabletManager> Owner_;

        explicit TOrchidService(TWeakPtr<THunkTabletManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND()
    };

    const IYPathServicePtr OrchidService_;

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderActive();

        HunkTabletScanner_ = CreateHunkTabletScanner(Bootstrap_, Slot_);
        ScannerExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND(&THunkTabletManager::ScanAllTablets, MakeWeak(this)),
            TDuration::MilliSeconds(100));
        ScannerExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        HunkTabletScanner_.Reset();

        if (ScannerExecutor_) {
            YT_UNUSED_FUTURE(ScannerExecutor_->Stop());
            ScannerExecutor_.Reset();
        }

        for (auto [tabletId, tablet] : TabletMap_) {
            tablet->OnStopLeading();
        }
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadValues(context);
    }

    void SaveKeys(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.SaveValues(context);
    }

    void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnAfterSnapshotLoaded();

        const auto& avenueDirectory = Slot_->GetAvenueDirectory();
        for (auto [tabletId, tablet] : TabletMap_) {
            if (auto masterEndpointId = tablet->GetMasterAvenueEndpointId()) {
                auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
                avenueDirectory->UpdateEndpoint(masterEndpointId, masterCellId);
            }
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        TabletMap_.Clear();
        OrphanedTabletMap_.Clear();
    }

    void HydraMountHunkTablet(NProto::TReqMountHunkTablet* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto masterAvenueEndpointId = FromProto<TAvenueEndpointId>(request->master_avenue_endpoint_id());

        auto tabletHolder = std::make_unique<THunkTablet>(MakeStrong(this), tabletId);
        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        tablet->SetState(ETabletState::Mounted);
        tablet->SetMountRevision(mountRevision);

        auto settings = DeserializeHunkStorageSettings(*request, tabletId);
        tablet->Reconfigure(settings);

        for (const auto& protoStoreId : request->store_ids()) {
            auto storeId = FromProto<TStoreId>(protoStoreId);
            auto store = New<THunkStore>(storeId, tablet);
            store->SetState(EHunkStoreState::Passive);
            tablet->AddStore(std::move(store));
        }

        if (masterAvenueEndpointId) {
            tablet->SetMasterAvenueEndpointId(masterAvenueEndpointId);
            Slot_->RegisterMasterAvenue(tablet->GetId(), masterAvenueEndpointId, /*cookie*/ {});
        }

        ScheduleScanTablet(tablet->GetId());

        YT_LOG_DEBUG(
            "Hunk tablet mounted (TabletId: %v, MountRevision: %x, MasterAvenueEndpointId: %v)",
            tabletId,
            mountRevision,
            masterAvenueEndpointId);

        {
            TRspMountHunkTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);

            // COMPAT(ifsmirnov)
            if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
                response.set_mount_revision(tablet->GetMountRevision());
            }

            Slot_->PostMasterMessage(tabletId, response);
        }
    }

    void HydraUnmountHunkTablet(NProto::TReqUnmountHunkTablet* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto force = request->force();

        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_LOG_DEBUG(
            "Unmounting hunk tablet (TabletId: %v, Force: %v)",
            tabletId,
            force);

        tablet->OnUnmount();

        if (force) {
            DoUnmountTablet(tablet);
        } else {
            tablet->SetState(ETabletState::UnmountPending);
            CheckUnmounted(tablet);
        }

        ScheduleScanTablet(tabletId);
    }

    void HydraPrepareUpdateHunkTabletStores(
        TTransaction* transaction,
        TReqUpdateHunkTabletStores* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        auto transactionId = transaction->GetId();
        tablet->LockTransaction(transactionId);

        for (const auto& storeToRemove : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(storeToRemove.store_id());
            auto store = tablet->GetStoreOrThrow(storeId);

            // NB: Cannot throw here since store state is transient.
            YT_VERIFY(store->GetState() == EHunkStoreState::Passive);

            store->Lock(transaction->GetId(), EObjectLockMode::Exclusive);

            if (!store->GetMarkedSealable()) {
                THROW_ERROR_EXCEPTION(
                    "Cannot remove store %v of tablet %v that was not marked as sealable",
                    storeId,
                    tabletId);
            }
        }

        for (const auto& storeToMarkSealable : request->stores_to_mark_sealable()) {
            auto storeId = FromProto<TStoreId>(storeToMarkSealable.store_id());
            auto store = tablet->GetStoreOrThrow(storeId);
            store->Lock(transaction->GetId(), EObjectLockMode::Exclusive);
        }

        YT_LOG_DEBUG(
            "Hunk tablet store update transaction prepared (TransactionId: %v)",
            transactionId);
    }

    void HydraCommitUpdateHunkTabletStores(
        TTransaction* transaction,
        TReqUpdateHunkTabletStores* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        // Tablet may be missing if it was forcefully removed.
        if (!tablet) {
            return;
        }

        // Mount revision mismatch is possible in case of force unmount.
        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto transactionId = transaction->GetId();
        if (!tablet->TryUnlockTransaction(transactionId)) {
            YT_LOG_ALERT("Failed to unlock hunk tablet (TransactionId: %v, HunkTabletLockTransactionId: %v)",
                transactionId,
                tablet->GetLockTransactionId());
        }

        for (const auto& storeToAdd : request->stores_to_add()) {
            auto sessionId = FromProto<TSessionId>(storeToAdd.session_id());
            auto store = New<THunkStore>(sessionId.ChunkId, tablet);
            auto now = GetCurrentMutationContext()->GetTimestamp();
            store->SetCreationTime(now);

            if (IsLeader() && tablet->GetState() == ETabletState::Mounted) {
                store->SetState(EHunkStoreState::Allocated);

                auto writer = CreateJournalHunkChunkWriter(
                    Bootstrap_->GetClient(),
                    sessionId,
                    tablet->StoreWriterOptions(),
                    tablet->StoreWriterConfig(),
                    Logger);
                store->SetWriter(std::move(writer));
                store->SetLastWriteTime(now);
            } else {
                store->SetState(EHunkStoreState::Passive);
            }

            tablet->AddStore(std::move(store));
        }

        for (const auto& storeToRemove : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(storeToRemove.store_id());
            auto store = tablet->GetStore(storeId);
            store->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);
            tablet->RemoveStore(std::move(store));
        }

        for (const auto& storeToMarkSealable : request->stores_to_mark_sealable()) {
            auto storeId = FromProto<TStoreId>(storeToMarkSealable.store_id());
            auto store = tablet->GetStore(storeId);
            store->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);
            store->SetMarkedSealable(true);

            YT_LOG_DEBUG(
                "Hunk tablet store marked as sealable "
                "(TransactionId: %v, StoreId: %v)",
                transactionId,
                store->GetId());
        }

        ScheduleScanTablet(tablet->GetId());

        YT_LOG_DEBUG(
            "Hunk tablet store update transaction committed (TransactionId: %v)",
            transactionId);

        // NB: May destroy tablet.
        CheckUnmounted(tablet);
    }

    void HydraAbortUpdateHunkTabletStores(
        TTransaction* transaction,
        TReqUpdateHunkTabletStores* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        // Tablet may be missing if it was forcefully removed.
        if (!tablet) {
            return;
        }

        // Mount revision mismatch is possible in case of force unmount.
        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto transactionId = transaction->GetId();
        tablet->TryUnlockTransaction(transactionId);

        for (const auto& storeToRemove : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(storeToRemove.store_id());
            if (auto store = tablet->FindStore(storeId)) {
                store->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);
            }
        }

        for (const auto& storeToMarkSealable : request->stores_to_mark_sealable()) {
            auto storeId = FromProto<TStoreId>(storeToMarkSealable.store_id());
            if (auto store = tablet->FindStore(storeId)) {
                store->Unlock(transaction->GetId(), EObjectLockMode::Exclusive);
            }
        }

        ScheduleScanTablet(tabletId);

        YT_LOG_DEBUG(
            "Hunk tablet store update transaction aborted "
            "(TransactionId: %v, TabletId: %v)",
            transactionId,
            tabletId);

        // NB: May destroy tablet.
        CheckUnmounted(tablet);
    }

    void HydraPrepareToggleHunkTabletStoreLock(
        TTransaction* transaction,
        NTabletClient::NProto::TReqToggleHunkTabletStoreLock* request,
        const TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(options.Persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto lockerTabletId = FromProto<TTabletId>(request->locker_tablet_id());

        auto* tablet = GetTabletOrThrow(tabletId);

        auto mountRevision = request->mount_revision();
        auto lock = request->lock();

        if (lock) {
            tablet->ValidateMounted(mountRevision);
        } else {
            tablet->ValidateMountRevision(mountRevision);
        }

        auto storeId = FromProto<TStoreId>(request->store_id());
        auto store = tablet->GetStoreOrThrow(storeId);
        store->Lock(transaction->GetId(), EObjectLockMode::Shared);
        if (!lock && !store->IsLockedByTablet(lockerTabletId)) {
            THROW_ERROR_EXCEPTION("Store %v of tablet %v is not locked by tablet %v",
                storeId,
                tabletId,
                lockerTabletId);
        }

        YT_LOG_DEBUG(
            "Hunk tablet store lock toggle prepared "
            "(TransactionId: %v, TabletId: %v, StoreId: %v, LockerTabletId: %v, Lock: %v)",
            transaction->GetId(),
            tabletId,
            storeId,
            lockerTabletId,
            lock);
    }

    void HydraCommitToggleHunkTabletStoreLock(
        TTransaction* transaction,
        NTabletClient::NProto::TReqToggleHunkTabletStoreLock* request,
        const TTransactionCommitOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto lockerTabletId = FromProto<TTabletId>(request->locker_tablet_id());

        auto* tablet = FindTablet(tabletId);
        // NB: Tablet may be missing if is was e.g. forcefully removed.
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto storeId = FromProto<TStoreId>(request->store_id());
        auto store = tablet->FindStore(storeId);
        if (!store) {
            YT_LOG_ALERT(
                "Hunk store is missing during hunk tablet store lock toggle "
                "(TransactionId: %v, TabletId: %v, StoreId: %v)",
                transaction->GetId(),
                tabletId,
                storeId);
            return;
        }

        auto lock = request->lock();
        if (lock) {
            store->Lock(lockerTabletId);
        } else {
            // NB: This is possible, since lock toggle is done under shared lock
            // and thus concurrent unlocks are possible.
            // However this should not happen if hunk tablets are properly unlocked
            // in regular tablets.
            if (!store->IsLockedByTablet(lockerTabletId)) {
                YT_LOG_ALERT(
                    "Hunk store lock is lost during lock toggle "
                    "(TransactionId: %v, TabletId: %v, StoreId: %v)",
                    transaction->GetId(),
                    tabletId,
                    store->GetId());
                return;
            }

            store->Unlock(lockerTabletId);
        }

        store->Unlock(transaction->GetId(), EObjectLockMode::Shared);

        YT_LOG_DEBUG(
            "Hunk tablet store lock toggle committed "
            "(TransactionId: %v, TabletId: %v, StoreId: %v, LockerTabletId: %v, Lock: %v)",
            transaction->GetId(),
            tabletId,
            storeId,
            lockerTabletId,
            lock);
    }

    void HydraAbortToggleHunkTabletStoreLock(
        TTransaction* transaction,
        NTabletClient::NProto::TReqToggleHunkTabletStoreLock* request,
        const TTransactionAbortOptions& /*options*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto lockerTabletId = FromProto<TTabletId>(request->locker_tablet_id());

        auto* tablet = FindTablet(tabletId);
        // NB: Tablet may be missing if is was e.g. forcefully removed.
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto storeId = FromProto<TStoreId>(request->store_id());
        auto store = tablet->FindStore(storeId);
        if (!store) {
            return;
        }

        store->Unlock(transaction->GetId(), EObjectLockMode::Shared);

        YT_LOG_DEBUG(
            "Hunk tablet store lock toggle aborted "
            "(TransactionId: %v, TabletId: %v, StoreId: %v, LockerTabletId: %v, Lock: %v)",
            transaction->GetId(),
            tabletId,
            storeId,
            lockerTabletId,
            request->lock());
    }

    void CheckUnmounted(THunkTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (IsInUnmountWorkflow(tablet->GetState()) && tablet->IsReadyToUnmount()) {
            DoUnmountTablet(tablet);
        }
    }

    void DoUnmountTablet(THunkTablet* tablet)
    {
        if (tablet->GetState() == ETabletState::Unmounted) {
            return;
        }

        tablet->SetState(ETabletState::Unmounted);

        auto tabletId = tablet->GetId();
        auto tabletHolder = TabletMap_.Release(tabletId);

        YT_LOG_DEBUG(
            "Tablet unmounted (TabletId: %v)",
            tabletId);

        if (tablet->IsFullyUnlocked()) {
            YT_LOG_DEBUG(
                "Tablet destroyed (TabletId: %v)",
                tabletId);
        } else {
            YT_LOG_DEBUG(
                "Tablet became orphaned (TabletId: %v)",
                tabletId);
            OrphanedTabletMap_.Insert(tabletId, std::move(tabletHolder));
        }

        if (auto masterEndpointId = tablet->GetMasterAvenueEndpointId()) {
            Slot_->UnregisterMasterAvenue(masterEndpointId);
        }

        {
            TRspUnmountHunkTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);

            if (GetCurrentMutationContext()->Request().Reign >= static_cast<int>(ETabletReign::SmoothTabletMovement)) {
                response.set_mount_revision(tablet->GetMountRevision());
            }

            Slot_->PostMasterMessage(tabletId, response);
        }
    }

    void ScanAllTablets()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (auto [tabletId, tablet] : TabletMap_) {
            ScheduleScanTablet(tabletId);
        }
    }

    void ScheduleScanTablet(TTabletId tabletId) override
    {
        if (IsLeader()) {
            EpochAutomatonInvoker_->Invoke(
                BIND(&THunkTabletManager::ScanTablet, MakeStrong(this), tabletId));
        }
    }

    void ScanTablet(TTabletId tabletId)
    {
        if (auto* tablet = FindTablet(tabletId)) {
            if (HunkTabletScanner_) {
                HunkTabletScanner_->Scan(tablet);
            }
        }
    }

    template <class TRequest>
    NTabletNode::THunkStorageSettings DeserializeHunkStorageSettings(const TRequest& request, TTabletId tabletId)
    {
        const auto& hunkStorageSettings = request.hunk_storage_settings();
        return {
            .MountConfig = DeserializeHunkStorageMountConfig(TYsonString{hunkStorageSettings.mount_config()}, tabletId),
            .StoreWriterConfig = DeserializeHunkStoreWriterConfig(TYsonString{hunkStorageSettings.hunk_store_config()}, tabletId),
            .StoreWriterOptions = DeserializeHunkStoreWriterOptions(TYsonString{hunkStorageSettings.hunk_store_options()}, tabletId),
        };
    }

    THunkStorageMountConfigPtr DeserializeHunkStorageMountConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<THunkStorageMountConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex,
                "Error deserializing hunk storage mount config (TabletId: %v)",
                tabletId);
            return New<THunkStorageMountConfig>();
        }
    }

    THunkStoreWriterConfigPtr DeserializeHunkStoreWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<THunkStoreWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex,
                "Error deserializing hunk store writer config (TabletId: %v)",
                tabletId);
            return New<THunkStoreWriterConfig>();
        }
    }

    THunkStoreWriterOptionsPtr DeserializeHunkStoreWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<THunkStoreWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex,
                "Error deserializing hunk store writer options (TabletId: %v)",
                tabletId);
            return New<THunkStoreWriterOptions>();
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(THunkTabletManager, Tablet, THunkTablet, TabletMap_);

////////////////////////////////////////////////////////////////////////////////

IHunkTabletManagerPtr CreateHunkTabletManager(IBootstrap* bootstrap, ITabletSlotPtr slot)
{
    return New<THunkTabletManager>(bootstrap, std::move(slot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
