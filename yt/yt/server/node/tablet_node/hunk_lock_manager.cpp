#include "hunk_lock_manager.h"

#include "bootstrap.h"
#include "serialize.h"
#include "private.h"
#include "tablet.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra_common/mutation_context.h>

#include <yt/yt/ytlib/tablet_client/proto/tablet_service.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTransactionSupervisor;
using namespace NTransactionClient;
using namespace NClusterNode;
using namespace NApi;
using namespace NApi::NNative;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TLockingState
{
    // Persistent.
    int PersistentLockCount = 0;
    // We need these for unlock.
    TCellId HunkCellId;
    TTabletId HunkTabletId;
    TRevision HunkMountRevision;

    // Transient.
    int TransientLockCount = 0;
    TInstant LastChangeTime;
    // Should only change false -> true.
    bool IsBeingUnlocked = false;

    void Save(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, PersistentLockCount);
        Save(context, HunkCellId);
        Save(context, HunkTabletId);
        Save(context, HunkMountRevision);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, PersistentLockCount);
        Load(context, HunkCellId);
        Load(context, HunkTabletId);
        Load(context, HunkMountRevision);
    }
};


void FormatValue(TStringBuilderBase* builder, const TLockingState& ref, TStringBuf /*spec*/)
{
    builder->AppendFormat("PersistentLockCount: %v, TransientLockCount}",
        ref.PersistentLockCount,
        ref.TransientLockCount);
}

TString ToString(const TLockingState& ref)
{
    return ToStringViaBuilder(ref);
}

////////////////////////////////////////////////////////////////////////////////

class THunkLockManager
    : public IHunkLockManager
{
public:
    THunkLockManager(
        ITabletContext* context,
        TTabletId tabletId)
        : TabletId_(tabletId)
        , Context_(context)
        , Logger(TabletNodeLogger.WithTag("LockerTabletId: %v", TabletId_))
    { }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Context_->GetDynamicConfigManager();
        configManager->SubscribeConfigChanged(BIND(&THunkLockManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void StartEpoch() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto config = GetDynamicConfig();

        if (Context_->GetAutomatonState() == EPeerState::Leading) {
            UnlockExecutor_.Store(New<TPeriodicExecutor>(
                Context_->GetEpochAutomatonInvoker(),
                BIND(&THunkLockManager::UnlockStaleHunkStores, MakeWeak(this)),
                config->UnlockCheckPeriod));
            UnlockExecutor_.Load()->Start();
        }
    }

    void StopEpoch() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        UnlockExecutor_.Store(nullptr);

        ClearTransientState();
    }

    void RegisterHunkStore(
        THunkStoreId hunkStoreId,
        TCellId hunkCellId,
        TTabletId hunkTabletId,
        TRevision hunkMountRevision) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Hunk store added (HunkStoreId: %v, HunkCellId: %v, HunkTabletId: %v)",
            hunkStoreId,
            hunkCellId,
            hunkTabletId);

        TLockingState lockingState{
            .HunkCellId = hunkCellId,
            .HunkTabletId = hunkTabletId,
            .HunkMountRevision = hunkMountRevision,
            .LastChangeTime = TInstant::Now()
        };
        if (!HunkStoreIdToLockingState_.emplace(hunkStoreId, lockingState).second) {
            YT_LOG_ALERT("Trying to register hunk lock that is already registered (HunkStoreId: %v)",
                hunkStoreId);
        }
        SetHunkStoreLockingFuture(hunkStoreId, {});
    }

    void UnregisterHunkStore(THunkStoreId hunkStoreId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto it = HunkStoreIdToLockingState_.find(hunkStoreId);
        YT_VERIFY(it != HunkStoreIdToLockingState_.end());
        YT_VERIFY(it->second.PersistentLockCount + it->second.TransientLockCount == 0);

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Hunk store removed (HunkStoreId: %v)",
            hunkStoreId);

        HunkStoreIdToLockingState_.erase(it);
    }

    void IncrementPersistentLockCount(THunkStoreId hunkStoreId, int count) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto& lockingState = GetOrCrash(HunkStoreIdToLockingState_, hunkStoreId);
        lockingState.PersistentLockCount += count;
        YT_VERIFY(lockingState.PersistentLockCount >= 0);

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Hunk store locked persistently (HunkStoreId: %v, LockingState: %v, Delta: %v)",
            hunkStoreId,
            lockingState,
            count);

        Touch(lockingState);
    }

    void IncrementTransientLockCount(THunkStoreId hunkStoreId, int count) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto it = HunkStoreIdToLockingState_.find(hunkStoreId);
        if (it == HunkStoreIdToLockingState_.end()) {
            THROW_ERROR_EXCEPTION("Cannot lock hunk store %v as it is not registered",
                hunkStoreId);
        }

        auto& lockingState = it->second;
        if (lockingState.IsBeingUnlocked) {
            THROW_ERROR_EXCEPTION("Cannot reference hunk store %v as it is being unlocked",
                hunkStoreId);
        }

        lockingState.TransientLockCount += count;
        YT_VERIFY(lockingState.TransientLockCount >= 0);

        YT_LOG_DEBUG("Hunk store locked transiently (HunkStoreId: %v, LockingState: %v, Delta: %v)",
            hunkStoreId,
            lockingState,
            count);

        Touch(lockingState);
    }

    int GetTotalLockedHunkStoreCount() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return std::ssize(HunkStoreIdToLockingState_);
    }

    std::optional<int> GetPersistentLockCount(THunkStoreId hunkStoreId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto it = HunkStoreIdToLockingState_.find(hunkStoreId); it != HunkStoreIdToLockingState_.end()) {
            return it->second.PersistentLockCount;
        }

        return std::nullopt;
    }

    std::optional<int> GetTotalLockCount(THunkStoreId hunkStoreId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto it = HunkStoreIdToLockingState_.find(hunkStoreId); it != HunkStoreIdToLockingState_.end()) {
            return it->second.TransientLockCount + it->second.PersistentLockCount;
        }

        return std::nullopt;
    }

    TFuture<void> LockHunkStores(const THunkChunksInfo& hunkChunksInfo) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TFuture<void>> futures;
        std::vector<TChunkId> hunkStoreIdsToLock;

        for (auto [hunkStoreId, _] : hunkChunksInfo.HunkChunkRefs) {
            if (auto it = HunkStoreIdToLockingState_.find(hunkStoreId); it != HunkStoreIdToLockingState_.end()) {
                if (it->second.IsBeingUnlocked) {
                    THROW_ERROR_EXCEPTION("Hunk store %v is being unlocked",
                        hunkStoreId);
                }
                continue;
            }

            if (auto it = HunkStoreIdsBeingLockedToPromise_.find(hunkStoreId); it != HunkStoreIdsBeingLockedToPromise_.end()) {
                futures.push_back(it->second.ToFuture().ToUncancelable());
                continue;
            }

            hunkStoreIdsToLock.push_back(hunkStoreId);
        }

        for (auto hunkStoreId : hunkStoreIdsToLock) {
            YT_LOG_DEBUG("Locking hunk store (HunkStoreId: %v, HunkCellId: %v, HunkTabletId: %v)",
                hunkStoreId,
                hunkChunksInfo.CellId,
                hunkChunksInfo.HunkTabletId);
            ToggleLock(
                hunkChunksInfo.CellId,
                hunkChunksInfo.HunkTabletId,
                hunkStoreId,
                hunkChunksInfo.MountRevision,
                /*lock*/ true);

            auto promise = NewPromise<void>();
            EmplaceOrCrash(HunkStoreIdsBeingLockedToPromise_, hunkStoreId, promise);
            futures.push_back(promise.ToFuture().ToUncancelable());
        }

        return AllSucceeded(std::move(futures));
    }

    void OnBoggleLockPrepared(THunkStoreId hunkStoreId, bool lock) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (lock) {
            if (!HunkStoreIdsBeingLockedToPromise_.find(hunkStoreId)) {
                HunkStoreIdsBeingLockedToPromise_.emplace(hunkStoreId, NewPromise<void>());
            }
        } else {
            auto& lockingState = GetOrCrash(HunkStoreIdToLockingState_, hunkStoreId);
            YT_LOG_ALERT_IF(
                Context_->GetAutomatonState() == EPeerState::Leading && !lockingState.IsBeingUnlocked,
                "Hunk store is not marked as being unlocked (HunkStoreId: %v)",
                hunkStoreId);
            lockingState.IsBeingUnlocked = true;
        }
    }

    void OnBoggleLockAborted(THunkStoreId hunkStoreId, bool lock) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (lock) {
            auto error = TError("Hunk store lock aborted")
                << TErrorAttribute("hunk_store_id", hunkStoreId);
            SetHunkStoreLockingFuture(hunkStoreId, std::move(error));
        } else {
            auto it = HunkStoreIdToLockingState_.find(hunkStoreId);
            if (it == HunkStoreIdToLockingState_.end()) {
                YT_LOG_ALERT(
                    "Hunk store is lost during abort (HunkStoreId: %v)",
                    hunkStoreId);
                return;
            }

            if (Context_->GetAutomatonState() == EPeerState::Leading) {
                YT_LOG_ALERT_IF(
                    !it->second.IsBeingUnlocked,
                    "Hunk store was not marked as being unlocked during abort (HunkStoreId: %v)",
                    hunkStoreId);
                it->second.IsBeingUnlocked = false;
            }
        }
    }

    void BuildOrchid(TFluentAny fluent) const override
    {
        fluent
            .BeginMap()
                .Item("hunk_store_ids_being_locked").DoListFor(
                    HunkStoreIdsBeingLockedToPromise_,
                    [&] (auto fluent, const auto& hunkStoreIdToPromise) {
                        fluent.Item().Value(hunkStoreIdToPromise.first);
                    })
                .Item("locked_hunk_stores").DoMapFor(
                    HunkStoreIdToLockingState_,
                    [&] (auto fluent, const auto& idToLockingState) {
                        const auto& lockingState = idToLockingState.second;
                        fluent.Item("id").Value(idToLockingState.first);
                        fluent.Item("persistent_lock_count").Value(lockingState.PersistentLockCount);
                        fluent.Item("transient_lock_count").Value(lockingState.TransientLockCount);
                        fluent.Item("last_change_time").Value(lockingState.LastChangeTime);
                        fluent.Item("is_being_unlocked").Value(lockingState.IsBeingUnlocked);
                        fluent.Item("hunk_cell_id").Value(lockingState.HunkCellId);
                        fluent.Item("hunk_tablet_id").Value(lockingState.HunkTabletId);
                        fluent.Item("hunk_mount_revision").Value(lockingState.HunkMountRevision);
                    })
            .EndMap();
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    const TTabletId TabletId_;
    ITabletContext* const Context_;
    const NLogging::TLogger Logger;

    TAtomicObject<TPeriodicExecutorPtr> UnlockExecutor_;

    THashMap<TChunkId, TPromise<void>> HunkStoreIdsBeingLockedToPromise_;
    THashMap<TChunkId, TLockingState> HunkStoreIdToLockingState_;

    void Save(TSaveContext& context) const override
    {
        using NYT::Save;

        Save(context, HunkStoreIdToLockingState_);
    }

    void Load(TLoadContext& context) override
    {
        using NYT::Load;

        Load(context, HunkStoreIdToLockingState_);
    }

    void ClearTransientState()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& [hunkStoreId, promise] : HunkStoreIdsBeingLockedToPromise_) {
            promise.TrySet(TError("Epoch stopped"));
        }
        HunkStoreIdsBeingLockedToPromise_.clear();

        for (auto& [hunkStoreId, lockingState] : HunkStoreIdToLockingState_) {
            lockingState.TransientLockCount = 0;
            lockingState.IsBeingUnlocked = false;
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& config = newNodeConfig->TabletNode->HunkLockManager;
        if (auto unlockExecutor = UnlockExecutor_.Load()) {
            unlockExecutor->SetPeriod(config->UnlockCheckPeriod);
        }
    }

    void SetHunkStoreLockingFuture(THunkStoreId hunkStoreId, const TError& result)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto it = HunkStoreIdsBeingLockedToPromise_.find(hunkStoreId); it != HunkStoreIdsBeingLockedToPromise_.end()) {
            it->second.TrySet(result);
            HunkStoreIdsBeingLockedToPromise_.erase(it);
        } else {
            if (Context_->GetAutomatonState() == EPeerState::Leading) {
                YT_LOG_ALERT("Hunk store locking future is lost (HunkStoreId: %v)",
                    hunkStoreId);
            }
        }
    }

    void Touch(TLockingState& state)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Transient, now is OK.
        state.LastChangeTime = TInstant::Now();
    }

    TFuture<void> ToggleLock(
        TCellId hunkCellId,
        TTabletId hunkTabletId,
        THunkStoreId hunkStoreId,
        TRevision hunkMountRevision,
        bool lock)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto term = Context_->GetAutomatonTerm();
        auto transactionFuture = Context_->GetClient()->StartNativeTransaction(
            ETransactionType::Tablet,
            /*options*/ {});

        return transactionFuture
            .Apply(BIND([=, this] (const NNative::ITransactionPtr& transaction) {
                NTabletClient::NProto::TReqToggleHunkTabletStoreLock hunkRequest;
                ToProto(hunkRequest.mutable_tablet_id(), hunkTabletId);
                ToProto(hunkRequest.mutable_store_id(), hunkStoreId);
                ToProto(hunkRequest.mutable_locker_tablet_id(), TabletId_);
                hunkRequest.set_lock(lock);
                hunkRequest.set_mount_revision(hunkMountRevision);
                transaction->AddAction(hunkCellId, MakeTransactionActionData(hunkRequest));

                NTabletClient::NProto::TReqBoggleHunkTabletStoreLock localRequest;
                ToProto(localRequest.mutable_hunk_tablet_id(), hunkTabletId);
                ToProto(localRequest.mutable_store_id(), hunkStoreId);
                ToProto(localRequest.mutable_tablet_id(), TabletId_);
                ToProto(localRequest.mutable_hunk_cell_id(), hunkCellId);
                localRequest.set_lock(lock);
                localRequest.set_mount_revision(hunkMountRevision);
                localRequest.set_term(term);
                transaction->AddAction(Context_->GetCellId(), MakeTransactionActionData(localRequest));

                NApi::TTransactionCommitOptions commitOptions{
                    .Force2PC = true,
                    .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late
                };

                return transaction->Commit(commitOptions)
                    .AsVoid();
            }));
    }

    void UnlockStaleHunkStores()
    {
        YT_LOG_DEBUG("Starting unlock check");

        auto now = TInstant::Now();
        const auto& config = GetDynamicConfig();
        // Can be null in tests.
        if (!config) {
            return;
        }

        for (auto& [hunkStoreId, lockingState] : HunkStoreIdToLockingState_) {
            if (lockingState.TransientLockCount + lockingState.PersistentLockCount == 0 &&
                now - lockingState.LastChangeTime >= config->HunkStoreExtraLifeTime &&
                !lockingState.IsBeingUnlocked)
            {
                YT_LOG_DEBUG("Unlocking hunk store (HunkStoreId: %v, HunkCellId: %v, HunkTabletId: %v)",
                    hunkStoreId,
                    lockingState.HunkCellId,
                    lockingState.HunkTabletId);

                lockingState.IsBeingUnlocked = true;
                ToggleLock(
                    lockingState.HunkCellId,
                    lockingState.HunkTabletId,
                    hunkStoreId,
                    lockingState.HunkMountRevision,
                    /*lock*/ false);
            } else {
                YT_LOG_DEBUG("Not unlocking hunk store (HunkStoreId: %v, HunkCellId: %v, HunkTabletId: %v)",
                    hunkStoreId,
                    lockingState.HunkCellId,
                    lockingState.HunkTabletId);
            }
        }
    }

    TTabletHunkLockManagerDynamicConfigPtr GetDynamicConfig() const
    {
        // For some tests.
        if (!Context_) {
            return nullptr;
        }

        const auto& dynamicConfigManager = Context_->GetDynamicConfigManager();
        return dynamicConfigManager->GetConfig()->TabletNode->HunkLockManager;
    }
};

////////////////////////////////////////////////////////////////////////////////

IHunkLockManagerPtr CreateHunkLockManager(
    ITabletContext* context,
    TTabletId tabletId)
{
    return New<THunkLockManager>(context, tabletId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
