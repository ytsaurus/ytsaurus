#include "hunk_chunk_sweeper.h"

#include "bootstrap.h"
#include "config.h"
#include "global_stores_update_throttler.h"
#include "hunk_chunk.h"
#include "private.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/iterator/zip.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <util/random/shuffle.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NTabletClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class THunkChunkSweeper
    : public IHunkChunkSweeper
{
public:
    explicit THunkChunkSweeper(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , GlobalStoresUpdateThrottler_(Bootstrap_->GetGlobalStoresUpdateThrottler())
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&THunkChunkSweeper::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&THunkChunkSweeper::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&THunkChunkSweeper::OnEndSlotScan, MakeStrong(this)));
    }

private:
    IBootstrap* const Bootstrap_;

    const TGlobalStoresUpdateThrottlerPtr GlobalStoresUpdateThrottler_;

    const NProfiling::TProfiler Profiler_ = TabletNodeProfiler()
        .WithPrefix("/hunk_chunk_sweeper")
        .WithSparse();
    const NProfiling::TCounter ScheduledHunkChunkCounter_ = Profiler_.Counter("/scheduled_hunk_chunk_count");
    const NProfiling::TCounter SweptHunkChunkCounter_ = Profiler_.Counter("/swept_hunk_chunk_count");

    struct TTask
    {
        ITabletSlotPtr Slot;
        TTabletId TabletId;
        IInvokerPtr EpochInvoker;
        std::vector<THunkChunkPtr> HunkChunks;
    };

    YT_DECLARE_SPIN_LOCK(TSpinLock, TaskSpinLock_);
    std::vector<TTask> Tasks_;

    std::atomic<bool> SchedulingInProgress_ = false;


    void OnBeginSlotScan()
    {
        auto guard = Guard(TaskSpinLock_);
        Tasks_.clear();
    }

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        auto dynamicConfig = Bootstrap_->GetTabletNodeDynamicConfig()->HunkChunkSweeper;
        if (!dynamicConfig->Enable) {
            return;
        }

        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& tabletManager = slot->GetTabletManager();
        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            ScanTablet(slot, tablet);
        }
    }

    void OnEndSlotScan()
    {
        std::vector<TTask> tasks;
        {
            auto guard = Guard(TaskSpinLock_);
            tasks.swap(Tasks_);
        }

        if (tasks.empty()) {
            return;
        }

        if (SchedulingInProgress_.load()) {
            const auto& Logger = TabletNodeLogger;
            YT_TLOG_DEBUG("Tablet hunk chunks sweep scheduling is still in progress");
            return;
        }

        SchedulingInProgress_.store(true);

        Bootstrap_->GetControlInvoker()->Invoke(BIND(
            &THunkChunkSweeper::ScheduleTasks,
            MakeStrong(this),
            Passed(std::move(tasks))));
    }

    void ScheduleTasks(std::vector<TTask> tasks)
    {
        Shuffle(tasks.begin(), tasks.end());

        std::vector<TGlobalStoresUpdateThrottler::TRequest> requests;
        requests.reserve(tasks.size());
        for (const auto& task : tasks) {
            requests.push_back({
                .BundleName = task.Slot->GetTabletCellBundleName(),
                .CellTag = CellTagFromId(task.TabletId),
                .StoreCount = static_cast<int>(ssize(task.HunkChunks)),
            });
        }

        auto throttleResult = GlobalStoresUpdateThrottler_->Throttle(
            requests,
            ETabletStoresUpdateReason::Sweep);

        std::vector<TFuture<void>> futures;
        futures.reserve(tasks.size());
        for (auto&& [task, isThrottlerAcquired] : Zip(tasks, throttleResult)) {
            if (!isThrottlerAcquired) {
                continue;
            }

            ScheduledHunkChunkCounter_.Increment(ssize(task.HunkChunks));
            futures.push_back(BIND(
                &THunkChunkSweeper::SweepHunkChunks,
                MakeStrong(this),
                Passed(std::move(task.Slot)),
                task.TabletId,
                Passed(std::move(task.HunkChunks)))
                .AsyncVia(task.EpochInvoker)
                .Run());
        }

        AllSet(std::move(futures))
            .Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TError>>&) {
                SchedulingInProgress_.store(false);
            }));
    }

    void ScanTablet(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        auto hunkChunks = PickHunkChunksForSweep(tablet);
        if (hunkChunks.empty()) {
            return;
        }

        auto guard = Guard(TaskSpinLock_);
        Tasks_.push_back(TTask{
            .Slot = slot,
            .TabletId = tablet->GetId(),
            .EpochInvoker = tablet->GetEpochAutomatonInvoker(),
            .HunkChunks = std::move(hunkChunks),
        });
    }

    std::vector<THunkChunkPtr> PickHunkChunksForSweep(TTablet* tablet)
    {
        std::vector<THunkChunkPtr> result;
        result.reserve(tablet->DanglingHunkChunks().size());
        for (const auto& hunkChunk : tablet->DanglingHunkChunks()) {
            if (hunkChunk->GetSweepState() == EHunkChunkSweepState::None) {
                result.push_back(hunkChunk);
            }
        }
        return result;
    }

    void BeginHunkChunkSweep(const THunkChunkPtr& hunkChunk)
    {
        hunkChunk->SetSweepState(EHunkChunkSweepState::Running);
    }

    void EndHunkChunkSweep(const THunkChunkPtr& hunkChunk)
    {
        hunkChunk->SetSweepState(EHunkChunkSweepState::Complete);
    }

    void BackoffHunkChunkSweep(const THunkChunkPtr& hunkChunk)
    {
        hunkChunk->SetSweepState(EHunkChunkSweepState::None);
    }

    void SweepHunkChunks(
        ITabletSlotPtr slot,
        TTabletId tabletId,
        std::vector<THunkChunkPtr> hunkChunks)
    {
        auto Logger = TabletNodeLogger();

        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            YT_TLOG_DEBUG("Tablet is missing; aborting hunk chunks sweep")
                .With("TabletId", tabletId);
            return;
        }

        Logger.AddTags(tablet->GetLoggingTags());

        std::erase_if(hunkChunks, [] (const THunkChunkPtr& hunkChunk) {
            return hunkChunk->GetSweepState() != EHunkChunkSweepState::None;
        });

        if (hunkChunks.empty()) {
            YT_TLOG_DEBUG("No hunk chunks left to sweep; aborting hunk chunks sweep");
            return;
        }

        for (const auto& hunkChunk : hunkChunks) {
            BeginHunkChunkSweep(hunkChunk);
        }

        try {
            YT_TLOG_INFO("Sweeping tablet hunk chunks")
                .With("ChunkIds", MakeFormattableView(hunkChunks, THunkChunkIdFormatter()));

            NNative::ITransactionPtr transaction;
            {
                YT_TLOG_INFO("Creating tablet hunk chunks sweep transaction");

                auto transactionAttributes = CreateEphemeralAttributes();
                transactionAttributes->Set("title", Format("Tablet hunk chunks sweep: table %v, tablet %v",
                    tablet->GetTablePath(),
                    tabletId));

                TTransactionStartOptions transactionOptions;
                transactionOptions.AutoAbort = false;
                transactionOptions.Attributes = std::move(transactionAttributes);
                transactionOptions.CoordinatorMasterCellTag = CellTagFromId(tablet->GetId());
                transactionOptions.ReplicateToMasterCellTags = TCellTagList();
                transactionOptions.StartCypressTransaction = false;
                auto asyncTransaction = Bootstrap_->GetClient()->StartNativeTransaction(
                    NTransactionClient::ETransactionType::Master,
                    transactionOptions);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                YT_TLOG_INFO("Tablet hunk chunks sweep transaction created")
                    .With("TransactionId", transaction->GetId());

                Logger.AddTag("TransactionId", transaction->GetId());
            }

            tablet->ThrottleTabletStoresUpdate(slot, Logger);

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
            for (const auto& hunkChunk : hunkChunks) {
                auto* descriptor = actionRequest.add_hunk_chunks_to_remove();
                ToProto(descriptor->mutable_chunk_id(), hunkChunk->GetId());
            }
            actionRequest.set_update_reason(ToProto(ETabletStoresUpdateReason::Sweep));

            auto actionData = MakeTransactionActionData(actionRequest);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            for (const auto& hunkChunk : hunkChunks) {
                EndHunkChunkSweep(hunkChunk);
            }

            SweptHunkChunkCounter_.Increment(hunkChunks.size());

            tablet->RuntimeData()->Errors.BackgroundErrors[ETabletBackgroundActivity::HunkChunkSweep]
                .Store(TError());
        } catch (const std::exception& ex) {
            std::vector<TChunkId> remainingHunkChunkIds;
            for (const auto& hunkChunk : hunkChunks) {
                BackoffHunkChunkSweep(hunkChunk);

                // A table overwrite may have removed all selected hunks while the sweep was running.
                if (tablet->FindHunkChunk(hunkChunk->GetId()) == hunkChunk) {
                    remainingHunkChunkIds.push_back(hunkChunk->GetId());
                }
            }

            if (remainingHunkChunkIds.empty()) {
                YT_TLOG_DEBUG("Hunk chunk sweep failed after the selected chunks were removed")
                    .With("HunkChunkCount", hunkChunks.size())
                    .With(ex);
                return;
            }

            static constexpr auto Message = "Error sweeping tablet hunk chunks"_sb;
            auto innerError = TError(ex)
                .With("hunk_chunk_ids", remainingHunkChunkIds);

            YT_TLOG_WARNING(Message)
                .With(innerError);

            auto error = TError(Message)
                .With("tablet_id", tabletId)
                .With("background_activity", ETabletBackgroundActivity::HunkChunkSweep)
                .With(std::move(innerError));

            tablet->RuntimeData()->Errors.BackgroundErrors[ETabletBackgroundActivity::HunkChunkSweep]
                .Store(std::move(error));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IHunkChunkSweeperPtr CreateHunkChunkSweeper(IBootstrap* bootstrap)
{
    return New<THunkChunkSweeper>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
