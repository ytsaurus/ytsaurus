#include "hunk_chunk_sweeper.h"
#include "bootstrap.h"
#include "hunk_chunk.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NTabletNode {

using namespace NApi;
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
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&THunkChunkSweeper::OnScanSlot, MakeStrong(this)));
    }

private:
    IBootstrap* const Bootstrap_;


    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->HunkChunkSweeper;
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

    void ScanTablet(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        auto hunkChunks = PickHunkChunksForSweep(tablet);
        if (hunkChunks.empty()) {
            return;
        }

        for (const auto& hunkChunk : hunkChunks) {
            BeginHunkChunkSweep(hunkChunk);
        }

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &THunkChunkSweeper::SweepHunkChunks,
            MakeStrong(this),
            slot,
            tablet,
            std::move(hunkChunks)));
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
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        const std::vector<THunkChunkPtr>& hunkChunks)
    {
        auto tabletId = tablet->GetId();

        auto Logger = TabletNodeLogger
            .WithTag("%v", tablet->GetLoggingTag());

        try {
            YT_LOG_INFO("Sweeping tablet hunk chunks (ChunkIds: %v)",
                MakeFormattableView(hunkChunks, THunkChunkIdFormatter()));

            NNative::ITransactionPtr transaction;
            {
                YT_LOG_INFO("Creating tablet hunk chunks sweep transaction");

                auto transactionAttributes = CreateEphemeralAttributes();
                transactionAttributes->Set("title", Format("Tablet hunk chunks sweep: table %v, tablet %v",
                    tablet->GetTablePath(),
                    tabletId));

                NApi::NNative::TNativeTransactionStartOptions transactionOptions;
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

                YT_LOG_INFO("Tablet hunk chunks sweep transaction created (TransactionId: %v)",
                    transaction->GetId());

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            tablet->ThrottleTabletStoresUpdate(slot, Logger);

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(tablet->GetMountRevision());
            for (const auto& hunkChunk : hunkChunks) {
                auto* descriptor = actionRequest.add_hunk_chunks_to_remove();
                ToProto(descriptor->mutable_chunk_id(), hunkChunk->GetId());
            }
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Sweep));

            auto actionData = MakeTransactionActionData(actionRequest);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            for (const auto& hunkChunk : hunkChunks) {
                EndHunkChunkSweep(hunkChunk);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error sweeping tablet hunk chunks");

            for (const auto& hunkChunk : hunkChunks) {
                BackoffHunkChunkSweep(hunkChunk);
            }
        }
    }
};

IHunkChunkSweeperPtr CreateHunkChunkSweeper(IBootstrap* bootstrap)
{
    return New<THunkChunkSweeper>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
