#include "store_trimmer.h"
#include "store.h"
#include "ordered_chunk_store.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "private.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TStoreTrimmer
    : public TRefCounted
{
public:
    TStoreTrimmer(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TStoreTrimmer::OnScanSlot, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    void OnScanSlot(const TTabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }
        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(const TTabletSlotPtr& slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (tablet->IsPhysicallySorted()) {
            return;
        }

        RequestStoreTrim(slot, tablet);

        auto stores = PickStoresForTrimming(tablet);
        if (stores.empty()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        for (const auto& store : stores) {
            storeManager->BeginStoreCompaction(store);
        }

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreTrimmer::TrimStores,
            MakeStrong(this),
            slot,
            tablet,
            std::move(stores)));
    }

    void RequestStoreTrim(
        const TTabletSlotPtr& slot,
        TTablet* tablet)
    {
        if (tablet->IsReplicated()) {
            return;
        }

        const auto& config = tablet->GetConfig();

        if (config->MinDataVersions != 0) {
            return;
        }

        auto dataTtl = config->MaxDataVersions == 0
            ? config->MinDataTtl
            : std::max(config->MinDataTtl, config->MaxDataTtl);

        auto minRowCount = config->RowCountToKeep;

        auto now = TimestampToInstant(Bootstrap_->GetLatestTimestamp()).first;
        auto deathTimestamp = InstantToTimestamp(now - dataTtl).first;

        i64 trimmedRowCount = 0;
        i64 remainingRowCount = tablet->GetTotalRowCount() - tablet->GetTrimmedRowCount();
        std::vector<TOrderedChunkStorePtr> result;
        for (const auto& pair : tablet->StoreRowIndexMap()) {
            const auto& store = pair.second;
            if (!store->IsChunk()) {
                break;
            }
            auto chunkStore = store->AsOrderedChunk();
            if (chunkStore->GetMaxTimestamp() >= deathTimestamp) {
                break;
            }

            remainingRowCount -= chunkStore->GetRowCount();
            if (remainingRowCount < minRowCount) {
                break;
            }

            trimmedRowCount = chunkStore->GetStartingRowIndex() + chunkStore->GetRowCount();
        }

        if (trimmedRowCount > tablet->GetTrimmedRowCount()) {
            NProto::TReqTrimRows hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
            hydraRequest.set_mount_revision(tablet->GetMountRevision());
            hydraRequest.set_trimmed_row_count(trimmedRowCount);
            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->Commit();
        }
    }

    void TrimStores(
        const TTabletSlotPtr& slot,
        TTablet* tablet,
        const std::vector<TOrderedChunkStorePtr>& stores)
    {
        auto tabletId = tablet->GetId();
        const auto& storeManager = tablet->GetStoreManager();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("%v", tablet->GetLoggingId());

        try {
            YT_LOG_INFO("Trimming tablet stores (StoreIds: %v)",
                MakeFormattableView(stores, TStoreIdFormatter()));

            NNative::ITransactionPtr transaction;
            {
                YT_LOG_INFO("Creating tablet trim transaction");

                auto transactionAttributes = CreateEphemeralAttributes();
                transactionAttributes->Set("title", Format("Tablet trim: table %v, tablet %v",
                    tablet->GetTablePath(),
                    tabletId));

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                    NTransactionClient::ETransactionType::Master,
                    TTransactionStartOptions{
                        .AutoAbort = false,
                        .Attributes = std::move(transactionAttributes),
                        .CoordinatorMasterCellTag = CellTagFromId(tablet->GetId()),
                        .ReplicateToMasterCellTags = TCellTagList()
                    });
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                YT_LOG_INFO("Tablet trim transaction created (TransactionId: %v)",
                    transaction->GetId());

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(tablet->GetMountRevision());
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Trim));

            auto actionData = MakeTransactionActionData(actionRequest);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            // NB: There's no need to call EndStoreCompaction since these stores are gone.
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error trimming tablet stores");

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }
    }

    std::vector<TOrderedChunkStorePtr> PickStoresForTrimming(TTablet* tablet)
    {
        i64 trimmedRowCount = tablet->GetTrimmedRowCount();
        std::vector<TOrderedChunkStorePtr> result;
        for (const auto& pair : tablet->StoreRowIndexMap()) {
            const auto& store = pair.second;
            if (!store->IsChunk()) {
                break;
            }
            auto chunkStore = store->AsOrderedChunk();
            if (chunkStore->GetCompactionState() != EStoreCompactionState::None) {
                break;
            }
            if (chunkStore->GetStartingRowIndex() + chunkStore->GetRowCount() > trimmedRowCount) {
                break;
            }
            result.push_back(chunkStore);
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

void StartStoreTrimmer(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    if (config->EnableStoreTrimmer) {
        New<TStoreTrimmer>(config, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
