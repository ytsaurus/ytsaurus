#include "store_trimmer.h"
#include "store.h"
#include "ordered_chunk_store.h"
#include "config.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

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

        auto now = TimestampToInstant(Bootstrap_->GetLatestTimestamp()).first;
        auto deathTimestamp = InstantToTimestamp(now - dataTtl).first;

        i64 trimmedRowCount = 0;
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
        const auto& tabletId = tablet->GetId();
        const auto& storeManager = tablet->GetStoreManager();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v", tablet->GetId());

        try {
            LOG_INFO("Trimming tablet stores (StoreIds: %v)",
                MakeFormattableRange(stores, TStoreIdFormatter()));

            NNative::ITransactionPtr transaction;
            {
                LOG_INFO("Creating tablet trim transaction");

                TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Tablet trim: table %v, tablet %v",
                    tablet->GetTablePath(),
                    tabletId));
                options.Attributes = std::move(attributes);

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                LOG_INFO("Tablet trim transaction created (TransactionId: %v)",
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

            auto actionData = MakeTransactionActionData(actionRequest);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            // NB: There's no need to call EndStoreCompaction since these stores are gone.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error trimming tablet stores");

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
