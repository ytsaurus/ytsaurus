#include "hunk_tablet_scanner.h"

#include "private.h"
#include "bootstrap.h"
#include "hunk_tablet.h"
#include "hunk_tablet_manager.h"
#include "hunk_store.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/action.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class THunkTabletScanner
    : public IHunkTabletScanner
{
public:
    THunkTabletScanner(
        IBootstrap* bootstrap,
        ITabletSlotPtr tabletSlot)
        : Bootstrap_(bootstrap)
        , TabletSlot_(MakeWeak(tabletSlot))
        , TabletCellId_(tabletSlot->GetCellId())
    { }

    void Scan(THunkTablet* tablet) override
    {
        auto tabletSlot = TabletSlot_.Lock();
        if (!tabletSlot) {
            return;
        }

        // Do not run concurrent scans.
        if (!tablet->TryLockScan()) {
            return;
        }
        auto unlockGuard = Finally([&] {
            tablet->UnlockScan();

            if (const auto& hunkTabletManager = tabletSlot->GetHunkTabletManager()) {
                // NB: May destroy tablet.
                hunkTabletManager->CheckFullyUnlocked(tablet);
            }
        });

        auto tabletId = tablet->GetId();

        YT_LOG_DEBUG("Scanning hunk tablet (TabletId: %v)",
            tabletId);

        try {
            MaybeAllocateStores(tablet);
            MaybeRotateActiveStore(tablet);
            MaybeMarkStoresAsSealable(tablet);
            MaybeRemoveStores(tablet);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to scan hunk tablet (TabletId: %v)", tabletId);
        }
    }

private:
    IBootstrap* const Bootstrap_;

    const TWeakPtr<ITabletSlot> TabletSlot_;
    TTabletCellId TabletCellId_;

    void MaybeAllocateStores(THunkTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        const auto& mountConfig = tablet->MountConfig();
        auto desiredAllocatedStoreCount = mountConfig->DesiredAllocatedStoreCount;

        int storesToAllocate = 0;
        auto allocatedStoreCount = std::ssize(tablet->AllocatedStores());
        if (allocatedStoreCount < desiredAllocatedStoreCount) {
            storesToAllocate += desiredAllocatedStoreCount - allocatedStoreCount;
        }

        if (!tablet->GetActiveStore()) {
            ++storesToAllocate;
        }

        DoAllocateStores(tablet, storesToAllocate);
    }

    void MaybeRotateActiveStore(THunkTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        auto Logger = TabletNodeLogger.WithTag("TabletId: %v", tablet->GetId());

        bool needToRotateActiveStore = false;

        auto activeStore = tablet->GetActiveStore();
        if (activeStore) {
            const auto& mountConfig = tablet->MountConfig();
            const auto& writerConfig = tablet->StoreWriterConfig();

            const auto& writer = activeStore->GetWriter();
            auto writerStatistics = writer->GetStatistics();
            if (writerStatistics.HunkCount >= writerConfig->DesiredHunkCountPerChunk) {
                YT_LOG_DEBUG("Rotating active store since there are too many hunks in chunk "
                    "(StoreId: %v, HunkCount: %v, DesiredHunkCount: %v)",
                    activeStore->GetId(),
                    writerStatistics.HunkCount,
                    writerConfig->DesiredHunkCountPerChunk);

                needToRotateActiveStore = true;
            } else if (writerStatistics.TotalSize >= writerConfig->DesiredChunkSize) {
                YT_LOG_DEBUG("Rotating active store since active store is too large "
                    "(StoreId: %v, StoreSize: %v, DesiredChunkSize: %v)",
                    activeStore->GetId(),
                    writerStatistics.TotalSize,
                    writerConfig->DesiredChunkSize);

                needToRotateActiveStore = true;
            } else if (activeStore->GetCreationTime() + mountConfig->StoreRotationPeriod < TInstant::Now()) {
                YT_LOG_DEBUG("Rotating active store since active store is too old "
                    "(StoreId: %v, StoreCreationInstant: %v, StoreRotationPeriod: %v)",
                    activeStore->GetId(),
                    activeStore->GetCreationTime(),
                    mountConfig->StoreRotationPeriod);

                needToRotateActiveStore = true;
            } else if (writer->IsCloseDemanded()) {
                YT_LOG_DEBUG("Rotating active store since writer close is demanded "
                    "(StoreId: %v)",
                    activeStore->GetId());

                needToRotateActiveStore = true;
            }
        } else {
            YT_LOG_DEBUG("Rotating active store since there is no active store");

            needToRotateActiveStore = true;
        }

        if (!needToRotateActiveStore) {
            return;
        }

        if (tablet->AllocatedStores().empty()) {
            YT_LOG_DEBUG("Cannot rotate active store since there are no allocated stores; allocating new store");

            DoAllocateStores(tablet, /*storeCount*/ 1);
        }

        // NB: Active store could have been changed during stores allocation.
        if (!tablet->GetActiveStore() || tablet->GetActiveStore() == activeStore) {
            tablet->RotateActiveStore();
        }
    }

    void MaybeMarkStoresAsSealable(THunkTablet* tablet)
    {
        std::vector<TStoreId> storeIdsToMarkSealable;
        for (const auto& store : tablet->PassiveStores()) {
            if (!store->GetMarkedSealable()) {
                YT_LOG_DEBUG("Passive store is not marked as sealable; marking "
                    "(TabletId: %v, StoreId: %v)",
                    tablet->GetId(),
                    store->GetId());

                storeIdsToMarkSealable.push_back(store->GetId());
            }
        }

        if (storeIdsToMarkSealable.empty()) {
            return;
        }

        auto actionRequest = MakeActionRequest(tablet);
        for (auto storeId : storeIdsToMarkSealable) {
            auto* storeToMarkSealable = actionRequest.add_stores_to_mark_sealable();
            ToProto(storeToMarkSealable->mutable_store_id(), storeId);
        }

        auto transaction = CreateTransaction(tablet);
        AddTransactionAction(transaction, actionRequest, tablet);

        CommitTransaction(transaction);
    }

    void MaybeRemoveStores(THunkTablet* tablet)
    {
        const auto& mountConfig = tablet->MountConfig();

        std::vector<TStoreId> storeIdsToRemove;
        auto state = tablet->GetState();
        for (const auto& store : tablet->PassiveStores()) {
            if (store->IsLocked()) {
                continue;
            }

            if (!store->GetMarkedSealable()) {
                continue;
            }

            if (store->GetLastWriteTime() + mountConfig->StoreRemovalGracePeriod > TInstant::Now() &&
                state == ETabletState::Mounted)
            {
                continue;
            }

            YT_LOG_DEBUG("Removing hunk store (TabletId: %v, StoreId: %v)",
                tablet->GetId(),
                store->GetId());

            storeIdsToRemove.push_back(store->GetId());
        }

        if (storeIdsToRemove.empty()) {
            return;
        }

        auto actionRequest = MakeActionRequest(tablet);
        for (auto storeId : storeIdsToRemove) {
            auto* storeToRemove = actionRequest.add_stores_to_remove();
            ToProto(storeToRemove->mutable_store_id(), storeId);
        }

        auto transaction = CreateTransaction(tablet);
        AddTransactionAction(transaction, actionRequest, tablet);

        CommitTransaction(transaction);
    }

    NApi::NNative::ITransactionPtr CreateTransaction(THunkTablet* tablet)
    {
        YT_LOG_DEBUG("Creating hunk tablet stores update transaction (TabletId: %v)",
            tablet->GetId());

        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", Format("Updating stores of tablet %v",
            tablet->GetId()));

        NApi::TTransactionStartOptions transactionOptions;
        transactionOptions.AutoAbort = false;
        transactionOptions.Attributes = std::move(transactionAttributes);
        transactionOptions.CoordinatorMasterCellTag = CellTagFromId(tablet->GetId());
        transactionOptions.ReplicateToMasterCellTags = TCellTagList();
        auto asyncTransaction = Bootstrap_->GetClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionOptions);
        auto transaction = WaitFor(asyncTransaction)
            .ValueOrThrow();

        YT_LOG_DEBUG("Hunk tablet stores update transaction created (TabletId: %v, TransactionId: %v)",
            tablet->GetId(),
            transaction->GetId());

        return transaction;
    }

    void CommitTransaction(const NApi::NNative::ITransactionPtr& transaction)
    {
        NApi::TTransactionCommitOptions commitOptions{
            .CoordinatorCommitMode = NApi::ETransactionCoordinatorCommitMode::Lazy,
            .GeneratePrepareTimestamp = false,
        };

        WaitFor(transaction->Commit(commitOptions))
            .ThrowOnError();
    }

    void DoAllocateStores(THunkTablet* tablet, int storeCount)
    {
        if (storeCount == 0) {
            return;
        }

        YT_LOG_DEBUG("Allocating stores for hunk tablet (TabletId: %v, StoreCount: %v)",
            tablet->GetId(),
            storeCount);

        auto transaction = CreateTransaction(tablet);
        auto chunkIds = DoCreateChunks(tablet, transaction->GetId(), storeCount);

        auto actionRequest = MakeActionRequest(tablet);
        for (auto chunkId : chunkIds) {
            auto* storeToAdd = actionRequest.add_stores_to_add();
            ToProto(storeToAdd->mutable_session_id(), chunkId);
        }

        AddTransactionAction(transaction, actionRequest, tablet);

        CommitTransaction(transaction);
    }

    std::vector<TSessionId> DoCreateChunks(
        THunkTablet* tablet,
        TTransactionId transactionId,
        int chunkCount)
    {
        auto tabletSlot = TabletSlot_.Lock();
        YT_VERIFY(tabletSlot);

        const auto& writerOptions = tablet->StoreWriterOptions();

        const auto& resourceLimitsManager = tabletSlot->GetResourceLimitsManager();
        resourceLimitsManager->ValidateResourceLimits(
            writerOptions->Account,
            writerOptions->MediumName);

        auto masterChannel = Bootstrap_->GetClient()->GetMasterChannelOrThrow(
            NApi::EMasterChannelKind::Leader,
            /*cellTag*/ CellTagFromId(tablet->GetId()));
        TChunkServiceProxy proxy(masterChannel);

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
        batchReq->set_suppress_upstream_sync(true);

        for (int index = 0; index < chunkCount; ++index) {
            auto* request = batchReq->add_create_chunk_subrequests();
            request->set_type(ToProto<int>(EObjectType::JournalChunk));
            request->set_account(writerOptions->Account);
            ToProto(request->mutable_transaction_id(), transactionId);
            if (writerOptions->ErasureCodec == NErasure::ECodec::None) {
                request->set_replication_factor(writerOptions->ReplicationFactor);
            } else {
                request->set_erasure_codec(ToProto<int>(writerOptions->ErasureCodec));
            }
            request->set_medium_name(writerOptions->MediumName);
            request->set_read_quorum(writerOptions->ReadQuorum);
            request->set_write_quorum(writerOptions->WriteQuorum);
            request->set_movable(true);
            request->set_vital(true);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error creating chunks");

        std::vector<TSessionId> chunkIds;
        chunkIds.reserve(chunkCount);

        const auto& batchRsp = batchRspOrError.Value();
        for (int index = 0; index < chunkCount; ++index) {
            const auto& rsp = batchRsp->create_chunk_subresponses(index);
            chunkIds.push_back(FromProto<TSessionId>(rsp.session_id()));
        }

        return chunkIds;
    }

    static NTabletServer::NProto::TReqUpdateHunkTabletStores MakeActionRequest(THunkTablet* tablet)
    {
        NTabletServer::NProto::TReqUpdateHunkTabletStores actionRequest;
        ToProto(actionRequest.mutable_tablet_id(), tablet->GetId());
        actionRequest.set_mount_revision(tablet->GetMountRevision());

        return actionRequest;
    }

    void AddTransactionAction(
        const NApi::NNative::ITransactionPtr& transaction,
        const NTabletServer::NProto::TReqUpdateHunkTabletStores& action,
        THunkTablet* tablet)
    {
        auto actionData = MakeTransactionActionData(action);
        auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));
        transaction->AddAction(masterCellId, actionData);
        transaction->AddAction(TabletCellId_, actionData);
    }
};

////////////////////////////////////////////////////////////////////////////////

IHunkTabletScannerPtr CreateHunkTabletScanner(
    IBootstrap* bootstrap,
    ITabletSlotPtr tabletSlot)
{
    return New<THunkTabletScanner>(bootstrap, std::move(tabletSlot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
