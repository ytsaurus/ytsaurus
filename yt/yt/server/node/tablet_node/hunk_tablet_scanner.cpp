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
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYTree;

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
    { }

    void Scan(THunkTablet* tablet) override
    {
        auto tabletSlot = TabletSlot_.Lock();
        if (!tabletSlot) {
            return;
        }

        TScanSession(Bootstrap_, tabletSlot, tablet).Run();
    }

private:
    IBootstrap* const Bootstrap_;
    const TWeakPtr<ITabletSlot> TabletSlot_;

    class TScanSession
    {
    public:
        TScanSession(
            IBootstrap* bootstrap,
            ITabletSlotPtr tabletSlot,
            THunkTablet* tablet)
            : Bootstrap_(bootstrap)
            , TabletSlot_(std::move(tabletSlot))
            , Tablet_(tablet)
            , Logger(TabletNodeLogger.WithTag("TabletId: %v", Tablet_->GetId()))
        { }

        void Run()
        {
            // Do not run concurrent scans.
            if (!Tablet_->TryLockScan()) {
                return;
            }

            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("HunkTabletScanner"));

            auto unlockGuard = Finally([&] {
                Tablet_->UnlockScan();

                if (const auto& hunkTabletManager = TabletSlot_->GetHunkTabletManager()) {
                    // NB: May destroy tablet.
                    hunkTabletManager->CheckFullyUnlocked(Tablet_);
                }
            });

            YT_LOG_DEBUG("Scanning hunk tablet");

            try {
                MaybeAllocateStores();
                MaybeRotateActiveStore();
                MaybeMarkStoresAsSealable();
                MaybeRemoveStores();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to scan hunk tablet")
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << ex;
                YT_LOG_ERROR(error);
            }
        }

    private:
        IBootstrap* const Bootstrap_;
        const ITabletSlotPtr TabletSlot_;
        THunkTablet* const Tablet_;

        const NLogging::TLogger Logger;

        void MaybeAllocateStores()
        {
            if (Tablet_->GetState() != ETabletState::Mounted) {
                return;
            }

            const auto& mountConfig = Tablet_->MountConfig();
            auto desiredAllocatedStoreCount = mountConfig->DesiredAllocatedStoreCount;

            int storesToAllocate = 0;
            auto allocatedStoreCount = std::ssize(Tablet_->AllocatedStores());
            if (allocatedStoreCount < desiredAllocatedStoreCount) {
                storesToAllocate += desiredAllocatedStoreCount - allocatedStoreCount;
            }

            if (!Tablet_->GetActiveStore()) {
                ++storesToAllocate;
            }

            AllocateStores(storesToAllocate);
        }

        void MaybeRotateActiveStore()
        {
            if (Tablet_->GetState() != ETabletState::Mounted) {
                return;
            }

            bool needToRotateActiveStore = false;

            auto activeStore = Tablet_->GetActiveStore();
            if (activeStore) {
                const auto& mountConfig = Tablet_->MountConfig();
                const auto& writerConfig = Tablet_->StoreWriterConfig();

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

            if (Tablet_->AllocatedStores().empty()) {
                YT_LOG_DEBUG("Cannot rotate active store since there are no allocated stores; allocating new store");

                AllocateStores(/*storeCount*/ 1);
            }

            // NB: Active store could have been changed during stores allocation.
            if (!Tablet_->GetActiveStore() || Tablet_->GetActiveStore() == activeStore) {
                Tablet_->RotateActiveStore();
            }
        }

        void MaybeMarkStoresAsSealable()
        {
            std::vector<TStoreId> storeIdsToMarkSealable;
            for (const auto& store : Tablet_->PassiveStores()) {
                if (!store->GetMarkedSealable()) {
                    YT_LOG_DEBUG("Passive store is not marked as sealable; marking (StoreId: %v)",
                        store->GetId());

                    storeIdsToMarkSealable.push_back(store->GetId());
                }
            }

            if (storeIdsToMarkSealable.empty()) {
                return;
            }

            auto actionRequest = MakeActionRequest();
            for (auto storeId : storeIdsToMarkSealable) {
                auto* storeToMarkSealable = actionRequest.add_stores_to_mark_sealable();
                ToProto(storeToMarkSealable->mutable_store_id(), storeId);
            }

            auto transaction = CreateTransaction();
            AddTransactionAction(transaction, actionRequest);
            CommitTransaction(transaction);
        }

        void MaybeRemoveStores()
        {
            const auto& mountConfig = Tablet_->MountConfig();

            std::vector<TStoreId> storeIdsToRemove;
            auto state = Tablet_->GetState();
            for (const auto& store : Tablet_->PassiveStores()) {
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

                YT_LOG_DEBUG("Removing hunk store (StoreId: %v)",
                    store->GetId());

                storeIdsToRemove.push_back(store->GetId());
            }

            if (storeIdsToRemove.empty()) {
                return;
            }

            auto actionRequest = MakeActionRequest();
            for (auto storeId : storeIdsToRemove) {
                auto* storeToRemove = actionRequest.add_stores_to_remove();
                ToProto(storeToRemove->mutable_store_id(), storeId);
            }

            auto transaction = CreateTransaction();
            AddTransactionAction(transaction, actionRequest);
            CommitTransaction(transaction);
        }

        NApi::NNative::ITransactionPtr CreateTransaction()
        {
            YT_LOG_DEBUG("Creating hunk tablet stores update transaction");

            auto transactionAttributes = CreateEphemeralAttributes();
            transactionAttributes->Set("title", Format("Updating stores of tablet %v",
                Tablet_->GetId()));

            NApi::TTransactionStartOptions transactionOptions;
            transactionOptions.AutoAbort = false;
            transactionOptions.Attributes = std::move(transactionAttributes);
            transactionOptions.CoordinatorMasterCellTag = CellTagFromId(Tablet_->GetId());
            transactionOptions.ReplicateToMasterCellTags = TCellTagList();
            transactionOptions.StartCypressTransaction = false;
            auto asyncTransaction = Bootstrap_->GetClient()->StartNativeTransaction(
                NTransactionClient::ETransactionType::Master,
                transactionOptions);
            auto transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            YT_LOG_DEBUG("Hunk tablet stores update transaction created (TransactionId: %v)",
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

        void AllocateStores(int storeCount)
        {
            try {
                DoAllocateStores(storeCount);
            } catch (const std::exception& ex) {
                auto error = TError("Hunk tablet scanner failed to allocated stores")
                    << TError(ex);
                Tablet_->OnStoreAllocationFailed(error);
                THROW_ERROR(error);
            }
        }

        void DoAllocateStores(int storeCount)
        {
            if (storeCount == 0) {
                return;
            }

            YT_LOG_DEBUG("Allocating stores for hunk tablet (StoreCount: %v)",
                storeCount);

            auto transaction = CreateTransaction();
            auto actionRequest = MakeActionRequest();
            auto chunkIds = DoCreateChunks(transaction->GetId(), storeCount);
            for (auto chunkId : chunkIds) {
                auto* storeToAdd = actionRequest.add_stores_to_add();
                ToProto(storeToAdd->mutable_session_id(), chunkId);
            }

            AddTransactionAction(transaction, actionRequest);
            CommitTransaction(transaction);
        }

        std::vector<TSessionId> DoCreateChunks(
            TTransactionId transactionId,
            int chunkCount)
        {
            const auto& writerOptions = Tablet_->StoreWriterOptions();

            const auto& resourceLimitsManager = TabletSlot_->GetResourceLimitsManager();
            resourceLimitsManager->ValidateResourceLimits(
                writerOptions->Account,
                writerOptions->MediumName);

            auto masterChannel = Bootstrap_->GetClient()->GetMasterChannelOrThrow(
                NApi::EMasterChannelKind::Leader,
                /*cellTag*/ CellTagFromId(Tablet_->GetId()));
            TChunkServiceProxy proxy(masterChannel);

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            SetSuppressUpstreamSync(&batchReq->Header(), true);
            // COMPAT(shakurov): prefer proto ext (above).
            batchReq->set_suppress_upstream_sync(true);

            for (int index = 0; index < chunkCount; ++index) {
                auto* request = batchReq->add_create_chunk_subrequests();
                auto chunkType = writerOptions->ErasureCodec == NErasure::ECodec::None
                    ? EObjectType::JournalChunk
                    : EObjectType::ErasureJournalChunk;
                request->set_type(ToProto<int>(chunkType));
                request->set_account(writerOptions->Account);
                ToProto(request->mutable_transaction_id(), transactionId);
                request->set_replication_factor(writerOptions->ReplicationFactor);
                request->set_erasure_codec(ToProto<int>(writerOptions->ErasureCodec));
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

        NTabletServer::NProto::TReqUpdateHunkTabletStores MakeActionRequest()
        {
            NTabletServer::NProto::TReqUpdateHunkTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), Tablet_->GetId());
            actionRequest.set_mount_revision(Tablet_->GetMountRevision());

            return actionRequest;
        }

        void AddTransactionAction(
            const NApi::NNative::ITransactionPtr& transaction,
            const NTabletServer::NProto::TReqUpdateHunkTabletStores& action)
        {
            auto actionData = MakeTransactionActionData(action);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(Tablet_->GetId()));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(TabletSlot_->GetCellId(), actionData);
        }
    };
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
