#include "store_trimmer.h"

#include "bootstrap.h"
#include "config.h"
#include "ordered_chunk_store.h"
#include "replication_log.h"
#include "slot_manager.h"
#include "store.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NLogging;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NTabletClient;
using namespace NChunkClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosDataTrimProgressGuard
    : public TMoveOnly
{
public:
    TChaosDataTrimProgressGuard(
        TChaosTabletDataPtr chaosTabletData,
        TCancelableContextPtr tabletCancelableContext,
        const TLogger& logger)
        : Logger(logger.WithTag("TrimGuardId: %v", TGuid::Create()))
        , TabletCancelableContext_(std::move(tabletCancelableContext))
        , ChaosTabletData_(std::move(chaosTabletData))
    {
        if (ChaosTabletData_) {
            ChaosTabletData_->IsTrimInProgress.store(true);
        }
    }

    TChaosDataTrimProgressGuard(TChaosDataTrimProgressGuard&& guard)
        : Logger(std::move(guard.Logger))
        , TabletCancelableContext_(std::move(guard.TabletCancelableContext_))
        , ChaosTabletData_(std::move(guard.ChaosTabletData_))
    { }

    ~TChaosDataTrimProgressGuard()
    {
        if (!ChaosTabletData_) {
            return;
        }

        if (TabletCancelableContext_ && TabletCancelableContext_->IsCanceled()) {
            YT_LOG_DEBUG("Tablet epoch changed, do nothing");
            return;
        }

        if (!ChaosTabletData_->IsTrimInProgress.load()) {
            YT_LOG_ALERT("Chaos data trimming progress flag was reset unexpectedly");
            return;
        }

        ChaosTabletData_->IsTrimInProgress.store(false);

        YT_LOG_DEBUG("Replication log trimming finished without actually trimming");
    }

    void Release()
    {
        ChaosTabletData_.Reset();
    }

private:
    const TLogger Logger;
    const TCancelableContextPtr TabletCancelableContext_;

    TChaosTabletDataPtr ChaosTabletData_;
};

////////////////////////////////////////////////////////////////////////////////
class TStoreTrimmer
    : public IStoreTrimmer
{
public:
    explicit TStoreTrimmer(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TStoreTrimmer::OnScanSlot, MakeStrong(this)));
    }

private:
    IBootstrap* const Bootstrap_;

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->StoreTrimmer;
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

        if (tablet->IsPhysicallySorted()) {
            return;
        }

        const auto& movementData = tablet->SmoothMovementData();
        if (!movementData.IsTabletStoresUpdateAllowed(/*isCommonFlush*/ false)) {
            return;
        }

        RequestStoreTrim(slot, tablet);

        auto stores = PickStoresForTrim(tablet);
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
        const ITabletSlotPtr& slot,
        TTablet* tablet)
    {
        if (tablet->IsPhysicallyLog()) {
            return RequestReplicationLogStoreTrim(slot, tablet);
        }

        RequestOrderedStoreTrim(slot, tablet);
    }

    void RequestOrderedStoreTrim(
        const ITabletSlotPtr& slot,
        TTablet* tablet)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;

        if (mountConfig->MinDataVersions != 0) {
            return;
        }

        auto dataTtl = mountConfig->MaxDataVersions == 0
            ? mountConfig->MinDataTtl
            : std::max(mountConfig->MinDataTtl, mountConfig->MaxDataTtl);

        auto minRowCount = mountConfig->RowCountToKeep;

        auto latestTimestamp = slot->GetLatestTimestamp();
        auto now = TimestampToInstant(latestTimestamp).first;
        auto deathTimestamp = InstantToTimestamp(now - dataTtl).first;

        if (tablet->GetReplicationCardId()) {
            deathTimestamp = std::min(deathTimestamp, tablet->GetOrderedChaosReplicationMinTimestamp());
        }

        i64 trimmedRowCount = 0;
        i64 remainingRowCount = tablet->GetTotalRowCount() - tablet->GetTrimmedRowCount();
        std::vector<TOrderedChunkStorePtr> result;
        for (const auto& [_, store] : tablet->StoreRowIndexMap()) {
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

        auto logger = TabletNodeLogger()
            .WithTag("%v", tablet->GetLoggingTag());
        auto finallyGuard = TChaosDataTrimProgressGuard(
            nullptr,
            tablet->GetCancelableContext(),
            logger);
        CommitTrimRowsMutation(
            std::move(slot),
            tablet->GetId(),
            trimmedRowCount,
            std::move(finallyGuard),
            std::move(logger));
    }

    void RequestReplicationLogStoreTrim(
        const ITabletSlotPtr& slot,
        TTablet* tablet)
    {
        if (tablet->IsReplicated()) {
            return;
        }

        auto replicationCard = tablet->RuntimeData()->ReplicationCard.Acquire();
        if (!replicationCard) {
            return;
        }

        i64 remainingRowCount = tablet->GetTotalRowCount() - tablet->GetTrimmedRowCount();
        if (remainingRowCount == 0) {
            return;
        }

        auto tabletId = tablet->GetId();
        auto tabletChaosData = tablet->ChaosData();

        auto Logger = TabletNodeLogger().WithTag("%v", tablet->GetLoggingTag());

        if (tabletChaosData->IsTrimInProgress.load()) {
            YT_LOG_DEBUG("Skipping replication log trimming because previous iteration is not finished yet");
            return;
        }

        auto minTimestamp = MaxTimestamp;
        auto lower = tablet->GetPivotKey().Get();
        auto upper = tablet->GetNextPivotKey().Get();

        for (const auto& [replicaId, replica] : replicationCard->Replicas) {
            auto replicaMinTimestamp = GetReplicationProgressMinTimestamp(
                replica.ReplicationProgress,
                lower,
                upper);
            minTimestamp = std::min(minTimestamp, replicaMinTimestamp);
        }

        auto finallyGuard = TChaosDataTrimProgressGuard(
            std::move(tabletChaosData),
            tablet->GetCancelableContext(),
            Logger);

        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosReplicationLogStoreTrim"));

        YT_LOG_DEBUG("Replication log trimming started");

        auto startRowIndexFuture = BIND(
            &TStoreTrimmer::ComputeReplicationLogTrimRowIndex,
            MakeStrong(this),
            tabletId,
            tablet->GetMountRevision(),
            minTimestamp,
            Logger)
            .AsyncVia(Bootstrap_->GetTableReplicatorPoolInvoker())
            .Run();

        auto tabletInvoker = tablet->GetEpochAutomatonInvoker();
        startRowIndexFuture.SubscribeUnique(
            BIND([
                tabletId,
                slot = std::move(slot),
                tabletInvoker = std::move(tabletInvoker),
                finallyGuard = std::move(finallyGuard),
                Logger
            ] (TErrorOr<std::optional<i64>>&& errorOrStartRowIndex) mutable {
                if (!errorOrStartRowIndex.IsOK()) {
                    YT_LOG_DEBUG(errorOrStartRowIndex,
                        "Skipping replication log trimming due to start index calculation error");
                    return;
                }

                if (!errorOrStartRowIndex.Value()) {
                    YT_LOG_DEBUG("Skipping replication log trimming due to start index calculated to null");
                    return;
                }

                tabletInvoker->Invoke(BIND([
                    tabletId,
                    slot = std::move(slot),
                    startRowIndex = *errorOrStartRowIndex.Value(),
                    finallyGuard = std::move(finallyGuard),
                    logger = Logger
                ] () mutable {
                    TStoreTrimmer::CommitTrimRowsMutation(
                        std::move(slot),
                        tabletId,
                        startRowIndex,
                        std::move(finallyGuard),
                        std::move(logger));
                }));
            }));
    }

    std::optional<i64> ComputeReplicationLogTrimRowIndex(
        TTabletId tabletId,
        TRevision mountRevision,
        TTimestamp trimTimestamp,
        TLogger Logger)
    {
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        auto tabletSnapshot = snapshotStore->FindLatestTabletSnapshot(tabletId);
        if (!tabletSnapshot) {
            return std::nullopt;
        }

        if (tabletSnapshot->MountRevision != mountRevision) {
            YT_LOG_DEBUG("Skipping replication log trim iteration due to mount revision mismatch "
                "(RequestedMountRevision: %v, CurrentMountRevision: %v)",
                mountRevision,
                tabletSnapshot->MountRevision);
            return std::nullopt;
        }

        try {
            TClientChunkReadOptions chunkReadOptions{
                .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletReplication),
                .ReadSessionId = TReadSessionId::Create(),
            };
            auto logParser = CreateReplicationLogParser(
                tabletSnapshot->TableSchema,
                tabletSnapshot->PhysicalSchema,
                tabletSnapshot->Settings.MountConfig,
                EWorkloadCategory::SystemTabletReplication,
                Logger());
            auto startRowIndex = logParser->ComputeStartRowIndex(
                tabletSnapshot,
                trimTimestamp,
                chunkReadOptions);

            YT_LOG_DEBUG("Computed replication log trim row count (TrimTimestamp: %v, TrimRowCount: %v)",
                trimTimestamp,
                startRowIndex);

            return startRowIndex;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error computing replication log trim row count");
            return std::nullopt;
        }
    }

    static void CommitTrimRowsMutation(
        ITabletSlotPtr slot,
        TTabletId tabletId,
        i64 trimmedRowCount,
        TChaosDataTrimProgressGuard&& finallyGuard,
        TLogger Logger)
    {
        auto* tablet = slot->GetTabletManager()->FindTablet(tabletId);
        if (!tablet) {
            YT_LOG_ALERT("Tablet not found while we are in tablet automaton invoker");
            return;
        }

        if (trimmedRowCount <= tablet->GetTrimmedRowCount()) {
            return;
        }

        NProto::TReqTrimRows hydraRequest;
        ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
        hydraRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
        hydraRequest.set_trimmed_row_count(trimmedRowCount);
        auto mutation = CreateMutation(slot->GetHydraManager(), hydraRequest);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
        finallyGuard.Release();
    }

    void TrimStores(
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        const std::vector<TOrderedChunkStorePtr>& stores)
    {
        auto tabletId = tablet->GetId();

        auto Logger = TabletNodeLogger()
            .WithTag("%v", tablet->GetLoggingTag());

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

                NApi::TTransactionStartOptions transactionOptions;
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

                YT_LOG_INFO("Tablet trim transaction created (TransactionId: %v)",
                    transaction->GetId());

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            tablet->ThrottleTabletStoresUpdate(slot, Logger);

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }
            actionRequest.set_update_reason(ToProto(ETabletStoresUpdateReason::Trim));

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

            const auto& storeManager = tablet->GetStoreManager();
            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }
    }

    std::vector<TOrderedChunkStorePtr> PickStoresForTrim(TTablet* tablet)
    {
        i64 trimmedRowCount = tablet->GetTrimmedRowCount();
        std::vector<TOrderedChunkStorePtr> result;
        for (const auto& [_, store] : tablet->StoreRowIndexMap()) {
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

IStoreTrimmerPtr CreateStoreTrimmer(IBootstrap* bootstrap)
{
    return New<TStoreTrimmer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
