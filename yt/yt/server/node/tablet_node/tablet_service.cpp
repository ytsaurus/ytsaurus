#include "tablet_service.h"
#include "bootstrap.h"
#include "private.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_write_manager.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NCompression;
using namespace NConcurrency;
using namespace NHydra;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public THydraServiceBase
{
public:
    TTabletService(
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : THydraServiceBase(
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Write),
            TTabletServiceProxy::GetDescriptor(),
            TabletNodeLogger,
            slot->GetCellId())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Slot_);
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterTransactionActions));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Trim));

        DeclareServerFeature(ETabletServiceFeatures::WriteGenerations);
    }

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Write)
    {
        ValidatePeer(EPeerKind::Leader);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();
        auto generation = request->generation();
        auto rowCount = request->row_count();
        auto dataWeight = request->data_weight();
        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto versioned = request->versioned();
        auto syncReplicaIds = FromProto<TSyncReplicaIdList>(request->sync_replica_ids());
        auto upstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        auto replicationEra = request->has_replication_era()
            ? std::make_optional(FromProto<TReplicationEra>(request->replication_era()))
            : std::nullopt;

        ValidateTabletTransactionId(transactionId);

        auto atomicity = AtomicityFromTransactionId(transactionId);
        auto durability = CheckedEnumCast<EDurability>(request->durability());

        context->SetRequestInfo("TabletId: %v, TransactionId: %v, TransactionStartTimestamp: %llx, "
            "TransactionTimeout: %v, Atomicity: %v, Durability: %v, Signature: %x, Generation: %x, RowCount: %v, DataWeight: %v, "
            "RequestCodec: %v, Versioned: %v, SyncReplicaIds: %v, UpstreamReplicaId: %v, ReplicationEra: %v",
            tabletId,
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            atomicity,
            durability,
            signature,
            generation,
            rowCount,
            dataWeight,
            requestCodecId,
            versioned,
            syncReplicaIds,
            upstreamReplicaId,
            replicationEra);

        // NB: Must serve the whole request within a single epoch.
        TCurrentInvokerGuard invokerGuard(Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write));

        auto tabletSnapshot = GetTabletSnapshotOrThrow(tabletId, mountRevision);

        if (tabletSnapshot->Atomicity != atomicity) {
            THROW_ERROR_EXCEPTION("Invalid atomicity mode: %Qlv instead of %Qlv",
                atomicity,
                tabletSnapshot->Atomicity);
        }

        if (versioned && context->GetAuthenticationIdentity().User != NSecurityClient::ReplicatorUserName) {
            THROW_ERROR_EXCEPTION("Versioned writes are only allowed for %Qv user",
                NSecurityClient::ReplicatorUserName);
        }

        auto checkUpstreamReplicaId = versioned || tabletSnapshot->UpstreamReplicaId;

        if (checkUpstreamReplicaId) {
            if (upstreamReplicaId && !tabletSnapshot->UpstreamReplicaId) {
                THROW_ERROR_EXCEPTION("Table is not bound to any upstream replica but replica %v was given",
                    upstreamReplicaId);
            } else if (!upstreamReplicaId && tabletSnapshot->UpstreamReplicaId) {
                THROW_ERROR_EXCEPTION("Table is bound to upstream replica %v; direct modifications are forbidden",
                    tabletSnapshot->UpstreamReplicaId);
            } else if (upstreamReplicaId != tabletSnapshot->UpstreamReplicaId) {
                THROW_ERROR_EXCEPTION("Mismatched upstream replica: expected %v, got %v",
                    tabletSnapshot->UpstreamReplicaId,
                    upstreamReplicaId);
            }
        }

        if (auto era = tabletSnapshot->TabletRuntimeData->ReplicationEra.load(); replicationEra && *replicationEra != era) {
            THROW_ERROR_EXCEPTION("Replication era mismatch: expected %v, got %v",
                *replicationEra,
                era);
        }

        const auto& resourceLimitsManager = Bootstrap_->GetResourceLimitsManager();
        const auto& tabletCellBundleName = Slot_->GetTabletCellBundleName();

        {
            auto* counters = tabletSnapshot->TableProfiler->GetWriteCounters(GetCurrentProfilingUser());
            NProfiling::TEventTimerGuard timerGuard(counters->ValidateResourceWallTime);

            resourceLimitsManager->ValidateResourceLimits(
                tabletSnapshot->Settings.StoreWriterOptions->Account,
                tabletSnapshot->Settings.StoreWriterOptions->MediumName,
                tabletCellBundleName,
                tabletSnapshot->Settings.MountConfig->InMemoryMode);

            resourceLimitsManager->ValidateResourceLimits(
                tabletSnapshot->Settings.HunkWriterOptions->Account,
                tabletSnapshot->Settings.HunkWriterOptions->MediumName,
                tabletCellBundleName,
                EInMemoryMode::None);
        }

        auto slotOptions = Slot_->GetOptions();
        resourceLimitsManager->ValidateResourceLimits(
            slotOptions->ChangelogAccount,
            slotOptions->ChangelogPrimaryMedium);
        resourceLimitsManager->ValidateResourceLimits(
            slotOptions->SnapshotAccount,
            slotOptions->SnapshotPrimaryMedium);

        auto throttlerKind = ETabletDistributedThrottlerKind::Write;
        const auto& writeThrottler = tabletSnapshot->DistributedThrottlers[throttlerKind];
        if (writeThrottler && !writeThrottler->TryAcquire(dataWeight)) {
            tabletSnapshot->TableProfiler->GetThrottlerCounter(throttlerKind)
                ->Increment();

            THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::RequestThrottled,
                "%v to tablet %v is throttled",
                throttlerKind,
                tabletId)
                << TErrorAttribute("queue_total_count", writeThrottler->GetQueueTotalCount());
        }

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto requestData = requestCodec->Decompress(request->Attachments()[0]);
        struct TWriteBufferTag { };
        TWireProtocolReader reader(requestData, New<TRowBuffer>(TWriteBufferTag()));

        const auto& tabletWriteManager = Slot_->GetTabletWriteManager();

        TFuture<void> commitResult;
        try {
            while (!reader.IsFinished()) {
                // Due to possible row blocking, serving the request may involve a number of write attempts.
                // Each attempt causes a mutation to be enqueued to Hydra.
                // Since all these mutations are enqueued within a single epoch, only the last commit outcome is
                // actually relevant.
                // Note that we're passing signature to every such call but only the last one actually uses it.
                tabletWriteManager->Write(
                    tabletSnapshot,
                    transactionId,
                    transactionStartTimestamp,
                    transactionTimeout,
                    signature,
                    generation,
                    rowCount,
                    dataWeight,
                    versioned,
                    syncReplicaIds,
                    &reader,
                    &commitResult);
            }
        } catch (const std::exception&) {
            ++tabletSnapshot->PerformanceCounters->WriteErrorCount;
            throw;
        }

        if (atomicity == EAtomicity::None && durability == EDurability::Sync) {
            context->ReplyFrom(commitResult);
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, RegisterTransactionActions)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();

        context->SetRequestInfo("TransactionId: %v, TransactionStartTimestamp: %llx, TransactionTimeout: %v, "
            "ActionCount: %v, Signature: %x",
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            request->actions_size(),
            signature);

        const auto& transactionManager = Slot_->GetTransactionManager();
        auto future = transactionManager->RegisterTransactionActions(
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            signature,
            std::move(*request->mutable_actions()));

        context->ReplyFrom(std::move(future));
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Trim)
    {
        ValidatePeer(EPeerKind::Leader);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto trimmedRowCount = request->trimmed_row_count();

        context->SetRequestInfo("TabletId: %v, TrimmedRowCount: %v",
            tabletId,
            trimmedRowCount);

        auto tabletSnapshot = GetTabletSnapshotOrThrow(tabletId, mountRevision);

        const auto& tabletManager = Slot_->GetTabletManager();
        auto future = tabletManager->Trim(tabletSnapshot, trimmedRowCount);

        context->ReplyFrom(std::move(future));
    }


    TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        NHydra::TRevision mountRevision)
    {
        auto cellId = Slot_->GetCellId();
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->GetTabletSnapshotOrThrow(tabletId, cellId, mountRevision);
        tabletSnapshot->ValidateCellId(cellId);
        return tabletSnapshot;
    }

    // THydraServiceBase overrides.
    IHydraManagerPtr GetHydraManager() override
    {
        return Slot_->GetHydraManager();
    }
};

IServicePtr CreateTabletService(ITabletSlotPtr slot, IBootstrap* bootstrap)
{
    return New<TTabletService>(slot, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
