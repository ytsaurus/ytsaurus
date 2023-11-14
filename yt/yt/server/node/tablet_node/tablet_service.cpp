#include "tablet_service.h"
#include "bootstrap.h"
#include "private.h"
#include "hunk_store.h"
#include "hunk_tablet_manager.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_cell_write_manager.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "tablet_snapshot_store.h"
#include "overload_controlling_service_base.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_service.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/actions/current_invoker.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NCompression;
using namespace NConcurrency;
using namespace NJournalClient;
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
    : public TOverloadControllingServiceBase<THydraServiceBase>
{
public:
    TTabletService(
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : TOverloadControllingServiceBase(
            bootstrap,
            slot->GetHydraManager(),
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Write),
            TTabletServiceProxy::GetDescriptor(),
            TabletNodeLogger,
            slot->GetCellId(),
            CreateHydraManagerUpstreamSynchronizer(slot->GetHydraManager()),
            bootstrap->GetNativeAuthenticator())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Slot_);
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterTransactionActions));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Trim));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendTabletCell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeTabletCell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteHunks));

        DeclareServerFeature(ETabletServiceFeatures::WriteGenerations);
        SubscribeLoadAdjusted();
    }

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Write)
    {
        ValidatePeer(EPeerKind::Leader);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto upstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        auto replicationEra = request->has_replication_era()
            ? std::make_optional(FromProto<TReplicationEra>(request->replication_era()))
            : std::nullopt;
        TTabletCellWriteParams params{
            .TransactionId = FromProto<TTransactionId>(request->transaction_id()),
            .TransactionStartTimestamp = request->transaction_start_timestamp(),
            .TransactionTimeout = FromProto<TDuration>(request->transaction_timeout()),
            .PrepareSignature = request->prepare_signature(),
            // COMPAT(gritukan)
            .CommitSignature = request->has_commit_signature() ? request->commit_signature() : request->prepare_signature(),
            .Generation = request->generation(),
            .RowCount = request->row_count(),
            .DataWeight = request->data_weight(),
            .Versioned = request->versioned(),
            .SyncReplicaIds = FromProto<TSyncReplicaIdList>(request->sync_replica_ids())
        };

        if (request->has_hunk_chunks_info()) {
            params.HunkChunksInfo = FromProto<THunkChunksInfo>(request->hunk_chunks_info());
        }

        ValidateTabletTransactionId(params.TransactionId);

        auto atomicity = AtomicityFromTransactionId(params.TransactionId);
        auto durability = CheckedEnumCast<EDurability>(request->durability());

        context->SetRequestInfo("TabletId: %v, TransactionId: %v, TransactionStartTimestamp: %v, "
            "TransactionTimeout: %v, Atomicity: %v, Durability: %v, PrepareSignature: %x, CommitSignature: %x, "
            "Generation: %x, RowCount: %v, DataWeight: %v, RequestCodec: %v, Versioned: %v, SyncReplicaIds: %v, "
            "UpstreamReplicaId: %v, ReplicationEra: %v",
            tabletId,
            params.TransactionId,
            params.TransactionStartTimestamp,
            params.TransactionTimeout,
            atomicity,
            durability,
            params.PrepareSignature,
            params.CommitSignature,
            params.Generation,
            params.RowCount,
            params.DataWeight,
            requestCodecId,
            params.Versioned,
            params.SyncReplicaIds,
            upstreamReplicaId,
            replicationEra);

        TServiceProfilerGuard profilerGuard;

        // NB: Must serve the whole request within a single epoch.
        auto epochInvoker = Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write);
        TCurrentInvokerGuard invokerGuard(epochInvoker.Get());

        auto tabletSnapshot = GetTabletSnapshotOrThrow(tabletId, mountRevision);
        Bootstrap_
            ->GetTabletSnapshotStore()
            ->ValidateBundleNotBanned(tabletSnapshot, Slot_);

        try {
            if (tabletSnapshot->Atomicity != atomicity) {
                THROW_ERROR_EXCEPTION("Invalid atomicity mode: %Qlv instead of %Qlv",
                    atomicity,
                    tabletSnapshot->Atomicity);
            }

            if (params.Versioned && context->GetAuthenticationIdentity().User != NSecurityClient::ReplicatorUserName) {
                THROW_ERROR_EXCEPTION("Versioned writes are only allowed for %Qv user",
                    NSecurityClient::ReplicatorUserName);
            }

            auto checkUpstreamReplicaId = params.Versioned || tabletSnapshot->UpstreamReplicaId;

            if (checkUpstreamReplicaId) {
                if (upstreamReplicaId && !tabletSnapshot->UpstreamReplicaId) {
                    THROW_ERROR_EXCEPTION("Table is not bound to any upstream replica but replica %v was given",
                        upstreamReplicaId);
                } else if (!upstreamReplicaId && tabletSnapshot->UpstreamReplicaId) {
                    THROW_ERROR_EXCEPTION("Table is bound to upstream replica %v; direct modifications are forbidden",
                        tabletSnapshot->UpstreamReplicaId);
                } else if (upstreamReplicaId != tabletSnapshot->UpstreamReplicaId) {
                    THROW_ERROR_EXCEPTION(
                        NTabletClient::EErrorCode::UpstreamReplicaMismatch,
                        "Mismatched upstream replica: expected %v, got %v",
                        tabletSnapshot->UpstreamReplicaId,
                        upstreamReplicaId);
                }
            }

            auto era = tabletSnapshot->TabletRuntimeData->ReplicationEra.load();
            if (replicationEra && *replicationEra != era) {
                if (era == InvalidReplicationEra) {
                    THROW_ERROR_EXCEPTION("Direct write is not allowed: replica is identifying replication era");
                }
                THROW_ERROR_EXCEPTION("Replication era mismatch: expected %v, got %v",
                    era,
                    *replicationEra);
            }

            auto writeMode = tabletSnapshot->TabletRuntimeData->WriteMode.load();
            if (writeMode != ETabletWriteMode::Direct &&
                context->GetAuthenticationIdentity().User != NSecurityClient::ReplicatorUserName)
            {
                THROW_ERROR_EXCEPTION("Direct write is not allowed: replica is probably catching up")
                    << TErrorAttribute("upstream_replica_id", tabletSnapshot->UpstreamReplicaId);
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
            if (writeThrottler && !writeThrottler->TryAcquire(params.DataWeight)) {
                tabletSnapshot->TableProfiler->GetThrottlerCounter(throttlerKind)
                    ->Increment();

                THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::RequestThrottled,
                    "Write to tablet %v is throttled",
                    tabletId)
                    << TErrorAttribute("throttler_kind", throttlerKind)
                    << TErrorAttribute("queue_total_count", writeThrottler->GetQueueTotalAmount());
            }
        } catch (const std::exception& ex) {
            THROW_ERROR ex
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("table_path", tabletSnapshot->TablePath);
        }

        auto* counters = tabletSnapshot->TableProfiler->GetTabletServiceCounters(GetCurrentProfilingUser());
        profilerGuard.Start(counters->Write);

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto requestData = requestCodec->Decompress(request->Attachments()[0]);
        struct TWriteBufferTag { };
        auto reader = CreateWireProtocolReader(requestData, New<TRowBuffer>(TWriteBufferTag()));

        TFuture<void> commitResult;
        try {
            const auto& tabletCellWriteManager = Slot_->GetTabletCellWriteManager();
            commitResult = tabletCellWriteManager->Write(
                tabletSnapshot,
                reader.get(),
                params);
        } catch (const std::exception&) {
            tabletSnapshot->PerformanceCounters->WriteError.Counter.fetch_add(1, std::memory_order::relaxed);
            throw;
        }

        commitResult.Subscribe(BIND([profilerGuard = std::move(profilerGuard)] (const TError& /*error*/) {}));

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

        context->SetRequestInfo("TransactionId: %v, TransactionStartTimestamp: %v, TransactionTimeout: %v, "
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
        Bootstrap_
            ->GetTabletSnapshotStore()
            ->ValidateBundleNotBanned(tabletSnapshot, Slot_);

        const auto& tabletManager = Slot_->GetTabletManager();
        auto future = tabletManager->Trim(tabletSnapshot, trimmedRowCount);

        context->ReplyFrom(std::move(future));
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, SuspendTabletCell)
    {
        ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo();

        const auto& hydraManager = Slot_->GetHydraManager();
        auto mutation = CreateMutation(hydraManager, NTabletServer::NProto::TReqSuspendTabletCell());
        mutation->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, ResumeTabletCell)
    {
        ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo();

        const auto& hydraManager = Slot_->GetHydraManager();
        auto mutation = CreateMutation(hydraManager, NTabletServer::NProto::TReqResumeTabletCell());
        mutation->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, WriteHunks)
    {
        ValidatePeer(EPeerKind::Leader);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        const auto& payloads = request->Attachments();

        context->SetRequestInfo("TabletId: %v, MountRevision: %v, HunkCount: %v",
            tabletId,
            mountRevision,
            payloads.size());

        const auto& tabletManager = Slot_->GetHunkTabletManager();
        auto* tablet = tabletManager->GetTabletOrThrow(tabletId);
        tablet->ValidateMounted(mountRevision);

        tablet->WriteHunks(payloads)
            .Apply(BIND([=] (const TErrorOr<std::vector<TJournalHunkDescriptor>>& descriptorsOrError) {
                if (!descriptorsOrError.IsOK()) {
                    context->Reply(descriptorsOrError);
                    return;
                }

                const auto& descriptors = descriptorsOrError.Value();
                for (const auto& descriptor : descriptors) {
                    auto* protoDescriptor = response->add_descriptors();
                    ToProto(protoDescriptor->mutable_chunk_id(), descriptor.ChunkId);
                    protoDescriptor->set_record_index(descriptor.RecordIndex);
                    protoDescriptor->set_record_offset(descriptor.RecordOffset);
                    protoDescriptor->set_length(descriptor.Size);
                }
                context->Reply();
            }));
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
};

IServicePtr CreateTabletService(ITabletSlotPtr slot, IBootstrap* bootstrap)
{
    return New<TTabletService>(slot, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
