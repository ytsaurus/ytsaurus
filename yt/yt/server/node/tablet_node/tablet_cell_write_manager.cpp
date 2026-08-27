#include "tablet_cell_write_manager.h"

#include "automaton.h"
#include "config.h"
#include "hunk_lock_manager.h"
#include "mutation_forwarder.h"
#include "sorted_dynamic_store.h"
#include "store_manager.h"
#include "tablet.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/hydra/automaton.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/codicil.h>

#include <library/cpp/yt/compact_containers/compact_flat_map.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NCompression;
using namespace NHydra;
using namespace NLeaseServer;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellWriteManager
    : public ITabletCellWriteManager
    , public TTabletAutomatonPart
{
public:
    TTabletCellWriteManager(
        ITabletCellWriteManagerHostPtr host,
        ISimpleHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IMutationForwarderPtr mutationForwarder)
        : TTabletAutomatonPart(
            host->GetCellId(),
            std::move(hydraManager),
            std::move(automaton),
            std::move(automatonInvoker),
            std::move(mutationForwarder))
        , Host_(std::move(host))
        , ChangelogCodec_(GetCodec(Host_->GetConfig()->ChangelogCodec))
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletCellWriteManager::HydraFollowerWriteRows, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletCellWriteManager::HydraWriteDelayedRows, Unretained(this)));
    }

    // ITabletCellWriteManager overrides.

    void Initialize() override
    {
        const auto& transactionManager = Host_->GetTransactionManager();
        transactionManager->SubscribeTransactionPrepared(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionPrepared, MakeWeak(this)));
        transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionCommitted, MakeWeak(this)));
        transactionManager->SubscribeTransactionCoarselySerialized(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionCoarselySerialized, MakeWeak(this)));
        transactionManager->SubscribeTransactionPerRowSerialized(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionPerRowSerialized, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionAborted, MakeWeak(this)));
        transactionManager->SubscribeTransactionTransientReset(BIND_NO_PROPAGATE(&TTabletCellWriteManager::OnTransactionTransientReset, MakeWeak(this)));
    }

    TFuture<void> Write(
        const TTabletSnapshotPtr& tabletSnapshot,
        TWireWriteCommandsBatchingReader* reader,
        const TTabletCellWriteParams& params) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (RandomNumber<double>() < GetDynamicConfig()->FailureProbabilityBeforeWrite) [[unlikely]] {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TestingFailureBeforeWrite,
                "Test error before write call execution")
                .With("tablet_id", tabletSnapshot->TabletId);
        }

        const auto& identity = NRpc::GetCurrentAuthenticationIdentity();
        bool replicatorWrite = IsReplicatorWrite(identity);

        TTablet* tablet = nullptr;
        const auto& transactionManager = Host_->GetTransactionManager();

        auto atomicity = AtomicityFromTransactionId(params.TransactionId);
        if (atomicity == EAtomicity::None) {
            ValidateClientTimestamp(params.TransactionId);
        }

        if (params.Generation > InitialTransactionGeneration) {
            if (params.Versioned) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying versioned writes is not supported");
            }
            if (replicatorWrite) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying replicator writes is not supported");
            }
            if (atomicity == EAtomicity::None) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying non-atomic writes is not supported");
            }
        }

        tabletSnapshot->TabletRuntimeData->ModificationTime = NProfiling::GetInstant();

        auto actualizeTablet = [&] (bool retryable) {
            if (tablet = Host_->FindTablet(tabletSnapshot->TabletId); !tablet) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::NoSuchTablet,
                    "No such tablet %v",
                    tabletSnapshot->TabletId)
                    .With("tablet_id", tabletSnapshot->TabletId)
                    .With("retryable", retryable);
            }

            tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
            ValidateTabletMounted(tablet);
        };

        tabletSnapshot->ValidateServantIsActive(Host_->GetCellDirectory(), /*waitForActivation*/ false)
            .ThrowOnError();
        actualizeTablet(/*retryable*/ true);

        if (!tablet->SmoothMovementData().IsWriteToTabletAllowed()) {
            WaitUntilServantIsWritable(
                tablet,
                Host_->GetCellDirectory(),
                Host_->GetDynamicConfig()->TabletManager->WaitOnReadOnlySmoothMovementStageTimeout);
            actualizeTablet(/*retryable*/ true);
            tablet->ValidateServantIsWritable(Host_->GetCellDirectory(), /*retryable*/ true)
                .ThrowOnError();
        }

        if (atomicity == EAtomicity::Full) {
            const auto& lockManager = tablet->GetLockManager();
            auto error = lockManager->ValidateTransactionConflict(params.TransactionStartTimestamp);
            if (!error.IsOK()) {
                THROW_ERROR error
                    .With("tablet_id", tablet->GetId())
                    .With("transaction_id", params.TransactionId);
            }
        }

        if (params.HunkChunksInfo) {
            const auto& hunkLockManager = tablet->GetHunkLockManager();
            auto future = hunkLockManager->LockHunkStores(*params.HunkChunksInfo);
            WaitForFast(std::move(future))
                .ThrowOnError();
        }

        auto throwPrerequisitesError = [&] (const TError& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(
                error,
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed");
        };

        auto error = WaitForFast(Host_->IssueLeases(params.PrerequisiteTransactionIds));
        if (!error.IsOK()) {
            throwPrerequisitesError(error);
        }

        // Due to possible row blocking, serving the request may involve a number of write attempts.
        // Each attempt causes a mutation to be enqueued to Hydra.
        // Since all these mutations are enqueued within a single epoch, only the last commit outcome is
        // actually relevant.
        // Note that we're passing signature to every such call but only the last one actually uses it.
        TFuture<void> commitResult;
        while (!reader->IsFinished()) {
            // NB: No yielding beyond this point.
            // May access tablet and transaction.

            actualizeTablet(/*retryable*/ false);

            ValidateTabletStoreLimit(tablet);

            auto poolTag = Host_->GetDynamicOptions()->EnableTabletDynamicMemoryLimit
                ? tablet->GetPoolTagByMemoryCategory(EMemoryCategory::TabletDynamic)
                : std::nullopt;
            Host_->ValidateMemoryLimit(poolTag);
            ValidateWriteBarrier(replicatorWrite, tablet);

            tablet->ValidateServantIsWritable(Host_->GetCellDirectory())
                .ThrowOnError();

            auto tabletId = tablet->GetId();

            TTransaction* transaction = nullptr;
            bool updateReplicationProgress = false;
            if (atomicity == EAtomicity::Full) {
                transactionManager->ValidateMaximumTransactionCount();

                transaction = transactionManager->GetOrCreateTransactionOrThrow(
                    params.TransactionId,
                    params.TransactionStartTimestamp,
                    params.TransactionTimeout,
                    /*transient*/ true);
                ValidateTransactionActive(transaction);

                try {
                    AddTransientLeasesOrThrow(transaction, params.PrerequisiteTransactionIds, /*force*/ false);
                } catch (const std::exception& ex) {
                    throwPrerequisitesError(TError(ex));
                }

                if (params.Generation > transaction->GetTransientGeneration()) {
                    // Promote transaction transient generation and clear the transaction transient state.
                    // In particular, we abort all rows that were prelocked or locked by the previous batches of our generation,
                    // but that is perfectly fine.
                    PromoteTransientGeneration(transaction, params.Generation);
                } else if (params.Generation < transaction->GetTransientGeneration()) {
                    // We may get here in two situations. The first one is when Write RPC call was late to arrive,
                    // while the second one is trickier. It happens in the case when next generation arrived while our
                    // fiber was waiting on the blocked row. In both cases we are not going to enqueue any more mutations
                    // in order to ensure monotonicity of mutation generations which is an important invariant.
                    YT_TLOG_DEBUG("Stopping obsolete generation write")
                        .With("TabletId", tabletId)
                        .With("TransactionId", params.TransactionId)
                        .WithFormat("Generation", "%x", params.Generation)
                        .WithFormat("TransientGeneration", "%x", transaction->GetTransientGeneration());
                    // Client already decided to go on with the next generation of rows, so we are ok to even ignore
                    // possible commit errors. Note that the result of this particular write does not affect the outcome of the
                    // transaction any more, so we are safe to lose some of freshly enqueued mutations.
                    return OKFuture;
                }

                updateReplicationProgress = tablet->GetReplicationCardId() && !params.Versioned;
            } else {
                YT_VERIFY(atomicity == EAtomicity::None);

                CheckTransientLeasesOrThrow(params.PrerequisiteTransactionIds);

                if (transactionManager->GetDecommission()) {
                    THROW_ERROR_EXCEPTION("Tablet cell is decommissioned");
                }
            }

            if (transaction) {
                AddTransientAffectedTablet(transaction, tablet);
            }

            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            auto context = tabletWriteManager->TransientWriteRows(
                transaction,
                reader,
                atomicity,
                params.Versioned,
                params.RowCount,
                params.DataWeight);

            // For last mutation we use signature from the request,
            // for other mutations signature is zero, see comment above.
            auto mutationPrepareSignature = InitialTransactionSignature;
            auto mutationCommitSignature = InitialTransactionSignature;
            if (reader->IsFinished()) {
                mutationPrepareSignature = params.PrepareSignature;
                mutationCommitSignature = params.CommitSignature;
            }

            auto lockless = context.Lockless;

            if (params.HunkChunksInfo) {
                const auto& hunkLockManager = tablet->GetHunkLockManager();
                for (const auto& [hunkStoreId, _] : params.HunkChunksInfo->HunkChunkRefs) {
                    hunkLockManager->IncrementTransientLockCount(hunkStoreId, +1);
                }
            }

            YT_TLOG_DEBUG_IF(context.RowCount > 0, "Rows written")
                .With("TransactionId", params.TransactionId)
                .With("TabletId", tabletId)
                .With("RowCount", context.RowCount)
                .With("Lockless", lockless)
                .WithFormat("Generation", "%x", params.Generation)
                .WithFormat("PrepareSignature", "%x", mutationPrepareSignature)
                .WithFormat("CommitSignature", "%x", mutationCommitSignature);

            if (atomicity == EAtomicity::Full) {
                transaction->TransientPrepareSignature() += mutationPrepareSignature;
            }

            auto tableProfiler = tablet->GetTableProfiler();

            if (!reader->IsBatchEmpty()) {
                auto writeCommandBatch = reader->FinishBatch();
                auto compressedRecordData = ChangelogCodec_->Compress(writeCommandBatch.Data());
                TTransactionWriteRecord writeRecord(
                    tabletId,
                    std::move(writeCommandBatch),
                    context.RowCount,
                    context.DataWeight,
                    params.SyncReplicaIds,
                    params.HunkChunksInfo);

                PrelockedTablets_.push(tablet);
                LockTablet(tablet, ETabletLockType::TransientWrite);

                IncrementTabletInFlightMutationCount(tablet, replicatorWrite, +1);

                TReqWriteRows hydraRequest;
                ToProto(hydraRequest.mutable_transaction_id(), params.TransactionId);
                hydraRequest.set_transaction_start_timestamp(ToProto(params.TransactionStartTimestamp));
                hydraRequest.set_transaction_timeout(ToProto(params.TransactionTimeout));
                ToProto(hydraRequest.mutable_tablet_id(), tabletId);
                hydraRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));
                hydraRequest.set_codec(ToProto(ChangelogCodec_->GetId()));
                hydraRequest.set_compressed_data(ToString(compressedRecordData));
                hydraRequest.set_prepare_signature(mutationPrepareSignature);
                hydraRequest.set_commit_signature(mutationCommitSignature);
                hydraRequest.set_generation(params.Generation);
                hydraRequest.set_lockless(lockless);
                hydraRequest.set_row_count(writeRecord.RowCount);
                hydraRequest.set_data_weight(writeRecord.DataWeight);
                hydraRequest.set_update_replication_progress(updateReplicationProgress);

                if (params.HunkChunksInfo) {
                    ToProto(hydraRequest.mutable_hunk_chunks_info(), *params.HunkChunksInfo);
                }

                ToProto(hydraRequest.mutable_sync_replica_ids(), params.SyncReplicaIds);
                ToProto(hydraRequest.mutable_prerequisite_transaction_ids(), params.PrerequisiteTransactionIds);

                NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, identity);

                auto mutation = CreateMutation(HydraManager_, hydraRequest);
                mutation->SetHandler(BIND_NO_PROPAGATE(
                    &TTabletCellWriteManager::HydraLeaderWriteRows,
                    MakeStrong(this),
                    params.TransactionId,
                    tablet->GetMountRevision(),
                    mutationPrepareSignature,
                    mutationCommitSignature,
                    params.Generation,
                    lockless,
                    writeRecord,
                    identity,
                    updateReplicationProgress,
                    params.PrerequisiteTransactionIds));
                mutation->SetCurrentTraceContext();
                commitResult = mutation->Commit().As<void>();

                auto counters = tableProfiler->GetWriteCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(writeRecord.RowCount);
                counters->DataWeight.Increment(writeRecord.DataWeight);
            }

            // NB: Yielding is now possible.
            // Cannot neither access tablet, nor transaction.
            if (context.BlockedStore) {
                auto waitOnBlockedRowDuration = context.BlockedStore->WaitOnBlockedRow(
                    context.BlockedRow,
                    context.BlockedLockMask,
                    context.BlockedTimestamp);

                tableProfiler
                    ->GetWriteCounters(GetCurrentProfilingUser())
                    ->WaitOnBlockedRowDuration
                    .Record(waitOnBlockedRowDuration);
            }

            context.Error.ThrowOnError();
        }

        if (RandomNumber<double>() < GetDynamicConfig()->FailureProbabilityAfterWrite) [[unlikely]] {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TestingFailureAfterWrite,
                "Test error after write call execution")
                .With("tablet_id", tabletSnapshot->TabletId);
        }

        return commitResult;
    }

    // TTabletAutomatonPart overrides.

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnStopLeading();

        while (!PrelockedTablets_.empty()) {
            auto* tablet = PrelockedTablets_.front();
            PrelockedTablets_.pop();
            UnlockTablet(tablet, ETabletLockType::TransientWrite);
        }
    }

    void OnAfterSnapshotLoaded() noexcept override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& transactionManager = Host_->GetTransactionManager();
        auto transactions = transactionManager->GetTransactions();

        for (auto* transaction : transactions) {
            YT_VERIFY(GetTransientAffectedTablets(transaction).empty());
            for (auto* tablet : GetPersistentAffectedTablets(transaction)) {
                LockTablet(tablet, ETabletLockType::PersistentTransaction);
            }
        }
    }

private:
    const ITabletCellWriteManagerHostPtr Host_;
    ICodec* const ChangelogCodec_;

    TRingQueue<TTablet*> PrelockedTablets_;

    // NB: Write logs are generally much smaller than dynamic stores,
    // so we don't worry about per-pool management here.
    TMemoryUsageTrackerGuard WriteLogsMemoryTrackerGuard_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraLeaderWriteRows(
        TTransactionId transactionId,
        NHydra::TRevision mountRevision,
        TTransactionSignature prepareSignature,
        TTransactionSignature commitSignature,
        TTransactionGeneration generation,
        bool lockless,
        const TTransactionWriteRecord& writeRecord,
        const NRpc::TAuthenticationIdentity& identity,
        bool updateReplicationProgress,
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        TMutationContext* context) noexcept
    {
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);
        bool replicatorWrite = IsReplicatorWrite(identity);

        auto atomicity = AtomicityFromTransactionId(transactionId);

        auto* tablet = PrelockedTablets_.front();
        PrelockedTablets_.pop();
        YT_VERIFY(tablet->GetId() == writeRecord.TabletId);
        auto finallyGuard = Finally([&] {
            UnlockTablet(tablet, ETabletLockType::TransientWrite);
        });

        IncrementTabletInFlightMutationCount(tablet, replicatorWrite, -1);

        if (mountRevision != tablet->GetMountRevision()) {
            YT_TLOG_DEBUG("Mount revision mismatch; write ignored")
                .With(tablet->GetLoggingTags())
                .With("TransactionId", transactionId)
                .WithFormat("MutationMountRevision", "%x", mountRevision)
                .WithFormat("CurrentMountRevision", "%x", tablet->GetMountRevision());
            return;
        }

        if (writeRecord.HunkChunksInfo) {
            TCompactVector<THunkStoreId, 1> lostHunkStoreIds;
            const auto& hunkLockManager = tablet->GetHunkLockManager();
            for (const auto& [hunkStoreId, _] : writeRecord.HunkChunksInfo->HunkChunkRefs) {
                if (!hunkLockManager->GetTotalLockCount(hunkStoreId)) {
                    lostHunkStoreIds.push_back(hunkStoreId);
                } else {
                    hunkLockManager->IncrementTransientLockCount(hunkStoreId, -1);
                }
            }

            if (!lostHunkStoreIds.empty()) {
                YT_TLOG_ALERT("Hunk store locks are lost; write ignored")
                    .With(tablet->GetLoggingTags())
                    .With("TransactionId", transactionId)
                    .With("HunkStoreIds", lostHunkStoreIds);
                return;
            }
        }

        TTransaction* transaction = nullptr;
        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Host_->GetTransactionManager();
                try {
                    // NB: May throw if tablet cell is decommissioned or suspended.
                    transaction = transactionManager->MakeTransactionPersistentOrThrow(transactionId);
                } catch (const std::exception& ex) {
                    YT_TLOG_DEBUG("Failed to make transaction persistent")
                        .With("TabletId", writeRecord.TabletId)
                        .With("TransactionId", transactionId)
                        .With(ex);
                    return;
                }

                AddPersistentAffectedTablet(transaction, tablet);

                AddPersistentLeases(transaction, prerequisiteTransactionIds);

                YT_TLOG_DEBUG("Performing atomic write as leader")
                    .With("TabletId", writeRecord.TabletId)
                    .With("TransactionId", transactionId)
                    .WithFormat("BatchGeneration", "%x", generation)
                    .WithFormat("TransientGeneration", "%x", transaction->GetTransientGeneration())
                    .WithFormat("PersistentGeneration", "%x", transaction->GetPersistentGeneration())
                    .With("PrerequisiteTransactionIds", prerequisiteTransactionIds);

                // Monotonicity of persistent generations is ensured by the early finish in #Write whenever the
                // current batch is obsolete.
                YT_VERIFY(generation >= transaction->GetPersistentGeneration());
                YT_VERIFY(generation <= transaction->GetTransientGeneration());
                if (generation > transaction->GetPersistentGeneration()) {
                    // Promote persistent generation and also clear current persistent transaction state (i.e. write logs).
                    PromotePersistentGeneration(transaction, generation);
                }

                const auto& tabletWriteManager = tablet->GetTabletWriteManager();
                tabletWriteManager->AtomicLeaderWriteRows(transaction, generation, writeRecord, lockless);

                transaction->PersistentPrepareSignature() += prepareSignature;
                // NB: May destroy transaction.
                transactionManager->IncrementCommitSignature(transaction, commitSignature);

                if (updateReplicationProgress) {
                    // Update replication progress for queue replicas so async replicas can pull from them as fast as possible.
                    // NB: This replication progress update is a best effort and does not require tablet locking.
                    transaction->TabletsToUpdateReplicationProgress().insert(tablet->GetId());
                }

                break;
            }

            case EAtomicity::None: {
                const auto& transactionManager = Host_->GetTransactionManager();
                if (transactionManager->GetDecommission()) {
                    YT_TLOG_DEBUG("Tablet cell is decommissioning, skip non-atomic write");
                    return;
                }

                // This is ensured by a corresponding check in #Write.
                YT_VERIFY(generation == InitialTransactionGeneration);

                if (tablet->GetState() == ETabletState::Orphaned) {
                    YT_TLOG_DEBUG("Tablet is orphaned; non-atomic write ignored")
                        .With(tablet->GetLoggingTags())
                        .With("TransactionId", transactionId);
                    return;
                }

                const auto& tabletWriteManager = tablet->GetTabletWriteManager();
                tabletWriteManager->NonAtomicWriteRows(transactionId, writeRecord, /*isLeader*/ true);
                break;
            }

            default:
                YT_ABORT();
        }

        if (writeRecord.HunkChunksInfo) {
            const auto& hunkLockManager = tablet->GetHunkLockManager();
            for (const auto& [hunkStoreId, _] : writeRecord.HunkChunksInfo->HunkChunkRefs) {
                hunkLockManager->IncrementPersistentLockCount(hunkStoreId, +1);
            }
        }

        if (tablet->SmoothMovementData().ShouldForwardMutation()) {
            TReqWriteRows forwardedRequest;
            DeserializeProtoWithEnvelope(&forwardedRequest, context->Request().Data);
            ForwardWriteRowsMutation(tablet, transaction, transactionId, std::move(forwardedRequest));
        }
    }

    void HydraFollowerWriteRows(TReqWriteRows* request) noexcept
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto atomicity = AtomicityFromTransactionId(transactionId);
        auto transactionStartTimestamp = FromProto<NTransactionClient::TTimestamp>(request->transaction_start_timestamp());
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto prepareSignature = request->prepare_signature();
        // COMPAT(gritukan)
        auto commitSignature = request->has_commit_signature() ? request->commit_signature() : prepareSignature;
        auto generation = request->generation();
        auto lockless = request->lockless();
        auto rowCount = request->row_count();
        auto dataWeight = request->data_weight();
        auto syncReplicaIds = FromProto<TSyncReplicaIdList>(request->sync_replica_ids());
        auto updateReplicationProgress = request->update_replication_progress();
        std::optional<THunkChunksInfo> hunkChunksInfo;
        if (request->has_hunk_chunks_info()) {
            hunkChunksInfo = FromProto<THunkChunksInfo>(request->hunk_chunks_info());
        }
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        auto transactionExternalizationToken = FromProto<TTransactionExternalizationToken>(
            request->transaction_externalization_token());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was, e.g., forcefully removed.
            return;
        }

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (mountRevision != tablet->GetMountRevision()) {
            // Same as above.
            return;
        }

        auto lockHunkStores = [&] {
            if (hunkChunksInfo) {
                const auto& hunkLockManager = tablet->GetHunkLockManager();
                for (const auto& [hunkChunkId, _] : hunkChunksInfo->HunkChunkRefs) {
                    hunkLockManager->IncrementPersistentLockCount(hunkChunkId, +1);
                }
            }
        };

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto codecId = FromProto<ECodec>(request->codec());
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(request->compressed_data());

        auto data = codec->Decompress(compressedRecordData);
        auto rowBuffer = New<TRowBuffer>();
        auto reader = CreateWireProtocolReader(data, rowBuffer);
        auto commands = ParseWriteCommands(
            tablet->TableSchemaData(),
            reader.get(),
            tablet->IsVersionedWriteUnversioned());

        auto batch = TWireWriteCommandsBatch(
            std::move(commands),
            std::move(rowBuffer),
            std::move(data));

        TTransactionWriteRecord writeRecord(
            tabletId,
            std::move(batch),
            rowCount,
            dataWeight,
            syncReplicaIds,
            hunkChunksInfo);

        YT_VERIFY(writeRecord.GetByteSize() != 0);

        TTransaction* transaction = nullptr;

        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Host_->GetTransactionManager();
                try {
                    // NB: May throw if tablet cell is decommissioned.
                    transaction = transactionManager->GetOrCreateTransactionOrThrow(
                        transactionId,
                        transactionStartTimestamp,
                        transactionTimeout,
                        false,
                        transactionExternalizationToken);
                } catch (const std::exception& ex) {
                    YT_TLOG_DEBUG("Failed to create transaction")
                        .With("TransactionId", FormatTransactionId(transactionId, transactionExternalizationToken))
                        .With("TabletId", tabletId)
                        .With(ex);
                    return;
                }

                lockHunkStores();

                AddPersistentAffectedTablet(transaction, tablet);

                if (tablet->IsActiveServant()) {
                    AddPersistentLeases(transaction, prerequisiteTransactionIds);
                }

                YT_TLOG_DEBUG("Performing atomic write as follower")
                    .With("TabletId", tabletId)
                    .With("TransactionId", FormatTransactionId(transactionId, transactionExternalizationToken))
                    .WithFormat("BatchGeneration", "%x", generation)
                    .WithFormat("PersistentGeneration", "%x", transaction->GetPersistentGeneration())
                    .With("PrerequisiteTransactionIds", prerequisiteTransactionIds);

                // This invariant holds during recovery.
                YT_VERIFY(transaction->GetPersistentGeneration() == transaction->GetTransientGeneration());
                // Monotonicity of persistent generations is ensured by the early finish in #Write whenever the
                // current batch is obsolete.
                YT_VERIFY(transaction->GetPersistentGeneration() <= generation);
                if (generation > transaction->GetPersistentGeneration()) {
                    // While in recovery, we are responsible for keeping both transient and persistent state up-to-date.
                    // Hence, generation promotion must be handles as a combination of transient and persistent generation promotions
                    // from the regular leader case.
                    PromoteTransientGeneration(transaction, generation);
                    PromotePersistentGeneration(transaction, generation);
                }

                const auto& tabletWriteManager = tablet->GetTabletWriteManager();
                tabletWriteManager->AtomicFollowerWriteRows(transaction, writeRecord, lockless);

                if (updateReplicationProgress) {
                    // Update replication progress for queue replicas so async replicas can pull from them as fast as possible.
                    // NB: This replication progress update is a best effort and does not require tablet locking.
                    transaction->TabletsToUpdateReplicationProgress().insert(tablet->GetId());
                }

                transaction->PersistentPrepareSignature() += prepareSignature;
                transactionManager->IncrementCommitSignature(transaction, commitSignature);

                break;
            }


            case EAtomicity::None: {
                const auto& transactionManager = Host_->GetTransactionManager();
                if (transactionManager->GetDecommission()) {
                    YT_TLOG_DEBUG("Tablet cell is decommissioning, skip non-atomic write");
                    return;
                }

                lockHunkStores();

                // This is ensured by a corresponding check in #Write.
                YT_VERIFY(generation == InitialTransactionGeneration);

                const auto& tabletWriteManager = tablet->GetTabletWriteManager();
                tabletWriteManager->NonAtomicWriteRows(transactionId, writeRecord, /*isLeader*/ false);
                break;
            }

            default:
                YT_ABORT();
        }

        if (tablet->SmoothMovementData().ShouldForwardMutation()) {
            ForwardWriteRowsMutation(tablet, transaction, transactionId, *request);
        }
    }

    void ForwardWriteRowsMutation(
        TTablet* tablet,
        TTransaction* transaction,
        TTransactionId transactionId,
        TReqWriteRows request)
    {
        YT_TLOG_DEBUG("Forwarding writes to sibling servant")
            .With(tablet->GetLoggingTags())
            .With("TransactionId", transactionId);

        TTransactionExternalizationToken token(tablet->SmoothMovementData().GetSiblingAvenueEndpointId());
        auto atomicity = AtomicityFromTransactionId(transactionId);

        if (atomicity == EAtomicity::Full) {
            YT_VERIFY(transaction);
            YT_VERIFY(transaction->GetId() == transactionId);

            const auto& transactionManager = Host_->GetTransactionManager();
            transactionManager->RegisterExternalizerTablet(transaction, tablet->GetId(), token);
        }

        auto newTransactionType = atomicity == EAtomicity::Full
            ? EObjectType::ExternalizedAtomicTabletTransaction
            : EObjectType::ExternalizedNonAtomicTabletTransaction;

        ToProto(request.mutable_transaction_externalization_token(), token);
        ToProto(
            request.mutable_transaction_id(),
            ReplaceTypeInId(transactionId, newTransactionType));
        request.set_mount_revision(
            ToProto(tablet->SmoothMovementData().GetSiblingMountRevision()));

        MutationForwarder_->MaybeForwardMutationToSiblingServant(
            tablet,
            request);
    }

    void HydraWriteDelayedRows(TReqWriteDelayedRows* request) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        YT_VERIFY(AtomicityFromTransactionId(transactionId) == EAtomicity::Full);

        auto rowCount = request->row_count();
        auto dataWeight = request->data_weight();
        auto commitSignature = request->commit_signature();

        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was, e.g., forcefully removed.
            YT_TLOG_DEBUG("Received delayed rows for nonexistent tablet; ignored")
                .With("TabletId", tabletId)
                .With("TransactionId", transactionId);
            return;
        }

        auto mountRevision = FromProto<NHydra::TRevision>(request->mount_revision());
        if (tablet->GetMountRevision() != mountRevision) {
            YT_TLOG_DEBUG("Received delayed rows with invalid mount revision; ignored")
                .With("TabletId", tabletId)
                .With("TransactionId", transactionId)
                .WithFormat("TabletMountRevision", "%x", tablet->GetMountRevision())
                .WithFormat("RequestMountRevision", "%x", mountRevision);
            return;
        }

        auto lockless = request->lockless();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto codecId = FromProto<ECodec>(request->codec());
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(request->compressed_data());

        auto data = codec->Decompress(compressedRecordData);
        auto rowBuffer = New<TRowBuffer>();
        auto reader = CreateWireProtocolReader(data, rowBuffer);
        auto commands = ParseWriteCommands(
            tablet->TableSchemaData(),
            reader.get(),
            tablet->IsVersionedWriteUnversioned());

        auto batch = TWireWriteCommandsBatch(
            std::move(commands),
            std::move(rowBuffer),
            std::move(data));

        TTransactionWriteRecord writeRecord(
            tabletId,
            std::move(batch),
            rowCount,
            dataWeight,
            /*syncReplicaIds*/ {},
            /*hunkChunksInfo*/ {});

        YT_VERIFY(writeRecord.GetByteSize() != 0);

        const auto& transactionManager = Host_->GetTransactionManager();
        auto* transaction = transactionManager->FindPersistentTransaction(transactionId);

        if (!transaction) {
            YT_TLOG_ALERT("Delayed rows sent for absent transaction, ignored")
                .With("TransactionId", transactionId)
                .With("TabletId", tablet->GetId())
                .With("RowCount", rowCount)
                .With("DataWeight", dataWeight)
                .WithFormat("CommitSignature", "%x", commitSignature);
            return;
        }

        YT_TLOG_DEBUG("Writing transaction delayed rows")
            .With("TabletId", tablet->GetId())
            .With("TransactionId", transaction->GetId())
            .With("RowCount", writeRecord.RowCount)
            .With("Lockless", lockless)
            .WithFormat("CommitSignature", "%x", commitSignature);

        auto tabletWriteManager = tablet->GetTabletWriteManager();
        tabletWriteManager->WriteDelayedRows(transaction, writeRecord, lockless);

        // NB: May destroy transaction.
        transactionManager->IncrementCommitSignature(transaction, commitSignature);
    }

    void OnTransactionPrepared(TTransaction* transaction, bool persistent)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext() == persistent);

        auto codicilGuard = MakeCodicilGuard(transaction);

        auto tablets = persistent
            ? GetPersistentAffectedTablets(transaction)
            : GetTransientAffectedTablets(transaction);

        for (auto* tablet : tablets) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionPrepared(transaction, persistent);
        }
    }

    void OnTransactionCommitted(TTransaction* transaction) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        auto codicilGuard = MakeCodicilGuard(transaction);

        transaction->IncrementPartsLeftToPerRowSerialize();

        for (auto* tablet : GetPersistentAffectedTablets(transaction)) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionCommitted(transaction);
        }

        transaction->DecrementPartsLeftToPerRowSerialize();

        if (transaction->GetPartsLeftToPerRowSerialize() == 0) {
            OnTransactionSerializationFinished(transaction, ESerializationStatus::PerRowFinished);
        }

        if (!transaction->IsCoarseSerializationNeeded()) {
            OnTransactionSerializationFinished(transaction, ESerializationStatus::CoarseFinished);
        }
    }

    void OnTransactionCoarselySerialized(TTransaction* transaction) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto codicilGuard = MakeCodicilGuard(transaction);

        auto coarseSerializingTabletIds = transaction->CoarseSerializingTabletIds();
        for (auto tabletId : coarseSerializingTabletIds) {
            auto* tablet = Host_->FindTablet(tabletId);
            if (!tablet) {
                EraseOrCrash(transaction->CoarseSerializingTabletIds(), tabletId);
                continue;
            }

            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionCoarselySerialized(transaction);
        }

        YT_VERIFY(transaction->CoarseSerializingTabletIds().empty());

        for (auto tabletId : transaction->TabletsToUpdateReplicationProgress()) {
            auto* tablet = Host_->FindTablet(tabletId);
            if (!tablet) {
                continue;
            }

            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->UpdateReplicationProgress(transaction);
        }

        OnTransactionSerializationFinished(transaction, ESerializationStatus::CoarseFinished);
    }

    void OnTransactionPerRowSerialized(TTransaction* transaction) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto coarseSerializingTabletIds = transaction->PerRowSerializingTabletIds();
        for (auto tabletId : coarseSerializingTabletIds) {
            auto* tablet = Host_->FindTablet(tabletId);
            if (!tablet) {
                EraseOrCrash(transaction->PerRowSerializingTabletIds(), tabletId);
                continue;
            }

            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionPerRowSerialized(transaction);
        }

        YT_VERIFY(transaction->PerRowSerializingTabletIds().empty());

        OnTransactionSerializationFinished(transaction, ESerializationStatus::PerRowFinished);
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto codicilGuard = MakeCodicilGuard(transaction);

        for (auto* tablet : GetAffectedTablets(transaction)) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionAborted(transaction);
        }

        OnTransactionFinished(transaction);
    }

    void OnTransactionSerializationFinished(TTransaction* transaction, ESerializationStatus type)
    {
        auto serializationStatus = transaction->GetSerializationStatus();
        YT_ASSERT((serializationStatus & type) == ESerializationStatus::None);

        transaction->SetSerializationStatus(serializationStatus | type);

        if (transaction->GetSerializationStatus() == (ESerializationStatus::CoarseFinished | ESerializationStatus::PerRowFinished)) {
            OnTransactionFinished(transaction);
        }
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

#ifndef NDEBUG
        auto affectedTablets = GetAffectedTablets(transaction);
#endif

        UnlockLockedTablets(transaction);
        ClearTransientLeases(transaction);
        ClearPersistentLeases(transaction);

#ifndef NDEBUG
        for (auto* tablet : affectedTablets) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            if (tabletWriteManager->HasWriteState(transaction)) {
                YT_TLOG_ALERT("Tablet still has transation write state on transaction finish")
                    .With(tablet->GetLoggingTags())
                    .With("TransactionId", transaction->GetId());
            }
        }
#endif
    }

    //! This method promotes transaction transient generation and also resets its transient state.
    //! In particular, it aborts all row locks in sorted dynamic stores induced by the transaction,
    //! and resets (transient) lists of prelocked and locked row refs.
    void PromoteTransientGeneration(TTransaction* transaction, TTransactionGeneration generation)
    {
        // This method may be called either with or without a mutation context.

        YT_TLOG_DEBUG("Promoting transaction transient generation")
            .With("TransactionId", transaction->GetId())
            .WithFormat("TransientGeneration", "%x -> %x", transaction->GetTransientGeneration(), generation);

        transaction->SetTransientGeneration(generation);
        transaction->TransientPrepareSignature() = InitialTransactionSignature;

        for (auto* tablet : GetAffectedTablets(transaction)) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransientGenerationPromoted(transaction);
        }

        // NB: It is ok not to unlock prelocked tablets since tablet locking is a lifetime ensurance mechanism
        // in contrast to row prelocking/locking which is a conflict prevention mechanism. Moreover, we do not
        // want the tablet to become fully unlocked while we still have in flight mutations, so it is better not
        // to touch tablet locks here at all.
    }

    //! This method promotes transaction persistent generation and also resets its persistent state by
    //! clearing all associated write logs.
    void PromotePersistentGeneration(TTransaction* transaction, TTransactionGeneration generation)
    {
        YT_VERIFY(HasMutationContext());

        YT_TLOG_DEBUG("Promoting transaction persistent generation")
            .With("TransactionId", transaction->GetId())
            .WithFormat("PersistentGeneration", "%x -> %x", transaction->GetPersistentGeneration(), generation);

        transaction->SetPersistentGeneration(generation);
        transaction->PersistentPrepareSignature() = InitialTransactionSignature;
        transaction->CommitSignature() = InitialTransactionSignature;

        for (auto* tablet : GetPersistentAffectedTablets(transaction)) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnPersistentGenerationPromoted(transaction);
        }
    }

    void OnTransactionTransientReset(TTransaction* transaction, TTimestamp transientPrepareTimestamp)
    {
        for (auto* tablet : GetAffectedTablets(transaction)) {
            const auto& tabletWriteManager = tablet->GetTabletWriteManager();
            tabletWriteManager->OnTransactionTransientReset(transaction, transientPrepareTimestamp);
        }

        // Release transient locks.
        for (auto* tablet : GetTransientAffectedTablets(transaction, /*includeOrphaned*/ true)) {
            UnlockTablet(tablet, ETabletLockType::TransientTransaction);
        }
        transaction->TransientAffectedTabletIds().clear();

        // NB: Transient lease ref counters are reset automatically by Lease Manager when epoch ends.
        transaction->TransientLeaseIds().clear();
    }

    void ValidateClientTimestamp(TTransactionId transactionId)
    {
        auto clientTimestamp = TimestampFromTransactionId(transactionId);
        auto serverTimestamp = Host_->GetLatestTimestamp();
        auto clientInstant = TimestampToInstant(clientTimestamp).first;
        auto serverInstant = TimestampToInstant(serverTimestamp).first;
        auto clientTimestampThreshold = Host_->GetConfig()->ClientTimestampThreshold;
        if (clientInstant > serverInstant + clientTimestampThreshold ||
            clientInstant < serverInstant - clientTimestampThreshold)
        {
            THROW_ERROR_EXCEPTION("Transaction timestamp is off limits, check the local clock readings")
            .With("client_timestamp", clientTimestamp)
            .With("server_timestamp", serverTimestamp);
        }
    }

    void ValidateTabletStoreLimit(TTablet* tablet)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto storeCount = std::ssize(tablet->StoreIdMap());
        auto storeLimit = mountConfig->MaxStoresPerTablet;
        if (storeCount >= storeLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many stores in tablet, all writes disabled")
                .With("tablet_id", tablet->GetId())
                .With("table_path", tablet->GetTablePath())
                .With("store_count", storeCount)
                .With("store_limit", storeLimit);
        }

        auto overlappingStoreCount = tablet->GetOverlappingStoreCount();
        auto overlappingStoreLimit = mountConfig->MaxOverlappingStoreCount;
        if (overlappingStoreCount >= overlappingStoreLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many overlapping stores in tablet, all writes disabled")
                .With("tablet_id", tablet->GetId())
                .With("table_path", tablet->GetTablePath())
                .With("overlapping_store_count", overlappingStoreCount)
                .With("overlapping_store_limit", overlappingStoreLimit);
        }

        auto edenStoreCount = tablet->GetEdenStoreCount();
        auto edenStoreCountLimit = mountConfig->MaxEdenStoresPerTablet;
        if (edenStoreCount >= edenStoreCountLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many eden stores in tablet, all writes disabled")
                .With("tablet_id", tablet->GetId())
                .With("table_path", tablet->GetTablePath())
                .With("eden_store_count", edenStoreCount)
                .With("eden_store_limit", edenStoreCountLimit);
        }

        auto dynamicStoreCount = tablet->GetDynamicStoreCount();
        if (dynamicStoreCount >= DynamicStoreCountLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many dynamic stores in tablet, all writes disabled")
                .With("tablet_id", tablet->GetId())
                .With("table_path", tablet->GetTablePath())
                .With("dynamic_store_count", dynamicStoreCount)
                .With("dynamic_store_count_limit", DynamicStoreCountLimit);
        }

        auto overflow = tablet->GetStoreManager()->CheckOverflow();
        if (!overflow.IsOK()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Active store is overflown, all writes disabled")
                .With("tablet_id", tablet->GetId())
                .With("table_path", tablet->GetTablePath())
                .With(overflow);
        }

        if (tablet->IsPhysicallyOrdered()) {
            i64 tabletDataWeight = tablet->GetTotalDataWeight();
            if (auto maxOrderedTabletDataWeight = mountConfig->MaxOrderedTabletDataWeight;
                maxOrderedTabletDataWeight && tabletDataWeight >= maxOrderedTabletDataWeight)
            {
                THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::RequestThrottled,
                    "Size of tablet %v exceeds the limit, all writes disabled",
                    tablet->GetId())
                    .With("data_weight", tabletDataWeight)
                    .With("data_weight_limit", *maxOrderedTabletDataWeight);
            }
        }
    }

    static bool IsReplicatorWrite(const NRpc::TAuthenticationIdentity& identity)
    {
        return identity.User == NSecurityClient::ReplicatorUserName;
    }

    static bool IsReplicatorWrite(TTransaction* transaction)
    {
        return IsReplicatorWrite(transaction->AuthenticationIdentity());
    }

    static void IncrementTabletInFlightMutationCount(TTablet* tablet, bool replicatorWrite, int delta)
    {
        if (replicatorWrite) {
            tablet->SetInFlightReplicatorMutationCount(tablet->GetInFlightReplicatorMutationCount() + delta);
        } else {
            tablet->SetInFlightUserMutationCount(tablet->GetInFlightUserMutationCount() + delta);
        }
    }

    static void ValidateWriteBarrier(bool replicatorWrite, TTablet* tablet)
    {
        if (replicatorWrite) {
            if (tablet->GetInFlightUserMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user mutations are still in flight")
                    .With("tablet_id", tablet->GetId())
                    .With("table_path", tablet->GetTablePath())
                    .With("in_flight_mutation_count", tablet->GetInFlightUserMutationCount());
            }
            if (tablet->GetPendingUserWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user writes are still pending")
                    .With("tablet_id", tablet->GetId())
                    .With("table_path", tablet->GetTablePath())
                    .With("pending_write_record_count", tablet->GetPendingUserWriteRecordCount());
            }
        } else {
            if (tablet->GetInFlightReplicatorMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator mutations are still in flight")
                    .With("tablet_id", tablet->GetId())
                    .With("table_path", tablet->GetTablePath())
                    .With("in_flight_mutation_count", tablet->GetInFlightReplicatorMutationCount());
            }
            if (tablet->GetPendingReplicatorWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator writes are still pending")
                    .With("tablet_id", tablet->GetId())
                    .With("table_path", tablet->GetTablePath())
                    .With("pending_write_record_count", tablet->GetPendingReplicatorWriteRecordCount());
            }
        }
    }

    std::vector<TTablet*> GetTabletByIds(const THashSet<TTabletId>& tabletIds, bool includeOrphaned = false)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        std::vector<TTablet*> tablets;
        tablets.reserve(tabletIds.size());
        for (auto tabletId : tabletIds) {
            if (auto* tablet = Host_->FindTablet(tabletId)) {
                tablets.push_back(tablet);
            } else if (includeOrphaned) {
                if (auto* tablet = Host_->FindOrphanedTablet(tabletId)) {
                    tablets.push_back(tablet);
                }
            }
        }

        return tablets;
    }

    void AddTransientAffectedTablet(TTransaction* transaction, TTablet* tablet) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto tabletId = tablet->GetId();
        if (transaction->TransientAffectedTabletIds().emplace(tabletId).second) {
            auto lockCount = LockTablet(tablet, ETabletLockType::TransientTransaction);
            YT_TLOG_DEBUG("Transaction transiently affects tablet")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tablet->GetId())
                .With("LockCount", lockCount);
        }
    }

    void AddPersistentAffectedTablet(TTransaction* transaction, TTablet* tablet) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(!transaction->GetTransient());

        auto tabletId = tablet->GetId();
        if (transaction->PersistentAffectedTabletIds().emplace(tabletId).second) {
            auto lockCount = LockTablet(tablet, ETabletLockType::PersistentTransaction);
            YT_TLOG_DEBUG("Transaction persistently affects tablet")
                .With("TransactionId", transaction->GetId())
                .With("TabletId", tablet->GetId())
                .With("LockCount", lockCount);
        }
    }

    std::vector<TTablet*> GetTransientAffectedTablets(TTransaction* transaction, bool includeOrphaned = false)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return GetTabletByIds(transaction->TransientAffectedTabletIds(), includeOrphaned);
    }

    std::vector<TTablet*> GetPersistentAffectedTablets(TTransaction* transaction, bool includeOrphaned = false)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return GetTabletByIds(transaction->PersistentAffectedTabletIds(), includeOrphaned);
    }

    std::vector<TTablet*> GetAffectedTablets(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return GetTabletByIds(transaction->GetAffectedTabletIds());
    }

    void ValidateTransactionActive(TTransaction* transaction)
    {
        if (transaction->GetTransientState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }
    }

    i64 LockTablet(TTablet* tablet, ETabletLockType lockType)
    {
        return Host_->LockTablet(tablet, lockType);
    }

    i64 UnlockTablet(TTablet* tablet, ETabletLockType lockType)
    {
        return Host_->UnlockTablet(tablet, lockType);
    }

    void UnlockLockedTablets(TTransaction* transaction)
    {
        // NB: Transaction may hold both transient and persistent lock on tablet,
        // so #GetAffectedTablets cannot be used here.
        for (auto* tablet : GetTransientAffectedTablets(transaction, /*includeOrphaned*/ true)) {
            UnlockTablet(tablet, ETabletLockType::TransientTransaction);
        }
        transaction->TransientAffectedTabletIds().clear();

        for (auto* tablet : GetPersistentAffectedTablets(transaction, /*includeOrphaned*/ true)) {
            UnlockTablet(tablet, ETabletLockType::PersistentTransaction);
        }
        transaction->PersistentAffectedTabletIds().clear();
    }

    void AddPersistentLeases(
        TTransaction* transaction,
        const std::vector<TTransactionId>& prerequisiteTransactionIds)
    {
        const auto& leaseManager = Host_->GetLeaseManager();
        for (auto prerequisiteTransactionId : prerequisiteTransactionIds) {
            auto* lease = leaseManager->GetLease(prerequisiteTransactionId);
            lease->RefPersistently(/*force*/ true);
            transaction->PersistentLeaseIds().push_back(lease->GetId());
        }
    }

    void CheckTransientLeasesOrThrow(const std::vector<TTransactionId>& prerequisiteTransactionIds)
    {
        const auto& leaseManager = Host_->GetLeaseManager();
        for (auto prerequisiteTransactionId : prerequisiteTransactionIds) {
            auto* lease = leaseManager->GetLeaseOrThrow(prerequisiteTransactionId);
            lease->RefTransiently(/*force*/ false);
            lease->UnrefTransiently();
        }
    }

    void AddTransientLeasesOrThrow(
        TTransaction* transaction,
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        bool force)
    {
        const auto& leaseManager = Host_->GetLeaseManager();
        for (auto prerequisiteTransactionId : prerequisiteTransactionIds) {
            auto* lease = leaseManager->GetLeaseOrThrow(prerequisiteTransactionId);
            lease->RefTransiently(force);
            transaction->TransientLeaseIds().push_back(lease->GetId());
        }
    }

    void ClearPersistentLeases(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& leaseManager = Host_->GetLeaseManager();
        for (auto leaseId : transaction->PersistentLeaseIds()) {
            if (auto* lease = leaseManager->FindLease(leaseId)) {
                lease->UnrefPersistently();
            }
        }
        transaction->PersistentLeaseIds().clear();
    }

    void ClearTransientLeases(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& leaseManager = Host_->GetLeaseManager();
        for (auto leaseId : transaction->TransientLeaseIds()) {
            if (auto* lease = leaseManager->FindLease(leaseId)) {
                lease->UnrefTransiently();
            }
        }
        transaction->TransientLeaseIds().clear();
    }

    TTabletCellWriteManagerDynamicConfigPtr GetDynamicConfig() const
    {
        return Host_->GetDynamicConfig()->TabletCellWriteManager;
    }

    TCodicilGuard MakeCodicilGuard(TTransaction* transaction)
    {
        return TCodicilGuard([transaction] (TCodicilFormatter* formatter) {
            formatter->AppendString("TransactionId: ");
            formatter->AppendGuid(transaction->GetId());
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletCellWriteManagerPtr CreateTabletCellWriteManager(
    ITabletCellWriteManagerHostPtr host,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    IMutationForwarderPtr mutationForwarder)
{
    return New<TTabletCellWriteManager>(
        std::move(host),
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker),
        std::move(mutationForwarder));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
