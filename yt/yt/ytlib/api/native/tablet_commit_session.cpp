#include "tablet_commit_session.h"

#include "cell_commit_session.h"
#include "client.h"
#include "config.h"
#include "tablet_request_batcher.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/backoff_strategy_config.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletCommitSession
    : public ITabletCommitSession
{
public:
    TTabletCommitSession(
        IClientPtr client,
        TTabletCommitOptions options,
        TWeakPtr<TTransaction> transaction,
        ICellCommitSessionProviderPtr cellCommitSessionProvider,
        TTabletInfoPtr tabletInfo,
        TTableMountInfoPtr tableInfo,
        TLogger logger)
        : Client_(std::move(client))
        , Config_(Client_->GetNativeConnection()->GetConfig())
        , Options_(std::move(options))
        , Transaction_(std::move(transaction))
        , TabletInfo_(std::move(tabletInfo))
        , TableInfo_(std::move(tableInfo))
        , CellCommitSession_(cellCommitSessionProvider->GetOrCreateCellCommitSession(TabletInfo_->CellId))
        , Logger(logger.WithTag("TabletId: %v", TabletInfo_->TabletId))
    {
        auto maxRowsPerTablet = Client_->GetOptions().GetAuthenticatedUser() == NSecurityClient::ReplicatorUserName
            ? std::nullopt
            : std::make_optional(Config_->MaxRowsPerTransaction);

        const auto& columnEvaluatorCache = Client_->GetNativeConnection()->GetColumnEvaluatorCache();
        auto columnEvaluator = columnEvaluatorCache->Find(TableInfo_->Schemas[ETableSchemaKind::Primary]);
        Batcher_ = CreateTabletRequestBatcher(
            TTabletRequestBatcherOptions{
                .MaxRowsPerBatch = Config_->MaxRowsPerWriteRequest,
                .MaxDataWeightPerBatch = Config_->MaxDataWeightPerWriteRequest,
                .MaxRowsPerTablet = maxRowsPerTablet
            },
            TableInfo_->Schemas[ETableSchemaKind::Primary],
            std::move(columnEvaluator));
    }

    void SubmitUnversionedRow(
        EWireProtocolCommand command,
        TUnversionedRow row,
        TLockMask lockMask) override
    {
        YT_VERIFY(!IsVersioned_);

        Batcher_->SubmitUnversionedRow(command, row, lockMask);
    }

    virtual void SubmitVersionedRow(TTypeErasedRow row) override
    {
        IsVersioned_ = true;

        Batcher_->SubmitVersionedRow(row);
    }

    virtual void PrepareRequests() override
    {
        Batches_ = Batcher_->PrepareBatches();

        auto batchCount = std::ssize(Batches_);
        CellCommitSession_
            ->GetPrepareSignatureGenerator()
            ->RegisterRequests(batchCount);
        CellCommitSession_
            ->GetCommitSignatureGenerator()
            ->RegisterRequests(batchCount);
    }

    // NB: Concurrent #Invoke calls with different retry indices are possible.
    virtual TFuture<void> Invoke(int retryIndex) override
    {
        if (retryIndex == 0) {
            YT_VERIFY(!Batches_.empty());
            for (const auto& batch : Batches_) {
                batch->Materialize(Config_->WriteRowsRequestCodec);
            }

            CalculateBatchSignatures();
        }

        auto cellId = TabletInfo_->CellId;

        auto commitContext = New<TCommitContext>();
        commitContext->RetryIndex = retryIndex;
        commitContext->CellChannel = Client_->GetCellChannelOrThrow(cellId);

        InvokeNextBatch(commitContext);

        return commitContext->CommitPromise;
    }

    void MemorizeHunkInfo(const THunkChunksInfo& hunkInfo) override
    {
        if (!HunkChunksInfo_) {
            HunkChunksInfo_ = std::move(hunkInfo);
            return;
        }

        YT_VERIFY(hunkInfo.CellId == HunkChunksInfo_->CellId);
        YT_VERIFY(hunkInfo.HunkTabletId == HunkChunksInfo_->HunkTabletId);
        YT_VERIFY(hunkInfo.MountRevision == HunkChunksInfo_->MountRevision);
        for (const auto& [hunkChunkId, otherRef] : hunkInfo.HunkChunkRefs) {
            // TODO(aleksandra-zh): helper
            auto& ref = HunkChunksInfo_->HunkChunkRefs[hunkChunkId];
            ref.ChunkId = otherRef.ChunkId;
            ref.ErasureCodec = otherRef.ErasureCodec;
            ref.HunkCount += otherRef.HunkCount;
            ref.TotalHunkLength += otherRef.TotalHunkLength;
        }
    }

private:
    const IClientPtr Client_;
    const TConnectionDynamicConfigPtr Config_;
    const TTabletCommitOptions Options_;
    const TWeakPtr<TTransaction> Transaction_;
    const TTabletInfoPtr TabletInfo_;
    const TTableMountInfoPtr TableInfo_;
    const ICellCommitSessionPtr CellCommitSession_;
    const TLogger Logger;

    std::optional<THunkChunksInfo> HunkChunksInfo_;

    ITabletRequestBatcherPtr Batcher_;

    std::vector<std::unique_ptr<ITabletRequestBatcher::TBatch>> Batches_;

    struct TBatchSignatures
    {
        TTransactionSignature PrepareSignature;
        TTransactionSignature CommitSignature;
    };
    std::vector<TBatchSignatures> BatchSignatures_;

    bool IsVersioned_ = false;

    struct TCommitContext final
    {
        int RetryIndex;
        int BatchIndex = 0;
        IChannelPtr CellChannel;
        TPromise<void> CommitPromise = NewPromise<void>();
    };
    using TCommitContextPtr = TIntrusivePtr<TCommitContext>;

    void InvokeNextBatch(TCommitContextPtr commitContext)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto batchIndex = commitContext->BatchIndex;

        if (batchIndex >= std::ssize(Batches_)) {
            commitContext->CommitPromise.Set(TError());
            return;
        }

        if (commitContext->CommitPromise.IsCanceled()) {
            return;
        }

        const auto& batch = Batches_[batchIndex];

        auto transaction = Transaction_.Lock();
        if (!transaction) {
            return;
        }

        TTabletServiceProxy proxy(commitContext->CellChannel);
        proxy.SetDefaultTimeout(Config_->WriteRowsTimeout);
        proxy.SetDefaultAcknowledgementTimeout(std::nullopt);

        auto req = proxy.Write();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_transaction_id(), transaction->GetId());
        if (transaction->GetAtomicity() == EAtomicity::Full) {
            req->set_transaction_start_timestamp(transaction->GetStartTimestamp());
            req->set_transaction_timeout(ToProto<i64>(transaction->GetTimeout()));
        }
        ToProto(req->mutable_tablet_id(), TabletInfo_->TabletId);
        req->set_mount_revision(TabletInfo_->MountRevision);
        req->set_durability(static_cast<int>(transaction->GetDurability()));

        const auto& batchSignatures = BatchSignatures_[batchIndex];
        req->set_prepare_signature(batchSignatures.PrepareSignature);
        req->set_commit_signature(batchSignatures.CommitSignature);

        req->set_generation(commitContext->RetryIndex);

        req->set_request_codec(static_cast<int>(Config_->WriteRowsRequestCodec));
        req->set_row_count(batch->RowCount);
        req->set_data_weight(batch->DataWeight);
        req->set_versioned(IsVersioned_);
        for (const auto& replicaInfo : TableInfo_->Replicas) {
            if (replicaInfo->Mode == ETableReplicaMode::Sync) {
                ToProto(req->add_sync_replica_ids(), replicaInfo->ReplicaId);
            }
        }
        if (Options_.UpstreamReplicaId) {
            ToProto(req->mutable_upstream_replica_id(), Options_.UpstreamReplicaId);
        }
        if (const auto& replicationCard = Options_.ReplicationCard) {
            req->set_replication_era(replicationCard->Era);
        }
        req->Attachments().push_back(batch->RequestData);

        if (HunkChunksInfo_) {
            ToProto(req->mutable_hunk_chunks_info(), *HunkChunksInfo_);
        }

        YT_LOG_DEBUG("Sending transaction rows (BatchIndex: %v/%v, RowCount: %v, "
            "PrepareSignature: %x, CommitSignature: %x, Versioned: %v, UpstreamReplicaId: %v%v)",
            batchIndex,
            Batches_.size(),
            batch->RowCount,
            req->prepare_signature(),
            req->commit_signature(),
            req->versioned(),
            Options_.UpstreamReplicaId,
            MakeFormatterWrapper([&] (auto* builder) {
                if (HunkChunksInfo_) {
                    builder->AppendFormat(", HunkCellId: %v, HunkTabletId: %v, HunkChunkRefs: %v",
                        HunkChunksInfo_->CellId,
                        HunkChunksInfo_->HunkTabletId,
                        MakeFormattableView(
                            HunkChunksInfo_->HunkChunkRefs,
                            [&] (auto* builder, const auto& info) {
                                builder->AppendFormat("%v: %v", info.first, info.second.HunkCount);
                            }));
                }
            }));

        req->Invoke().Subscribe(
            BIND(&TTabletCommitSession::OnResponse, MakeStrong(this), commitContext));
    }

    void OnResponse(
        TCommitContextPtr commitContext,
        const TTabletServiceProxy::TErrorOrRspWritePtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            auto error = TError("Error sending transaction rows")
                << TErrorAttribute("table_id", TableInfo_->TableId)
                << TErrorAttribute("tablet_id", TabletInfo_->TabletId)
                << TErrorAttribute("cell_id", TabletInfo_->CellId)
                << rspOrError;
            YT_LOG_DEBUG(error);
            const auto& tableMountCache = Client_->GetTableMountCache();
            tableMountCache->InvalidateOnError(error, /*forceRetry*/ true);
            commitContext->CommitPromise.Set(error);
            return;
        }

        auto owner = Transaction_.Lock();
        if (!owner) {
            return;
        }

        YT_LOG_DEBUG("Transaction rows sent successfully (BatchIndex: %v/%v)",
            commitContext->BatchIndex,
            Batches_.size());

        commitContext->BatchIndex++;
        InvokeNextBatch(commitContext);
    }

    void CalculateBatchSignatures()
    {
        YT_VERIFY(BatchSignatures_.empty());
        BatchSignatures_.resize(Batches_.size());

        for (int batchIndex = 0; batchIndex < std::ssize(Batches_); ++batchIndex) {
            auto& batchSignatures = BatchSignatures_[batchIndex];

            auto prepareSignature = CellCommitSession_
                ->GetPrepareSignatureGenerator()
                ->GenerateSignature();
            auto commitSignature = CellCommitSession_
                ->GetCommitSignatureGenerator()
                ->GenerateSignature();
            batchSignatures = TBatchSignatures{
                .PrepareSignature = prepareSignature,
                .CommitSignature = commitSignature,
            };
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletCommitSessionPtr CreateTabletCommitSession(
    IClientPtr client,
    TTabletCommitOptions options,
    TWeakPtr<TTransaction> transaction,
    ICellCommitSessionProviderPtr cellCommitSessionProvider,
    TTabletInfoPtr tabletInfo,
    TTableMountInfoPtr tableInfo,
    TLogger logger)
{
    return New<TTabletCommitSession>(
        std::move(client),
        std::move(options),
        std::move(transaction),
        std::move(cellCommitSessionProvider),
        std::move(tabletInfo),
        std::move(tableInfo),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TTabletSessionsCommitter
    : public TRefCounted
{
public:
    TTabletSessionsCommitter(
        std::vector<ITabletCommitSessionPtr> sessions,
        TSerializableExponentialBackoffOptionsPtr backoffOptions,
        TLogger logger,
        TTransactionCounters counters)
        : Logger(std::move(logger))
        , Counters_(std::move(counters))
        , Sessions_(std::move(sessions))
        , BackoffStrategy_(*backoffOptions)
    { }

    TFuture<void> Run()
    {
        Counters_.TabletSessionCommitCounter.Increment();

        CommitSessions();
        return Promise_;
    }

private:
    const TLogger Logger;

    const TTransactionCounters Counters_;

    const std::vector<ITabletCommitSessionPtr> Sessions_;

    TBackoffStrategy BackoffStrategy_;

    const TPromise<void> Promise_ = NewPromise<void>();

    std::vector<TError> Errors_;

    void CommitSessions()
    {
        if (Promise_.IsCanceled()) {
            return;
        }

        auto retryIndex = BackoffStrategy_.GetInvocationIndex();

        YT_LOG_DEBUG("Committing tablet sessions (AttemptIndex: %v/%v)",
            retryIndex,
            BackoffStrategy_.GetInvocationCount());

        YT_UNUSED_FUTURE(DoCommitSessions(retryIndex)
            .Apply(BIND(&TTabletSessionsCommitter::OnSessionsCommitted, MakeStrong(this))));
    }

    void OnSessionsCommitted(const TError& error)
    {
        auto retryIndex = BackoffStrategy_.GetInvocationIndex();

        if (error.IsOK()) {
            YT_LOG_DEBUG("Tablet sessions committed (AttemptIndex: %v/%v)",
                retryIndex,
                BackoffStrategy_.GetInvocationCount());

            Counters_.SuccessfulTabletSessionCommitCounter.Increment();
            if (retryIndex > 0) {
                Counters_.RetriedSuccessfulTabletSessionCommitCounter.Increment();
            }

            Promise_.Set();
            return;
        }

        Errors_.push_back(error);

        YT_LOG_DEBUG(error, "Tablet sessions commit attempt failed (AttemptIndex: %v/%v)",
            retryIndex,
            BackoffStrategy_.GetInvocationCount());

        if (BackoffStrategy_.Next()) {
            auto backoff = BackoffStrategy_.GetBackoff();
            YT_LOG_DEBUG("Waiting before next tablet sessions commit attempt (Backoff: %v)",
                backoff);

            auto backoffFuture = TDelayedExecutor::MakeDelayed(backoff);
            YT_UNUSED_FUTURE(backoffFuture.Apply(BIND(&TTabletSessionsCommitter::CommitSessions, MakeStrong(this))));
        } else {
            auto error = TError("Failed to commit tablet sessions")
                << Errors_;
            YT_LOG_DEBUG(error);

            Counters_.FailedTabletSessionCommitCounter.Increment();

            Promise_.Set(error);
        }
    }

    TFuture<void> DoCommitSessions(int retryIndex)
    {
        std::vector<TFuture<void>> commitFutures;
        commitFutures.reserve(Sessions_.size());
        for (const auto& session : Sessions_) {
            commitFutures.push_back(session->Invoke(retryIndex));
        }

        return AllSucceeded(std::move(commitFutures));
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> CommitTabletSessions(
    std::vector<ITabletCommitSessionPtr> sessions,
    TSerializableExponentialBackoffOptionsPtr backoffOptions,
    TLogger logger,
    TTransactionCounters counters)
{
    auto committer = New<TTabletSessionsCommitter>(
        std::move(sessions),
        std::move(backoffOptions),
        std::move(logger),
        std::move(counters));
    return committer->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
