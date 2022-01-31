#include "tablet_commit_session.h"

#include "cell_commit_session.h"
#include "client.h"
#include "config.h"
#include "tablet_request_batcher.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi::NNative {

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

        auto* signatureGenerator = CellCommitSession_->GetSignatureGenerator();
        signatureGenerator->RegisterRequests(std::ssize(Batches_));
    }

    virtual TFuture<void> Invoke() override
    {
        YT_VERIFY(!Batches_.empty());
        for (const auto& batch : Batches_) {
            batch->Materialize(Config_->WriteRowsRequestCodec);
        }

        auto cellId = TabletInfo_->CellId;
        InvokeChannel_ = Client_->GetCellChannelOrThrow(cellId);
        InvokeNextBatch();
        return InvokePromise_;
    }

private:
    const IClientPtr Client_;
    const TConnectionConfigPtr Config_;
    const TTabletCommitOptions Options_;
    const TWeakPtr<TTransaction> Transaction_;
    const TTabletInfoPtr TabletInfo_;
    const TTableMountInfoPtr TableInfo_;
    const ICellCommitSessionPtr CellCommitSession_;

    ITabletRequestBatcherPtr Batcher_;

    const TLogger Logger;

    std::vector<std::unique_ptr<ITabletRequestBatcher::TBatch>> Batches_;

    bool IsVersioned_ = false;

    IChannelPtr InvokeChannel_;
    int InvokeBatchIndex_ = 0;
    const TPromise<void> InvokePromise_ = NewPromise<void>();

    void InvokeNextBatch()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (InvokeBatchIndex_ >= std::ssize(Batches_)) {
            InvokePromise_.Set(TError());
            return;
        }

        const auto& batch = Batches_[InvokeBatchIndex_++];

        auto transaction = Transaction_.Lock();
        if (!transaction) {
            return;
        }

        TTabletServiceProxy proxy(InvokeChannel_);
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
        auto* signatureGenerator = CellCommitSession_->GetSignatureGenerator();
        req->set_signature(signatureGenerator->GenerateSignature());
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

        YT_LOG_DEBUG("Sending transaction rows (BatchIndex: %v/%v, RowCount: %v, Signature: %x, "
            "Versioned: %v, UpstreamReplicaId: %v)",
            InvokeBatchIndex_,
            Batches_.size(),
            batch->RowCount,
            req->signature(),
            req->versioned(),
            Options_.UpstreamReplicaId);

        req->Invoke().Subscribe(
            BIND(&TTabletCommitSession::OnResponse, MakeStrong(this)));
    }

    void OnResponse(const TTabletServiceProxy::TErrorOrRspWritePtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            auto error = TError("Error sending transaction rows")
                << TErrorAttribute("table_id", TableInfo_->TableId)
                << TErrorAttribute("tablet_id", TabletInfo_->TabletId)
                << rspOrError;
            YT_LOG_DEBUG(error);
            const auto& tableMountCache = Client_->GetTableMountCache();
            tableMountCache->InvalidateOnError(error, /*forceRetry*/ true);
            InvokePromise_.Set(error);
            return;
        }

        auto owner = Transaction_.Lock();
        if (!owner) {
            return;
        }

        YT_LOG_DEBUG("Transaction rows sent successfully (BatchIndex: %v/%v)",
            InvokeBatchIndex_,
            Batches_.size());

        InvokeNextBatch();
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

} // namespace NYT::NApi::NNative
