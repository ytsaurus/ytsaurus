#include "stdafx.h"
#include "async_writer.h"
#include "table_writer.h"
#include "config.h"
#include "private.h"
#include "table_chunk_writer.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/actions/async_pipeline.h>

#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NChunkClient;

typedef TMultiChunkSequentialWriter<TTableChunkWriter> TTableMultiChunkWriter;

////////////////////////////////////////////////////////////////////////////////

class TAsyncTableWriter
    : public IAsyncWriter
    , public TTransactionListener
{
public:
    TAsyncTableWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        ITransactionPtr transaction,
        TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath,
        const TNullable<TKeyColumns>& keyColumns);

    virtual TAsyncError AsyncOpen() override;

    virtual void WriteRow(const TRow& row) override;

    virtual bool IsReady() override;

    virtual TAsyncError GetReadyEvent() override;

    virtual TAsyncError AsyncClose() override;

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const override;

    virtual i64 GetRowCount() const override;


private:
    typedef TAsyncTableWriter TThis;

    TFuture<TErrorOr<ITransactionPtr>> CreateUploadTransaction();
    void OnTransactionCreated(ITransactionPtr transactionOrError);

    TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> FetchTableInfo();
    TChunkListId OnInfoFetched(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    TAsyncError OpenChunkWriter(TChunkListId chunkListId);
    void OnChunkWriterOpened();

    TAsyncError CloseChunkWriter();
    TAsyncError SetIsSorted();
    TAsyncError CommitUploadTransaction();

    TTableWriterConfigPtr Config;
    TTableWriterOptionsPtr Options;

    NRpc::IChannelPtr MasterChannel;
    ITransactionPtr Transaction;
    TTransactionId TransactionId;
    TTransactionManagerPtr TransactionManager;
    NYPath::TRichYPath RichPath;

    bool IsOpen;
    bool IsClosed;
    NObjectClient::TObjectServiceProxy ObjectProxy;
    NLog::TTaggedLogger Logger;

    ITransactionPtr UploadTransaction;

    TIntrusivePtr<TTableMultiChunkWriter> Writer;
    TTableChunkWriter::TFacade* CurrentWriterFacade;

    TAsyncError WriteFuture_;
};


////////////////////////////////////////////////////////////////////////////////

TAsyncTableWriter::TAsyncTableWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const NYPath::TRichYPath& richPath,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , Options(New<TTableWriterOptions>())
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , RichPath(richPath)
    , IsOpen(false)
    , IsClosed(false)
    , ObjectProxy(masterChannel)
    , Logger(TableWriterLogger)
    , CurrentWriterFacade(nullptr)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(transactionManager);

    Options->KeyColumns = keyColumns;

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~richPath.GetPath(),
        ~ToString(TransactionId)));
}

TAsyncError TAsyncTableWriter::AsyncOpen()
{
    YCHECK(!IsOpen);
    YCHECK(!IsClosed);

    LOG_INFO("Opening table writer");

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(GetSyncInvoker())
        ->Add(BIND(&TThis::CreateUploadTransaction, this_))
        ->Add(BIND(&TThis::OnTransactionCreated, this_))
        ->Add(BIND(&TThis::FetchTableInfo, this_))
        ->Add(BIND(&TThis::OnInfoFetched, this_))
        ->Add(BIND(&TThis::OpenChunkWriter, this_))
        ->Add(BIND(&TThis::OnChunkWriterOpened, this_))
        ->Run();
}

TFuture<TErrorOr<ITransactionPtr>> TAsyncTableWriter::CreateUploadTransaction()
{
    LOG_INFO("Creating upload transaction");

    TTransactionStartOptions options;
    options.ParentId = TransactionId;
    options.EnableUncommittedAccounting = false;
    options.Attributes->Set("title", Sprintf("Table upload to %s", ~RichPath.GetPath()));
    return TransactionManager->AsyncStart(options).Apply(
        BIND([] (TErrorOr<ITransactionPtr> transactionOrError) -> TErrorOr<ITransactionPtr> {
            if (!transactionOrError.IsOK()) {
                return TError("Error creating upload transaction") << transactionOrError;
            }
            return transactionOrError;
        })
    );
}

void TAsyncTableWriter::OnTransactionCreated(ITransactionPtr transaction)
{
    UploadTransaction = transaction;
    auto uploadTransactionId = transaction->GetId();
    ListenTransaction(UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~ToString(uploadTransactionId));
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TAsyncTableWriter::FetchTableInfo()
{
    LOG_INFO("Requesting table info");

    auto path = RichPath.GetPath();

    bool overwrite = ExtractOverwriteFlag(RichPath.Attributes());
    bool clear = Options->KeyColumns.HasValue() || overwrite;
    auto uploadTransactionId = UploadTransaction->GetId();

    auto batchReq = ObjectProxy.ExecuteBatch();
    {
        auto req = TCypressYPathProxy::Get(path);
        SetTransactionId(req, uploadTransactionId);
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
        attributeFilter.Keys.push_back("type");
        attributeFilter.Keys.push_back("replication_factor");
        attributeFilter.Keys.push_back("channels");
        attributeFilter.Keys.push_back("compression_codec");
        attributeFilter.Keys.push_back("erasure_codec");
        if (Options->KeyColumns) {
            attributeFilter.Keys.push_back("row_count");
        }
        attributeFilter.Keys.push_back("account");
		attributeFilter.Keys.push_back("vital");
		ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_attributes");
    }

    {
        auto req = TTableYPathProxy::PrepareForUpdate(path);
        SetTransactionId(req, uploadTransactionId);
        NMetaState::GenerateMutationId(req);
        req->set_mode(clear ? EUpdateMode::Overwrite : EUpdateMode::Append);
        batchReq->AddRequest(req, "prepare_for_update");
    }

    return batchReq->Invoke();
}

TChunkListId TAsyncTableWriter::OnInfoFetched(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting table info");

    bool overwrite = ExtractOverwriteFlag(RichPath.Attributes());
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting table attributes");

        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        auto type = attributes.Get<EObjectType>("type");
        if (type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %s: expected %s, actual %s",
                ~RichPath.GetPath(),
                ~FormatEnum(EObjectType(EObjectType::Table)).Quote(),
                ~FormatEnum(type).Quote());
        }

        // TODO(psushin): Keep in sync with OnInputsReceived (operation_controller_detail.cpp).
        if (Options->KeyColumns && !overwrite) {
            if (attributes.Get<i64>("row_count") > 0) {
                THROW_ERROR_EXCEPTION("Cannot write sorted data into a non-empty table");
            }
        }

        Options->Channels = attributes.Get<TChannels>("channels");
        Options->ReplicationFactor = attributes.Get<int>("replication_factor");
        Options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        Options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec");
        Options->Account = attributes.Get<Stroka>("account");
        Options->ChunksVital = attributes.Get<bool>("vital");
    }

    TChunkListId  chunkListId;
    {
        auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing table for update");
        chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
    }
    LOG_INFO("Table info received (ChunkListId: %s)", ~ToString(chunkListId));

    return chunkListId;
}

TAsyncError TAsyncTableWriter::OpenChunkWriter(TChunkListId chunkListId)
{
    auto provider = New<TTableChunkWriterProvider>(
        Config,
        Options);

    Writer = New<TTableMultiChunkWriter>(
        Config,
        Options,
        provider,
        MasterChannel,
        UploadTransaction->GetId(),
        chunkListId);

    return Writer->AsyncOpen().Apply(
        BIND([] (TError error) -> TError{
            if (!error.IsOK()) {
                return TError("Error opening table chunk writer") << error;
            }
            return error;
        }));
}

void TAsyncTableWriter::OnChunkWriterOpened()
{
    if (Transaction) {
        ListenTransaction(Transaction);
    }

    CurrentWriterFacade = Writer->GetCurrentWriter();
    YASSERT(CurrentWriterFacade);

    IsOpen = true;

    LOG_INFO("Table writer opened");
}

void TAsyncTableWriter::WriteRow(const TRow& row)
{
    YCHECK(IsOpen);
    YASSERT(CurrentWriterFacade);

    CurrentWriterFacade->WriteRow(row);
}

bool TAsyncTableWriter::IsReady()
{
    if (IsAborted()) {
        WriteFuture_ = MakeFuture(TError("Transaction aborted"));
        return false;
    }

    CurrentWriterFacade = Writer->GetCurrentWriter();
    if (CurrentWriterFacade) {
        return true;
    } else {
        auto this_ = MakeStrong(this);
        auto readyEvent = NewPromise<TError>();
        WriteFuture_ = readyEvent;
        Writer->GetReadyEvent().Subscribe(BIND([=] (TError error) mutable {
            if (error.IsOK()) {
                this_->CurrentWriterFacade = this_->Writer->GetCurrentWriter();
                YCHECK(this_->CurrentWriterFacade);
            }
            readyEvent.Set(error);
        }));
        return false;
    }
}

TAsyncError TAsyncTableWriter::GetReadyEvent()
{
    return WriteFuture_;
}

TAsyncError TAsyncTableWriter::AsyncClose()
{
    if (!IsOpen) {
        return MakeFuture(TError());
    }

    LOG_INFO("Closing table writer");

    IsOpen = false;
    IsClosed = true;

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(GetSyncInvoker())
        ->Add(BIND(&TThis::CloseChunkWriter, this_))
        ->Add(BIND(&TThis::SetIsSorted, this_))
        ->Add(BIND(&TThis::CommitUploadTransaction, this_))
        ->Run();
}

TAsyncError TAsyncTableWriter::CloseChunkWriter()
{
    CheckAborted();

    LOG_INFO("Closing chunk writer");

    auto this_ = MakeStrong(this);
    return Writer->AsyncClose().Apply(
        BIND([this, this_] (TError error) -> TError {
            if (!error.IsOK()) {
                return TError("Error closing chunk writer") << error;
            }
            LOG_INFO("Chunk writer closed");
            return error;
        }));
}

TAsyncError TAsyncTableWriter::SetIsSorted()
{
    if (Options->KeyColumns) {
        auto path = RichPath.GetPath();
        auto keyColumns = Options->KeyColumns.Get();
        LOG_INFO("Marking table as sorted by %s",
            ~ConvertToYsonString(keyColumns, NYson::EYsonFormat::Text).Data());

        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction);
        NMetaState::GenerateMutationId(req);
        ToProto(req->mutable_key_columns(), keyColumns);

        auto this_ = MakeStrong(this);
        return ObjectProxy.Execute(req)
            .Apply(BIND([this, this_] (TTableYPathProxy::TRspSetSortedPtr rsp) -> TError {
                if (!rsp->IsOK()) {
                    return TError("Error marking table as sorted") << *rsp;
                }
                LOG_INFO("Table is marked as sorted");
                return *rsp;
            }));
    }
    return MakeFuture(TError());
}

TAsyncError TAsyncTableWriter::CommitUploadTransaction()
{
    LOG_INFO("Committing upload transaction");

    auto this_ = MakeStrong(this);
    return UploadTransaction->AsyncCommit()
        .Apply(BIND([this, this_] (TError error) -> TError {
            if (!error.IsOK()) {
                return TError("Error committing upload transaction") << error;
            }
            LOG_INFO("Upload transaction committed");
            LOG_INFO("Table writer closed");
            return TError();
        }));
}

const TNullable<TKeyColumns>& TAsyncTableWriter::GetKeyColumns() const
{
    return Writer->GetProvider()->GetKeyColumns();
}

i64 TAsyncTableWriter::GetRowCount() const
{
    return Writer->GetProvider()->GetRowCount();
}

////////////////////////////////////////////////////////////////////////////////

class TTableWriter
    : public ISyncWriter
{
public:
    TTableWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        ITransactionPtr transaction,
        TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath,
        const TNullable<TKeyColumns>& keyColumns)
            : Writer_(
                New<TAsyncTableWriter>(
                    config,
                    masterChannel,
                    transaction,
                    transactionManager,
                    richPath,
                    keyColumns))
    { }

    void WriteRow(const TRow& row) override
    {
        if (!Writer_->IsReady()) {
            Sync(~Writer_, &IAsyncWriter::GetReadyEvent);
        }
        Writer_->WriteRow(row);
    }

    void Close() override
    {
        Sync(~Writer_, &IAsyncWriter::AsyncClose);
    }

    const TNullable<TKeyColumns>& GetKeyColumns() const override
    {
        return Writer_->GetKeyColumns();
    }

    i64 GetRowCount() const override
    {
        return Writer_->GetRowCount();
    }

private:
    IAsyncWriterPtr Writer_;
};

////////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateAsyncTableWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        ITransactionPtr transaction,
        TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath,
        const TNullable<TKeyColumns>& keyColumns)
{
    return New<TAsyncTableWriter>(
        config,
        masterChannel,
        transaction,
        transactionManager,
        richPath,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

ISyncWriterPtr CreateSyncTableWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        ITransactionPtr transaction,
        TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath,
        const TNullable<TKeyColumns>& keyColumns)
{
    return New<TTableWriter>(
        config,
        masterChannel,
        transaction,
        transactionManager,
        richPath,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
