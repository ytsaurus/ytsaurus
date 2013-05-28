#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_writer.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const TRichYPath& richPath)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionManager(transactionManager)
    , RichPath(richPath.Simplify())
    , Logger(FileWriterLogger)
{
    YCHECK(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

TAsyncError TAsyncWriter::AsyncOpen()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction aborted"));
    }

    LOG_INFO("Creating upload transaction");
    TTransactionStartOptions options;
    options.ParentId = Transaction ? Transaction->GetId() : NullTransactionId;
    options.EnableUncommittedAccounting = false;
    options.Attributes->Set("title", Sprintf("File upload to %s", ~RichPath.GetPath()));
    return TransactionManager->AsyncStart(options).Apply(
            BIND(&TThis::OnUploadTransactionStarted, MakeStrong(this)));
}

TAsyncError TAsyncWriter::OnUploadTransactionStarted(TValueOrError<ITransactionPtr> transactionOrError)
{
    if (!transactionOrError.IsOK()) {
        return MakeFuture(TError("Error creating upload transaction") << transactionOrError);
    }
    
    UploadTransaction = transactionOrError.Value();
    ListenTransaction(UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~ToString(UploadTransaction->GetId()));

    TObjectServiceProxy proxy(MasterChannel);

    LOG_INFO("Requesting file info");
    
    auto batchReq = proxy.ExecuteBatch();

    auto path = RichPath.GetPath();
    bool overwrite = NChunkClient::ExtractOverwriteFlag(RichPath.Attributes());
    {
        auto req = TCypressYPathProxy::Get(path);
        SetTransactionId(req, UploadTransaction);
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
        attributeFilter.Keys.push_back("replication_factor");
        attributeFilter.Keys.push_back("account");
        attributeFilter.Keys.push_back("compression_codec");
        attributeFilter.Keys.push_back("erasure_codec");
        ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_attributes");
    }

    {
        auto req = TFileYPathProxy::PrepareForUpdate(path);
        req->set_mode(overwrite ? EUpdateMode::Overwrite : EUpdateMode::Append);
        NMetaState::GenerateMutationId(req);
        SetTransactionId(req, UploadTransaction);
        batchReq->AddRequest(req, "prepare_for_update");
    }

    return batchReq->Invoke().Apply(BIND(&TThis::OnFileInfoReceived, MakeStrong(this)));
}

TAsyncError TAsyncWriter::OnFileInfoReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (!batchRsp->IsOK()) {
        return MakeFuture(TError("Error preparing file for update"));
    }

    auto options = New<TMultiChunkWriterOptions>();
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        if (!rsp->IsOK()) {
            return MakeFuture(TError("Error getting file attributes"));
        }

        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        options->ReplicationFactor = attributes.Get<int>("replication_factor");
        options->Account = attributes.Get<Stroka>("account");
        options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
    }

    TChunkListId chunkListId;
    {
        auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        if (!rsp->IsOK()) {
            return MakeFuture(TError("Error preparing file for update"));
        }
        chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    LOG_INFO("File info received (Account: %s, ChunkListId: %s)",
        ~options->Account,
        ~ToString(chunkListId));

    auto provider = New<TFileChunkWriterProvider>(
        Config,
        options);

    Writer = New<TWriter>(
        Config,
        options,
        provider,
        MasterChannel,
        UploadTransaction->GetId(),
        chunkListId);
    return Writer->AsyncOpen();
}


TAsyncError TAsyncWriter::AsyncWrite(const TRef& data)
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction aborted"));
    }

    auto future = MakeFuture(TError());
    if (!Writer->GetCurrentWriter()) {
        future = Writer->GetReadyEvent();
    }

    auto this_ = MakeStrong(this);
    return future.Apply(BIND([this, this_, data] (TError error) {
        RETURN_IF_ERROR(error);
        Writer->GetCurrentWriter()->Write(data);
        return TError();
    }));
}

TAsyncError TAsyncWriter::AsyncClose()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction aborted"));
    }

    LOG_INFO("Closing file writer and committing upload transaction");
    auto this_ = MakeStrong(this);
    return ConvertToTErrorFuture(
        StartAsyncPipeline(GetSyncInvoker())
            ->Add(BIND(&TWriter::AsyncClose, Writer))
            ->Add(BIND(&NTransactionClient::ITransaction::AsyncCommit, UploadTransaction, NMetaState::NullMutationId))
            //->Add(BIND([this_] () {}))
            ->Run()
    );
}

////////////////////////////////////////////////////////////////////////////////

TSyncWriter::TSyncWriter(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const TRichYPath& richPath)
        : AsyncWriter_(New<TAsyncWriter>(
            config,
            masterChannel,
            transaction,
            transactionManager,
            richPath))
{ }

void TSyncWriter::Open()
{
    Sync(~AsyncWriter_, &TAsyncWriter::AsyncOpen);
}

void TSyncWriter::Write(const TRef& data)
{
    Sync(~AsyncWriter_, &TAsyncWriter::AsyncWrite, data);
}

void TSyncWriter::Close()
{
    Sync(~AsyncWriter_, &TAsyncWriter::AsyncClose);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
