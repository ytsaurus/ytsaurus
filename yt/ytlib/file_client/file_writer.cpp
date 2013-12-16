#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_writer.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/rpc_helpers.h>

#include <ytlib/hydra/rpc_helpers.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    TTransactionPtr transaction,
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

TAsyncWriter::~TAsyncWriter()
{ }

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
    return TransactionManager->Start(options).Apply(
            BIND(&TThis::OnUploadTransactionStarted, MakeStrong(this)));
}

TAsyncError TAsyncWriter::OnUploadTransactionStarted(TErrorOr<TTransactionPtr> transactionOrError)
{
    if (!transactionOrError.IsOK()) {
        return MakeFuture(TError("Error creating upload transaction")
            << transactionOrError);
    }

    UploadTransaction = transactionOrError.GetValue();
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
        attributeFilter.Keys.push_back("type");
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
        NHydra::GenerateMutationId(req);
        SetTransactionId(req, UploadTransaction);
        batchReq->AddRequest(req, "prepare_for_update");
    }

    return batchReq->Invoke().Apply(BIND(&TThis::OnFileInfoReceived, MakeStrong(this)));
}

TAsyncError TAsyncWriter::OnFileInfoReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (!batchRsp->IsOK()) {
        return MakeFuture(TError("Error requesting file info")
            << *batchRsp);
    }

    auto options = New<TMultiChunkWriterOptions>();
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        if (!rsp->IsOK()) {
            return MakeFuture(TError("Error getting file attributes")
                << *rsp);
        }

        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        auto type = attributes.Get<EObjectType>("type");
        if (type != EObjectType::File) {
            return MakeFuture(TError("Invalid type of %s: expected %s, actual %s",
                ~RichPath.GetPath(),
                ~FormatEnum(EObjectType(EObjectType::File)).Quote(),
                ~FormatEnum(type).Quote()));
        }

        options->ReplicationFactor = attributes.Get<int>("replication_factor");
        options->Account = attributes.Get<Stroka>("account");
        options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
    }

    TChunkListId chunkListId;
    {
        auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        if (!rsp->IsOK()) {
            return MakeFuture(TError("Error preparing file for update")
                << *rsp);
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

    if (auto writer = Writer->GetCurrentWriter()) {
        writer->Write(data);
        return MakeFuture(TError());
    } else {
        auto this_ = MakeStrong(this);
        return Writer->GetReadyEvent().Apply(
            BIND([this, this_, data] (TError error) -> TError {
                RETURN_IF_ERROR(error);
                Writer->GetCurrentWriter()->Write(data);
                return TError();
            }));
    }
}

void TAsyncWriter::Close()
{
    if (IsAborted()) {
        return;
    }

    LOG_INFO("Closing file writer and committing upload transaction");

    {
        auto error = WaitFor(Writer->AsyncClose());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to close file writer");
    }

    {
        auto error = WaitFor(UploadTransaction->Commit(NHydra::NullMutationId));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to commit upload transaction");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
