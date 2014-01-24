#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_writer.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/chunk_client/private.h>
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
    YCHECK(Config);
    YCHECK(MasterChannel);
    YCHECK(TransactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        Transaction ? ~ToString(Transaction->GetId()) : ~ToString(NullTransactionId)));

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

TAsyncWriter::~TAsyncWriter()
{ }

TAsyncError TAsyncWriter::Open()
{
    return BIND(&TAsyncWriter::DoOpen, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TError TAsyncWriter::DoOpen()
{
    try {
        CheckAborted();

        LOG_INFO("Creating upload transaction");

        {
            TTransactionStartOptions options;
            options.ParentId = Transaction ? Transaction->GetId() : NullTransactionId;
            options.EnableUncommittedAccounting = false;
            options.Attributes->Set("title", Sprintf("File upload to %s", ~RichPath.GetPath()));
            auto transactionOrError = WaitFor(TransactionManager->Start(options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error creating upload transaction");
            UploadTransaction = transactionOrError.GetValue();
        }

        LOG_INFO("Upload transaction created (TransactionId: %s)",
            ~ToString(UploadTransaction->GetId()));

        ListenTransaction(UploadTransaction);

        LOG_INFO("Requesting file info");

        TObjectServiceProxy proxy(MasterChannel);
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

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting file info");

        auto writerOptions = New<TMultiChunkWriterOptions>();
        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting file attributes");

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            auto type = attributes.Get<EObjectType>("type");
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %s: expected %s, actual %s",
                    ~RichPath.GetPath(),
                    ~FormatEnum(EObjectType(EObjectType::File)).Quote(),
                    ~FormatEnum(type).Quote());
            }

            writerOptions->ReplicationFactor = attributes.Get<int>("replication_factor");
            writerOptions->Account = attributes.Get<Stroka>("account");
            writerOptions->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            writerOptions->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
        }

        TChunkListId chunkListId;
        {
            auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing file for update");
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }

        LOG_INFO("File info received (Account: %s, ChunkListId: %s)",
            ~writerOptions->Account,
            ~ToString(chunkListId));

        auto provider = New<TFileChunkWriterProvider>(
            Config,
            writerOptions);

        Writer = New<TWriter>(
            Config,
            writerOptions,
            provider,
            MasterChannel,
            UploadTransaction->GetId(),
            chunkListId);

        {
            auto result = WaitFor(Writer->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        return TError();
    } catch (const std::exception& ex) {
        return ex;
    }
}

TAsyncError TAsyncWriter::Write(const TRef& data)
{
    return BIND(&TAsyncWriter::DoWrite, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run(data);
}

TError TAsyncWriter::DoWrite(const TRef& data)
{
    try {
        CheckAborted();

        while (!Writer->GetCurrentWriter()) {
            auto result = WaitFor(Writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        
        Writer->GetCurrentWriter()->Write(data);
        
        return TError();
    } catch (const std::exception& ex) {
        return ex;
    }
}

TAsyncError TAsyncWriter::Close()
{
    return BIND(&TAsyncWriter::DoClose, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TError TAsyncWriter::DoClose()
{
    try {
        CheckAborted();

        LOG_INFO("Closing file writer and committing upload transaction");

        {
            auto result = WaitFor(Writer->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to close file writer");
        }

        {
            auto result = WaitFor(UploadTransaction->Commit(NHydra::NullMutationId));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to commit upload transaction");
        }

        return TError();
    } catch (const std::exception& ex) {
        return ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
