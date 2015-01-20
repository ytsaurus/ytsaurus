#include "stdafx.h"
#include "file_writer.h"
#include "config.h"
#include "client.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/helpers.h>

#include <core/logging/log.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>
#include <ytlib/file_client/file_chunk_writer.h>

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/helpers.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;
using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public TTransactionListener
    , public IFileWriter
{
public:
    TFileWriter(
        IClientPtr client,
        const TYPath& path,
        const TFileWriterOptions& options)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileWriterConfig>())
    {
        if (Options_.TransactionId != NullTransactionId) {
            auto transactionManager = Client_->GetTransactionManager();
            TTransactionAttachOptions attachOptions(Options_.TransactionId);
            attachOptions.AutoAbort = false;
            Transaction_ = transactionManager->Attach(attachOptions);
            ListenTransaction(Transaction_);
        }

        Logger.AddTag("Path: %v, TransactionId: %v",
            Path_,
            Options_.TransactionId);
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TFileWriter::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual TFuture<void> Write(const TRef& data) override
    {
        return BIND(&TFileWriter::DoWrite, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run(data);
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TFileWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    IClientPtr Client_;
    TYPath Path_;
    TFileWriterOptions Options_;
    TFileWriterConfigPtr Config_;

    TTransactionPtr Transaction_;
    TTransactionPtr UploadTransaction_;

    IFileMultiChunkWriterPtr Writer_;

    NLog::TLogger Logger = ApiLogger;


    void DoOpen()
    {
        CheckAborted();

        LOG_INFO("Creating upload transaction");

        {
            NTransactionClient::TTransactionStartOptions options;
            options.ParentId = Transaction_ ? Transaction_->GetId() : NullTransactionId;
            options.EnableUncommittedAccounting = false;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("File upload to %v", Path_));
            options.Attributes = std::move(attributes);
            options.PrerequisiteTransactionIds = Options_.PrerequisiteTransactionIds;

            auto transactionManager = Client_->GetTransactionManager();
            auto transactionOrError = WaitFor(transactionManager->Start(
                ETransactionType::Master,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error creating upload transaction");
            UploadTransaction_ = transactionOrError.Value();
        }

        LOG_INFO("Upload transaction created (TransactionId: %v)",
            UploadTransaction_->GetId());

        ListenTransaction(UploadTransaction_);

        LOG_INFO("Opening file");

        auto masterChannel = Client_->GetMasterChannel(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(masterChannel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
            for (const auto& id : Options_.PrerequisiteTransactionIds) {
                auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
            }
        }

        {
            auto req = TCypressYPathProxy::Get(Path_);
            SetTransactionId(req, UploadTransaction_);
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
            auto req = TFileYPathProxy::PrepareForUpdate(Path_);
            req->set_mode(static_cast<int>(Options_.Append ? EUpdateMode::Append : EUpdateMode::Overwrite));
            GenerateMutationId(req);
            SetTransactionId(req, UploadTransaction_);
            batchReq->AddRequest(req, "prepare_for_update");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error opening file");
        const auto& batchRsp = batchRspOrError.Value();

        auto writerOptions = New<TMultiChunkWriterOptions>();
        {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting file attributes");
            const auto& rsp = rspOrError.Value();

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            auto type = attributes.Get<EObjectType>("type");
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::File,
                    type);
            }

            writerOptions->ReplicationFactor = attributes.Get<int>("replication_factor");
            writerOptions->Account = attributes.Get<Stroka>("account");
            writerOptions->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            writerOptions->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
        }

        TChunkListId chunkListId;
        {
            auto rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error preparing file for update");
            const auto& rsp = rspOrError.Value();
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }

        LOG_INFO("File opened (Account: %v, ChunkListId: %v)",
            writerOptions->Account,
            chunkListId);

        Writer_ = CreateFileMultiChunkWriter(
            Config_,
            writerOptions,
            provider,
            masterChannel,
            UploadTransaction_->GetId(),
            chunkListId);

        {
            auto result = WaitFor(Writer_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }

    void DoWrite(const TRef& data)
    {
        CheckAborted();

        if (!Writer_->Write(data)) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }
    }

    void DoClose()
    {
        CheckAborted();

        LOG_INFO("Closing file writer and committing upload transaction");

        {
            auto result = WaitFor(Writer_->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to close file writer");
        }

        {
            TTransactionCommitOptions options;
            options.PrerequisiteTransactionIds = Options_.PrerequisiteTransactionIds;
            auto result = WaitFor(UploadTransaction_->Commit(options));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to commit upload transaction");
        }
    }

};

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const TYPath& path,
    const TFileWriterOptions& options)
{
    return New<TFileWriter>(client, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
