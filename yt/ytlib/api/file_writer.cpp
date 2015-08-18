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
#include <ytlib/transaction_client/config.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
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
            Transaction_ = transactionManager->Attach(Options_.TransactionId);
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

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateAborted();

        if (Writer_->Write(data)) {
            return VoidFuture;
        }

        return Writer_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TFileWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const IClientPtr Client_;
    const TYPath Path_;
    const TFileWriterOptions Options_;
    const TFileWriterConfigPtr Config_;

    TTransactionPtr Transaction_;
    TTransactionPtr UploadTransaction_;

    IFileMultiChunkWriterPtr Writer_;

    TCellTag CellTag_ = InvalidCellTag;
    TObjectId ObjectId_;

    NLogging::TLogger Logger = ApiLogger;


    void DoOpen()
    {
        auto writerOptions = New<TMultiChunkWriterOptions>();

        {
            LOG_INFO("Requesting basic file attributes");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            req->set_permissions(static_cast<ui32>(EPermission::Write));
            SetTransactionId(req, Transaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting basic attributes of file %v",
                Path_);

            const auto& rsp = rspOrError.Value();
            ObjectId_ = FromProto<TObjectId>(rsp->object_id());
            CellTag_ = rsp->cell_tag();

            LOG_INFO("Basic file attributes received (ObjectId: %v, CellTag: %v)",
                ObjectId_,
                CellTag_);
        }

        {
            auto type = TypeFromId(ObjectId_);
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::File,
                    type);
            }
        }

        auto objectIdPath = FromObjectId(ObjectId_);

        {
            LOG_INFO("Requesting extended file attributes");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
            TObjectServiceProxy proxy(channel);

            auto req = TCypressYPathProxy::Get(objectIdPath);
            SetTransactionId(req, UploadTransaction_);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("account");
            attributeFilter.Keys.push_back("compression_codec");
            attributeFilter.Keys.push_back("erasure_codec");
            ToProto(req->mutable_attribute_filter(), attributeFilter);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting extended attributes of file %v",
                Path_);

            auto rsp = rspOrError.Value();
            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();
            writerOptions->ReplicationFactor = attributes.Get<int>("replication_factor");
            writerOptions->Account = attributes.Get<Stroka>("account");
            writerOptions->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            writerOptions->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

            LOG_INFO("Extended file attributes received (Account: %v)",
                writerOptions->Account);
        }

        {
            LOG_INFO("Starting file upload");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::Leader);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();

            {
                auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                for (const auto& id : Options_.PrerequisiteTransactionIds) {
                    auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                    ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                }
            }

            {
                auto transactionManager = Client_->GetTransactionManager();

                auto req = TFileYPathProxy::BeginUpload(objectIdPath);
                req->set_update_mode(static_cast<int>(Options_.Append ? EUpdateMode::Append : EUpdateMode::Overwrite));
                req->set_lock_mode(static_cast<int>(Options_.Append ? ELockMode::Shared : ELockMode::Exclusive));
                req->set_upload_transaction_title(Format("Upload to %v", Path_));
                req->set_upload_transaction_timeout(transactionManager->GetConfig()->DefaultTransactionTimeout.MicroSeconds());
                GenerateMutationId(req);
                SetTransactionId(req, Transaction_);
                batchReq->AddRequest(req, "begin_upload");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error starting upload to file %v",
                Path_);
            const auto& batchRsp = batchRspOrError.Value();

            {
                auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspBeginUpload>("begin_upload").Value();
                auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

                NTransactionClient::TTransactionAttachOptions options;
                options.PingAncestors = Options_.PingAncestors;
                options.AutoAbort = true;

                auto transactionManager = Client_->GetTransactionManager();
                UploadTransaction_ = transactionManager->Attach(uploadTransactionId, options);
                ListenTransaction(UploadTransaction_);

                LOG_INFO("File upload started (UploadTransactionId: %v)",
                    uploadTransactionId);
            }
        }

        TChunkListId chunkListId;

        {
            LOG_INFO("Requesting file upload parameters");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, CellTag_);
            TObjectServiceProxy proxy(channel);

            auto req = TFileYPathProxy::GetUploadParams(objectIdPath);
            SetTransactionId(req, UploadTransaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting upload parameters for file %v",
                Path_);

            const auto& rsp = rspOrError.Value();
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());

            LOG_INFO("File upload parameters received (ChunkListId: %v)",
                chunkListId);
        }

        Writer_ = CreateFileMultiChunkWriter(
            Config_,
            writerOptions,
            Client_->GetMasterChannel(EMasterChannelKind::Leader, CellTag_),
            UploadTransaction_->GetId(),
            chunkListId);

        WaitFor(Writer_->Open())
            .ThrowOnError();

        LOG_INFO("File opened");
    }

    void DoWrite(const TSharedRef& data)
    {
        ValidateAborted();

        if (!Writer_->Write(data)) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }
    }

    void DoClose()
    {
        ValidateAborted();

        LOG_INFO("Closing file");

        {
            auto result = WaitFor(Writer_->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to close file writer");
        }

        auto objectIdPath = FromObjectId(ObjectId_);

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
            for (const auto& id : Options_.PrerequisiteTransactionIds) {
                auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
            }
        }

        {
            auto req = TFileYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = Writer_->GetDataStatistics();
            SetTransactionId(req, UploadTransaction_);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error finishing upload to file %v",
            Path_);

        UploadTransaction_->Detach();

        LOG_INFO("File closed");
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
