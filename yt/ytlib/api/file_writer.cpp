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

#include <ytlib/object_client/helpers.h>

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

    NLogging::TLogger Logger = ApiLogger;


    void DoOpen()
    {
        {
            LOG_INFO("Creating upload transaction");

            NTransactionClient::TTransactionStartOptions options;
            options.ParentId = Transaction_ ? Transaction_->GetId() : NullTransactionId;
            options.EnableUncommittedAccounting = false;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Upload to %v", Path_));
            options.Attributes = std::move(attributes);
            options.PrerequisiteTransactionIds = Options_.PrerequisiteTransactionIds;

            auto transactionManager = Client_->GetTransactionManager();
            auto transactionOrError = WaitFor(transactionManager->Start(
                ETransactionType::Master,
                options));

            THROW_ERROR_EXCEPTION_IF_FAILED(
                transactionOrError,
                "Error creating upload transaction");

            UploadTransaction_ = transactionOrError.Value();
            ListenTransaction(UploadTransaction_);

            LOG_INFO("Upload transaction created (TransactionId: %v)",
                UploadTransaction_->GetId());
        }

        auto cellTag = InvalidCellTag;
        TObjectId objectId;
        auto writerOptions = New<TMultiChunkWriterOptions>();

        {
            auto channel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
            TObjectServiceProxy proxy(channel);

            LOG_INFO("Requesting basic file attributes");

            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            SetTransactionId(req, Transaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting basic attributes of file %v",
                Path_);

            const auto& rsp = rspOrError.Value();
            objectId = FromProto<TObjectId>(rsp->object_id());
            cellTag = rsp->cell_tag();

            LOG_INFO("Basic file attributes received (ObjectId: %v, CellTag: %v)",
                objectId,
                cellTag);
        }

        {
            auto type = TypeFromId(objectId);
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::File,
                    type);
            }
        }

        auto objectIdPath = FromObjectId(objectId);

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

        TChunkListId chunkListId;
        IChannelPtr writerChannel;

        auto makePrepareForUpdateRequest = [&] () -> TFileYPathProxy::TReqPrepareForUpdatePtr {
            auto req = TFileYPathProxy::PrepareForUpdate(objectIdPath);
            req->set_update_mode(static_cast<int>(Options_.Append ? EUpdateMode::Append : EUpdateMode::Overwrite));
            req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
            GenerateMutationId(req);
            SetTransactionId(req, UploadTransaction_);
            return req;
        };

        auto handlePrepareForUpdateResponse = [&] (
            TFileYPathProxy::TRspPrepareForUpdatePtr rsp,
            IChannelPtr channel)
        {
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            writerChannel = channel;

            LOG_INFO("File prepared for update (ChunkListId: %v)",
                chunkListId);
        };

        {
            LOG_INFO("Preparing file for update at primary master");

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
                auto req = makePrepareForUpdateRequest();
                batchReq->AddRequest(req, "prepare_for_update");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error preparing file %v for update at primary master",
                Path_);
            const auto& batchRsp = batchRspOrError.Value();

            if (cellTag == Client_->GetConnection()->GetPrimaryMasterCellTag()) {
                auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update")
                    .Value();
                handlePrepareForUpdateResponse(rsp, channel);
            }
        }

        if (cellTag != Client_->GetConnection()->GetPrimaryMasterCellTag()) {
            LOG_INFO("Preparing file for update at secondary master");

            auto channel = Client_->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
            TObjectServiceProxy proxy(channel);

            auto req = makePrepareForUpdateRequest();

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error preparing file %v for update at secondary master",
                Path_);

            const auto& rsp = rspOrError.Value();
            handlePrepareForUpdateResponse(rsp, channel);
        }

        Writer_ = CreateFileMultiChunkWriter(
            Config_,
            writerOptions,
            writerChannel,
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
