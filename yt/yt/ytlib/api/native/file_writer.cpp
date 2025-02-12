#include "file_writer.h"
#include "private.h"
#include "client.h"
#include "config.h"
#include "transaction.h"
#include "connection.h"
#include "private.h"

#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_chunk_writer.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NCrypto;
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
using namespace NApi;
using namespace NTransactionClient;
using namespace NFileClient;
using namespace NHiveClient;

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public TTransactionListener
    , public IFileWriter
{
public:
    TFileWriter(
        IClientPtr client,
        const TRichYPath& path,
        const TFileWriterOptions& options)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileWriterConfig>())
        , Logger(ApiLogger().WithTag("Path: %v, TransactionId: %v",
            Path_.GetPath(),
            Options_.TransactionId))
    { }

    TFuture<void> Open() override
    {
        return BIND(&TFileWriter::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        try {
            ValidateAborted();

            if (Options_.ComputeMD5 && MD5Hasher_) {
                MD5Hasher_->Append(data);
            }

            if (Writer_->Write(data)) {
                return VoidFuture;
            }

            return Writer_->GetReadyEvent();
        } catch (const std::exception& ex) {
            return MakeFuture<void>(ex);
        }
    }

    TFuture<void> Close() override
    {
        return BIND(&TFileWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const IClientPtr Client_;
    const TRichYPath Path_;
    const TFileWriterOptions Options_;
    const TFileWriterConfigPtr Config_;

    NApi::ITransactionPtr Transaction_;
    NApi::ITransactionPtr UploadTransaction_;

    std::optional<TMD5Hasher> MD5Hasher_;

    IFileMultiChunkWriterPtr Writer_;

    TCellTag NativeCellTag_ = InvalidCellTag;
    TCellTag ExternalCellTag_ = InvalidCellTag;
    TObjectId ObjectId_;

    const NLogging::TLogger Logger;


    void DoOpen()
    {
        if (Path_.GetAppend() && Path_.GetCompressionCodec()) {
            THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"compression_codec\" are not compatible")
                << TErrorAttribute("path", Path_);
        }

        if (Path_.GetAppend() && Path_.GetErasureCodec()) {
            THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"erasure_codec\" are not compatible")
                << TErrorAttribute("path", Path_);
        }

        if (Options_.TransactionId) {
            Transaction_ = Client_->AttachTransaction(Options_.TransactionId);
            StartListenTransaction(Transaction_);
        }

        auto writerOptions = New<TMultiChunkWriterOptions>();

        TUserObject userObject(Path_);

        GetUserObjectBasicAttributes(
            Client_,
            {&userObject},
            Options_.TransactionId,
            Logger,
            EPermission::Write);

        ObjectId_ = userObject.ObjectId;
        NativeCellTag_ = CellTagFromId(ObjectId_);
        ExternalCellTag_ = userObject.ExternalCellTag;

        if (userObject.Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                Path_.GetPath(),
                EObjectType::File,
                userObject.Type);
        }

        auto objectIdPath = FromObjectId(ObjectId_);

        {
            YT_LOG_INFO("Requesting extended file attributes");

            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower,
                NativeCellTag_);
            auto req = TCypressYPathProxy::Get(objectIdPath + "/@");
            AddCellTagToSyncWith(req, ObjectId_);
            SetTransactionId(req, Transaction_);
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "account",
                "compression_codec",
                "erasure_codec",
                "primary_medium",
                "replication_factor",
                "enable_striped_erasure",
            });

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting extended attributes of file %v",
                Path_.GetPath());

            auto rsp = rspOrError.Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            auto attributesCompressionCodec = attributes->Get<NCompression::ECodec>("compression_codec");
            auto attributesErasureCodec = attributes->Get<NErasure::ECodec>("erasure_codec");

            writerOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
            writerOptions->MediumName = attributes->Get<TString>("primary_medium");
            writerOptions->Account = attributes->Get<TString>("account");
            writerOptions->CompressionCodec = Path_.GetCompressionCodec().value_or(attributesCompressionCodec);
            writerOptions->ErasureCodec = Path_.GetErasureCodec().value_or(attributesErasureCodec);
            // COMPAT(gritukan)
            writerOptions->EnableStripedErasure = attributes->Get<bool>("enable_striped_erasure", false);

            YT_LOG_INFO("Extended file attributes received (Account: %v)",
                writerOptions->Account);
        }

        {
            YT_LOG_INFO("Starting file upload");

            auto proxy = CreateObjectServiceWriteProxy(
                Client_,
                NativeCellTag_);
            auto batchReq = proxy.ExecuteBatch();

            {
                auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
                for (const auto& id : Options_.PrerequisiteTransactionIds) {
                    auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                    ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
                }
            }

            {
                bool append = Path_.GetAppend();
                auto req = TFileYPathProxy::BeginUpload(objectIdPath);
                auto updateMode = append ? EUpdateMode::Append : EUpdateMode::Overwrite;
                req->set_update_mode(ToProto(updateMode));
                auto lockMode = (append && !Options_.ComputeMD5) ? ELockMode::Shared : ELockMode::Exclusive;
                req->set_lock_mode(ToProto(lockMode));
                req->set_upload_transaction_title(Format("Upload to %v", Path_.GetPath()));
                req->set_upload_transaction_timeout(ToProto(Client_->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
                GenerateMutationId(req);
                SetTransactionId(req, Transaction_);
                batchReq->AddRequest(req, "begin_upload");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error starting upload to file %v",
                Path_.GetPath());
            const auto& batchRsp = batchRspOrError.Value();

            {
                auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspBeginUpload>("begin_upload").Value();
                auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

                UploadTransaction_ = Client_->AttachTransaction(uploadTransactionId, TTransactionAttachOptions{
                    .AutoAbort = true,
                    .PingPeriod = Client_->GetNativeConnection()->GetConfig()->UploadTransactionPingPeriod,
                    .PingAncestors = Options_.PingAncestors
                });
                StartListenTransaction(UploadTransaction_);

                YT_LOG_INFO("File upload started (UploadTransactionId: %v)",
                    uploadTransactionId);
            }
        }

        TChunkListId chunkListId;

        {
            YT_LOG_INFO("Requesting file upload parameters");

            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower,
                ExternalCellTag_);
            auto req = TFileYPathProxy::GetUploadParams(objectIdPath);
            SetTransactionId(req, UploadTransaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting upload parameters for file %v",
                Path_.GetPath());

            const auto& rsp = rspOrError.Value();
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());

            if (Options_.ComputeMD5) {
                if (Path_.GetAppend()) {
                    FromProto(&MD5Hasher_, rsp->md5_hasher());
                    if (!MD5Hasher_) {
                        THROW_ERROR_EXCEPTION(
                            "Non-empty file %v has no computed MD5 hash thus "
                            "cannot append data and update the hash simultaneously",
                            Path_.GetPath());
                    }
                } else {
                    MD5Hasher_.emplace();
                }
            }

            YT_LOG_INFO("File upload parameters received (ChunkListId: %v)",
                chunkListId);
        }

        NChunkClient::TDataSink dataSink;
        dataSink.SetPath(Path_.GetPath());
        dataSink.SetObjectId(userObject.ObjectId);
        dataSink.SetAccount(writerOptions->Account);

        auto throttler = Options_.Throttler
            ? Options_.Throttler
            : NConcurrency::GetUnlimitedThrottler();

        Writer_ = CreateFileMultiChunkWriter(
            Config_,
            writerOptions,
            Client_,
            ExternalCellTag_,
            UploadTransaction_->GetId(),
            chunkListId,
            dataSink,
            /*writeBlocksOptions*/ {},
            /*trafficMetter*/ nullptr,
            std::move(throttler));

        YT_LOG_INFO("File opened");
    }

    void DoClose()
    {
        ValidateAborted();

        YT_LOG_INFO("Closing file");

        {
            auto result = WaitFor(Writer_->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to close file writer");
        }

        auto objectIdPath = FromObjectId(ObjectId_);

        auto proxy = CreateObjectServiceWriteProxy(
            Client_,
            NativeCellTag_);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
            for (const auto& id : Options_.PrerequisiteTransactionIds) {
                auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
                ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
            }
        }

        StopListenTransaction(UploadTransaction_);

        {
            auto req = TFileYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = Writer_->GetDataStatistics();
            ToProto(req->mutable_md5_hasher(), MD5Hasher_);

            if (auto compressionCodec = Path_.GetCompressionCodec()) {
                req->set_compression_codec(ToProto(*compressionCodec));
            }

            if (auto erasureCodec = Path_.GetErasureCodec()) {
                req->set_erasure_codec(ToProto(*erasureCodec));
            }

            if (auto securityTags = Path_.GetSecurityTags()) {
                ToProto(req->mutable_security_tags()->mutable_items(), *securityTags);
            }

            SetTransactionId(req, UploadTransaction_);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error finishing upload to file %v",
            Path_.GetPath());

        UploadTransaction_->Detach();

        YT_LOG_INFO("File closed");
    }
};

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    return New<TFileWriter>(client, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
