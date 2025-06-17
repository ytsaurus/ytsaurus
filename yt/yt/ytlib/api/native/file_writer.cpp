#include "file_builder.h"
#include "file_writer.h"
#include "private.h"
#include "client.h"
#include "config.h"
#include "transaction.h"
#include "connection.h"
#include "private.h"

#include <yt/yt/client/api/file_builder.h>
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
    : public IFileWriter
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
            FileBuilder_->ValidateAborted();

            if (Options_.ComputeMD5 && FileBuilder_->MD5Hasher) {
                FileBuilder_->MD5Hasher->Append(data);
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

    NApi::ITransactionPtr UploadTransaction_;

    IFileMultiChunkWriterPtr Writer_;

    const NLogging::TLogger Logger;

    TFileBuilderPtr FileBuilder_ = nullptr;

    void DoOpen()
    {
        FileBuilder_ = New<TFileBuilder>(Client_, Path_, Options_);

        NChunkClient::TDataSink dataSink;
        dataSink.SetPath(Path_.GetPath());
        dataSink.SetObjectId(FileBuilder_->ObjectId);
        dataSink.SetAccount(FileBuilder_->WriterOptions->Account);

        auto throttler = Options_.Throttler
            ? Options_.Throttler
            : NConcurrency::GetUnlimitedThrottler();

        Writer_ = CreateFileMultiChunkWriter(
            Config_,
            FileBuilder_->WriterOptions,
            Client_,
            FileBuilder_->ExternalCellTag,
            UploadTransaction_->GetId(),
            FileBuilder_->ChunkListId,
            dataSink,
            /*writeBlocksOptions*/ {},
            /*trafficMetter*/ nullptr,
            std::move(throttler));

        YT_LOG_INFO("File opened");
    }

    void DoClose()
    {
        FileBuilder_->ValidateAborted();

        YT_LOG_INFO("Closing file");

        {
            auto result = WaitFor(Writer_->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Failed to close file writer");
        }
        auto objectIdPath = FromObjectId(FileBuilder_->ObjectId);
        auto req = TFileYPathProxy::EndUpload(objectIdPath);
        *req->mutable_statistics() = Writer_->GetDataStatistics();
        FileBuilder_->Close(req);
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
