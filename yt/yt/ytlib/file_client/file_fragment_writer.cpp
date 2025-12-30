#include "file_fragment_writer.h"

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/file_client/config.h>
#include <yt/yt/ytlib/file_client/helpers.h>
#include <yt/yt/ytlib/file_client/file_chunk_writer.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>
#include <yt/yt/ytlib/file_client/private.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFileClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TFileFragmentWriter
    : public IFileFragmentWriter
{
public:
    TFileFragmentWriter(
        TFileWriterConfigPtr config,
        TWriteFileFragmentCookie cookie,
        NNative::IClientPtr client,
        TTransactionId transactionId,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(std::move(config))
        , Cookie_(std::move(cookie))
        , Client_(std::move(client))
        , TrafficMeter_(std::move(trafficMeter))
        , Throttler_(std::move(throttler))
        , WriteBlocksOptions_(std::move(writeBlocksOptions))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , Logger(FileClientLogger().WithTag("Path: %v, TransactionId: %v",
            cookie.CookieData.RichPath,
            transactionId))
    { }

    TFuture<void> Open() override
    {
        ValidateNotClosed();

        return BIND(&TFileFragmentWriter::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateOpened();
        ValidateNotClosed();

        if (UnderlyingWriter_->Write(data)) {
            return OKFuture;
        }
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        ValidateOpened();
        ValidateNotClosed();

        return BIND(&TFileFragmentWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    TSignedWriteFileFragmentResultPtr GetWriteFragmentResult() const override
    {
        return SignedResult_;
    }

private:
    const TFileWriterConfigPtr Config_;
    const TWriteFileFragmentCookie Cookie_;
    const NNative::IClientPtr Client_;
    const TTrafficMeterPtr TrafficMeter_;
    const IThroughputThrottlerPtr Throttler_;
    const IChunkWriter::TWriteBlocksOptions WriteBlocksOptions_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    const NLogging::TLogger Logger;

    IFileMultiChunkWriterPtr UnderlyingWriter_;
    bool Opened_ = false;
    bool Closed_ = false;

    TWriteFileFragmentResult WriteResult_;
    // NB(achains): There is no signature here actually, we simply
    // convert WriteResult_ to the proper type possibly to be signed
    // by rpc proxy later.
    TSignedWriteFileFragmentResultPtr SignedResult_;

    void DoOpen()
    {
        YT_LOG_DEBUG("Opening file fragment writer");

        const auto& cookieData = Cookie_.CookieData;

        auto attributes = IAttributeDictionary::FromMap(cookieData.FileAttributes->AsMap());
        auto writerOptions = GetWriterOptions(
            attributes,
            cookieData.RichPath,
            MemoryUsageTracker_);

        auto objectIdPath = FromObjectId(cookieData.FileId);

        {
            YT_LOG_DEBUG("Reading file upload parameters");

            // TODO(achains): Can we allocate all chunk lists on start session?
            auto masterChannel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cookieData.ExternalCellTag);
            TChunkServiceProxy proxy(masterChannel);

            auto req = proxy.CreateChunkLists();
            GenerateMutationId(req);

            ToProto(req->mutable_transaction_id(), cookieData.MainTransactionId);
            req->set_count(1);

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            YT_VERIFY(rsp->chunk_list_ids_size() > 0);

            WriteResult_.SessionId = Cookie_.SessionId;
            WriteResult_.CookieId = Cookie_.CookieId;
            WriteResult_.ChunkListId = FromProto<TChunkListId>(rsp->chunk_list_ids()[0]);

            YT_LOG_DEBUG(
                "File upload parameters read (ChunkListId: %v)",
                WriteResult_.ChunkListId);
        }

        NChunkClient::TDataSink dataSink;
        dataSink.SetPath(cookieData.RichPath.GetPath());
        dataSink.SetObjectId(cookieData.FileId);
        dataSink.SetAccount(writerOptions->Account);

        UnderlyingWriter_ = CreateFileMultiChunkWriter(
            Config_,
            std::move(writerOptions),
            Client_,
            cookieData.ExternalCellTag,
            cookieData.MainTransactionId,
            WriteResult_.ChunkListId,
            std::move(dataSink),
            WriteBlocksOptions_,
            TrafficMeter_,
            Throttler_);

        Opened_ = true;

        YT_LOG_DEBUG("Opened file fragment writer");
    }

    void DoClose()
    {
        YT_LOG_DEBUG("Closing file fragment writer");

        auto underlyingWriterCloseError = WaitFor(UnderlyingWriter_->Close());

        if (!underlyingWriterCloseError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error closing underlying chunk writer")
                << underlyingWriterCloseError;
        }

        Closed_ = true;

        const auto& signatureGenerator = Client_->GetNativeConnection()->GetSignatureGenerator();
        SignedResult_ = TSignedWriteFileFragmentResultPtr(
            signatureGenerator->Sign(ConvertToYsonString(WriteResult_).ToString()));

        // Log all statistics.
        YT_LOG_DEBUG("Writer data statistics (DataStatistics: %v)", UnderlyingWriter_->GetDataStatistics());
        YT_LOG_DEBUG("Writer compression codec statistics (CodecStatistics: %v)", UnderlyingWriter_->GetCompressionStatistics());

        YT_LOG_DEBUG("Closed file fragment writer");
    }

    void ValidateOpened()
    {
        if (!Opened_) {
            THROW_ERROR_EXCEPTION("Can't write into an unopened file fragment writer");
        }
    }

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("File fragment writer is closed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileFragmentWriterPtr CreateFileFragmentWriter(
    TFileWriterConfigPtr config,
    TWriteFileFragmentCookie cookie,
    NNative::IClientPtr client,
    TTransactionId transactionId,
    IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
{
    auto writer = New<TFileFragmentWriter>(
        std::move(config),
        std::move(cookie),
        std::move(client),
        transactionId,
        std::move(trafficMeter),
        std::move(throttler),
        std::move(writeBlocksOptions),
        std::move(memoryUsageTracker));
    return writer;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
