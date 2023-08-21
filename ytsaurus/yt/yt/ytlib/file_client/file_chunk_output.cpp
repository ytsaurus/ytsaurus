#include "file_chunk_output.h"
#include "private.h"
#include "config.h"
#include "file_chunk_writer.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NFileClient {

using namespace NYTree;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TFileChunkOutput::TFileChunkOutput(
    TFileWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NNative::IClientPtr client,
    TTransactionId transactionId,
    NChunkClient::TDataSink dataSink,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    i64 sizeLimit)
    : Logger(FileClientLogger.WithTag("TransactionId: %v", transactionId))
    , Config_(std::move(config))
    , Options_(std::move(options))
    , Client_(std::move(client))
    , TransactionId_(transactionId)
    , DataSink_(std::move(dataSink))
    , TrafficMeter_(std::move(trafficMeter))
    , Throttler_(std::move(throttler))
    , SizeLimit_(sizeLimit)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Client_);
}

void TFileChunkOutput::DoWrite(const void* buf, size_t len)
{
    EnsureOpen();

    if (GetSize() > SizeLimit_) {
        return;
    }

    if (!FileChunkWriter_->Write(TRef(const_cast<void*>(buf), len))) {
        WaitFor(FileChunkWriter_->GetReadyEvent())
            .ThrowOnError();
    }
}

void TFileChunkOutput::DoFinish()
{
    EnsureOpen();

    if (GetSize() > 0) {
        YT_LOG_INFO("Closing file writer");

        WaitFor(FileChunkWriter_->Close())
            .ThrowOnError();
    }

    YT_LOG_INFO("File writer closed");
}

TChunkId TFileChunkOutput::GetChunkId() const
{
    return ConfirmingChunkWriter_->GetChunkId();
}

void TFileChunkOutput::EnsureOpen()
{
    if (Open_) {
        return;
    }

    YT_LOG_INFO("Opening file chunk output");

    const auto& connection = Client_->GetNativeConnection();

    auto cellTag = PickChunkHostingCell(connection, Logger);

    ConfirmingChunkWriter_ = CreateConfirmingWriter(
        Config_,
        Options_,
        cellTag,
        TransactionId_,
        NullChunkListId,
        Client_,
        /*localHostName*/ TString(), // Locality is not important for files.
        GetNullBlockCache(),
        TrafficMeter_,
        Throttler_);

    FileChunkWriter_ = CreateFileChunkWriter(
        Config_,
        New<TEncodingWriterOptions>(),
        ConfirmingChunkWriter_,
        DataSink_);

    YT_LOG_INFO("File chunk output opened (Account: %v, ReplicationFactor: %v, MediumName: %v, CellTag: %v)",
        Options_->Account,
        Options_->ReplicationFactor,
        Options_->MediumName,
        cellTag);

    Open_ = true;
}

i64 TFileChunkOutput::GetSize() const
{
    if (!FileChunkWriter_) {
        return 0;
    }
    return FileChunkWriter_->GetCompressedDataSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
