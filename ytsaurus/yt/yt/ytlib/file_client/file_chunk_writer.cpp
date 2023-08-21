#include "file_chunk_writer.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/core/misc/blob.h>

namespace NYT::NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;
using namespace NTracing;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TFileChunkWriterBufferTag
{ };

class TFileChunkWriter
    : public IFileChunkWriter
{
public:
    TFileChunkWriter(
        TFileChunkWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        const NChunkClient::TDataSink& dataSink,
        NChunkClient::IBlockCachePtr blockCache);

    bool Write(TRef data) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close() override;

    i64 GetMetaSize() const override;
    i64 GetCompressedDataSize() const override;

    i64 GetDataWeight() const override;

    bool IsCloseDemanded() const override;

    TDeferredChunkMetaPtr GetMeta() const override;
    TChunkId GetChunkId() const override;

    TDataStatistics GetDataStatistics() const override;
    TCodecStatistics GetCompressionStatistics() const override;

private:
    const NLogging::TLogger Logger;
    const TFileChunkWriterConfigPtr Config_;
    const TEncodingChunkWriterPtr EncodingChunkWriter_;

    TBlob Buffer_{GetRefCountedTypeCookie<TFileChunkWriterBufferTag>()};

    TBlocksExt BlocksExt_;
    i64 BlocksExtSize_ = 0;

    TTraceContextPtr TraceContext_;
    TTraceContextFinishGuard FinishGuard_;

    void FlushBlock();
};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriter)

////////////////////////////////////////////////////////////////////////////////

TFileChunkWriter::TFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    const NChunkClient::TDataSink& dataSink,
    IBlockCachePtr blockCache)
    : Logger(FileClientLogger.WithTag("ChunkWriterId: %v", TGuid::Create()))
    , Config_(config)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(
        config,
        options,
        chunkWriter,
        blockCache,
        Logger))
    , TraceContext_(CreateTraceContextFromCurrent("FileChunkWriter"))
    , FinishGuard_(TraceContext_)
{
    PackBaggageForChunkWriter(
        TraceContext_,
        dataSink,
        TExtraChunkTags{
            .CompressionCodec = options->CompressionCodec,
            .ErasureCodec = chunkWriter->GetErasureCodecId(),
        });
}

bool TFileChunkWriter::Write(TRef data)
{
    YT_LOG_DEBUG("Writing data (Size: %v)", data.Size());

    if (data.Empty()) {
        return true;
    }

    TCurrentTraceContextGuard guard(TraceContext_);

    if (Buffer_.IsEmpty()) {
        Buffer_.Reserve(static_cast<size_t>(Config_->BlockSize));
    }

    size_t dataSize = data.Size();
    const char* dataPtr = data.Begin();
    while (dataSize != 0) {
        // Copy a part of data trying to fill up the current block.
        size_t remainingSize = static_cast<size_t>(Config_->BlockSize) - Buffer_.Size();
        size_t bytesToCopy = std::min(dataSize, remainingSize);
        Buffer_.Append(dataPtr, bytesToCopy);
        dataPtr += bytesToCopy;
        dataSize -= bytesToCopy;

        // Flush the block if full.
        if (std::ssize(Buffer_) == Config_->BlockSize) {
            FlushBlock();
        }
    }

    return EncodingChunkWriter_->IsReady();
}

TFuture<void> TFileChunkWriter::GetReadyEvent()
{
    return EncodingChunkWriter_->GetReadyEvent();
}

void TFileChunkWriter::FlushBlock()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    YT_VERIFY(!Buffer_.IsEmpty());
    YT_LOG_DEBUG("Flushing block (BlockSize: %v)", Buffer_.Size());

    auto* block = BlocksExt_.add_blocks();
    block->set_size(Buffer_.Size());

    BlocksExtSize_ += sizeof(TBlockInfo);

    EncodingChunkWriter_->WriteBlock(
        TSharedRef::FromBlob(std::move(Buffer_)),
        EBlockType::UncompressedData);
}

TFuture<void> TFileChunkWriter::Close()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    if (!Buffer_.IsEmpty()) {
        FlushBlock();
    }

    auto meta = EncodingChunkWriter_->GetMeta();
    meta->set_type(ToProto<int>(EChunkType::File));
    meta->set_format(ToProto<int>(EChunkFormat::FileDefault));
    SetProtoExtension(meta->mutable_extensions(), BlocksExt_);

    return BIND(&TEncodingChunkWriter::Close, EncodingChunkWriter_)
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

i64 TFileChunkWriter::GetCompressedDataSize() const
{
    return EncodingChunkWriter_->GetDataStatistics().compressed_data_size() + Buffer_.Size();
}

i64 TFileChunkWriter::GetDataWeight() const
{
    return 0;
}

i64 TFileChunkWriter::GetMetaSize() const
{
    return BlocksExtSize_;
}

bool TFileChunkWriter::IsCloseDemanded() const
{
    return EncodingChunkWriter_->IsCloseDemanded();
}

TDeferredChunkMetaPtr TFileChunkWriter::GetMeta() const
{
    return EncodingChunkWriter_->GetMeta();
}

TChunkId TFileChunkWriter::GetChunkId() const
{
    return EncodingChunkWriter_->GetChunkId();
}

TDataStatistics TFileChunkWriter::GetDataStatistics() const
{
    return EncodingChunkWriter_->GetDataStatistics();
}

TCodecStatistics TFileChunkWriter::GetCompressionStatistics() const
{
    return EncodingChunkWriter_->GetCompressionStatistics();
}

////////////////////////////////////////////////////////////////////////////////

IFileChunkWriterPtr CreateFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    const NChunkClient::TDataSink& dataSink,
    IBlockCachePtr blockCache)
{
    return New<TFileChunkWriter>(
        config,
        options,
        chunkWriter,
        dataSink,
        blockCache);
}

IFileMultiChunkWriterPtr CreateFileMultiChunkWriter(
    TFileWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NNative::IClientPtr client,
    TCellTag cellTag,
    TTransactionId transactionId,
    TChunkListId parentChunkListId,
    const NChunkClient::TDataSink& dataSink,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    typedef TMultiChunkWriterBase<
        IFileMultiChunkWriter,
        IFileChunkWriter,
        TRef> TFileMultiChunkWriter;

    auto createChunkWriter = [=] (IChunkWriterPtr chunkWriter) {
         return CreateFileChunkWriter(
            config,
            options,
            chunkWriter,
            dataSink);
    };

    auto writer = New<TFileMultiChunkWriter>(
        config,
        options,
        client,
        /*localHostName*/ TString(), // Locality is not important for files.
        cellTag,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        trafficMeter,
        throttler,
        blockCache);

    writer->Init();

    return writer;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
