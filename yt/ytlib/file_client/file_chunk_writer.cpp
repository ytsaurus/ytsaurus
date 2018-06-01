#include "file_chunk_writer.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer_base.h>

namespace NYT {
namespace NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

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
        NChunkClient::IBlockCachePtr blockCache);

    virtual bool Write(const TRef& data) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetCompressedDataSize() const override;

    virtual i64 GetDataWeight() const override;

    virtual bool IsCloseDemanded() const override;
    virtual TChunkMeta GetMasterMeta() const override;
    virtual TChunkMeta GetSchedulerMeta() const override;
    virtual TChunkMeta GetNodeMeta() const override;
    virtual TChunkId GetChunkId() const override;

    virtual TDataStatistics GetDataStatistics() const override;
    virtual TCodecStatistics GetCompressionStatistics() const override;

private:
    NLogging::TLogger Logger;

    const TFileChunkWriterConfigPtr Config_;
    const TEncodingChunkWriterPtr EncodingChunkWriter_;


    TBlob Buffer_ { TFileChunkWriterBufferTag() };

    TBlocksExt BlocksExt_;
    i64 BlocksExtSize_ = 0;

    void FlushBlock();
};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriter)

////////////////////////////////////////////////////////////////////////////////

TFileChunkWriter::TFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache)
    : Logger(NLogging::TLogger(FileClientLogger)
        .AddTag("ChunkWriterId: %v", TGuid::Create()))
    , Config_(config)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(
        config,
        options,
        chunkWriter,
        blockCache,
        Logger))
{ }

bool TFileChunkWriter::Write(const TRef& data)
{
    LOG_DEBUG("Writing data (Size: %v)", data.Size());

    if (data.Empty()) {
        return true;
    }

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
        if (Buffer_.Size() == Config_->BlockSize) {
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
    YCHECK(!Buffer_.IsEmpty());
    LOG_DEBUG("Flushing block (BlockSize: %v)", Buffer_.Size());

    auto* block = BlocksExt_.add_blocks();
    block->set_size(Buffer_.Size());

    BlocksExtSize_ += sizeof(TBlockInfo);

    EncodingChunkWriter_->WriteBlock(TSharedRef::FromBlob(std::move(Buffer_)));
}

TFuture<void> TFileChunkWriter::Close()
{
    if (!Buffer_.IsEmpty()) {
        FlushBlock();
    }

    auto& meta = EncodingChunkWriter_->Meta();
    meta.set_type(static_cast<int>(EChunkType::File));
    meta.set_version(FormatVersion);

    SetProtoExtension(meta.mutable_extensions(), BlocksExt_);

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
    return false;
}

TChunkMeta TFileChunkWriter::GetMasterMeta() const
{
    TChunkMeta meta;
    meta.set_type(static_cast<int>(EChunkType::File));
    meta.set_version(FormatVersion);
    SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
    return meta;
}

TChunkMeta TFileChunkWriter::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

TChunkMeta TFileChunkWriter::GetNodeMeta() const
{
    return GetMasterMeta();
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
    IBlockCachePtr blockCache)
{
    return New<TFileChunkWriter>(
        config,
        options,
        chunkWriter,
        blockCache);
}

IFileMultiChunkWriterPtr CreateFileMultiChunkWriter(
    TFileWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    INativeClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    typedef TMultiChunkWriterBase<
        IFileMultiChunkWriter,
        IFileChunkWriter,
        const TRef&> TFileMultiChunkWriter;

    auto createChunkWriter = [=] (IChunkWriterPtr chunkWriter) {
         return CreateFileChunkWriter(
            config,
            options,
            chunkWriter);
    };

    auto writer = New<TFileMultiChunkWriter>(
        config,
        options,
        client,
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

} // namespace NFileClient
} // namespace NYT
