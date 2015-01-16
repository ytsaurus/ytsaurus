#include "stdafx.h"

#include "file_chunk_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "private.h"

#include <ytlib/api/public.h>

#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/multi_chunk_writer_base.h>

#include <core/logging/log.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriter
    : public IFileChunkWriter
{
public:
    TFileChunkWriter(
        TFileChunkWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter);

    virtual bool Write(const TRef& data) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Open() override;
    virtual TFuture<void> Close() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual TChunkMeta GetMasterMeta() const override;
    virtual TChunkMeta GetSchedulerMeta() const override;

    virtual TDataStatistics GetDataStatistics() const override;

private:
    TFileChunkWriterConfigPtr Config_;
    TEncodingChunkWriterPtr EncodingChunkWriter_;

    TBlob Buffer_;
    
    TBlocksExt BlocksExt_;
    i64 BlocksExtSize_ = 0;

    NLog::TLogger Logger;

    void FlushBlock();

};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriter)

////////////////////////////////////////////////////////////////////////////////

struct TFileChunkBlockTag { };

TFileChunkWriter::TFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter)
    : Config_(config)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(config, options, chunkWriter))
    , Buffer_(TFileChunkBlockTag())
    , Logger(FileClientLogger)
{ }

bool TFileChunkWriter::Write(const TRef& data)
{
    LOG_DEBUG("Writing data (Size: %v)", data.Size());

    if (data.Empty())
        return true;

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

    auto* block = BlocksExt_.add_blocks();
    block->set_size(Buffer_.Size());

    BlocksExtSize_ += sizeof(TBlockInfo);

    EncodingChunkWriter_->WriteBlock(TSharedRef::FromBlob(std::move(Buffer_)));
}

TFuture<void> TFileChunkWriter::Open()
{
    return VoidFuture;
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
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

i64 TFileChunkWriter::GetDataSize() const
{
    return EncodingChunkWriter_->GetDataStatistics().compressed_data_size(); + Buffer_.Size();
}

i64 TFileChunkWriter::GetMetaSize() const
{
    return BlocksExtSize_;
}

TChunkMeta TFileChunkWriter::GetMasterMeta() const
{
    TChunkMeta meta;
    SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
    return meta;
}

TChunkMeta TFileChunkWriter::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

TDataStatistics TFileChunkWriter::GetDataStatistics() const
{
    return EncodingChunkWriter_->GetDataStatistics();
}

////////////////////////////////////////////////////////////////////////////////

IFileChunkWriterPtr CreateFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter)
{
    return New<TFileChunkWriter>(config, options, chunkWriter);
}

IFileMultiChunkWriterPtr CreateFileMultiChunkWriter(
    TFileWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId)
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

    return New<TFileMultiChunkWriter>(
        config, 
        options, 
        masterChannel, 
        transactionId, 
        parentChunkListId, 
        createChunkWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
