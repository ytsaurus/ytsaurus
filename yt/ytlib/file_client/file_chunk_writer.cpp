#include "stdafx.h"
#include "file_chunk_writer.h"
#include "private.h"
#include "config.h"

#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/dispatcher.h>

namespace NYT {
namespace NFileClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct TCompressedFileChunkBlockTag { };

TFileChunkWriter::TFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter)
    : Config(config)
    , Options(options)
    , EncodingWriter(New<TEncodingWriter>(config, options, chunkWriter))
    , ChunkWriter(chunkWriter)
    , Facade(this)
    , Buffer(TCompressedFileChunkBlockTag())
    , Size(0)
    , BlockCount(0)
    , Logger(FileClientLogger)
{ }

TFileChunkWriter::~TFileChunkWriter()
{ }

TFileChunkWriterFacade* TFileChunkWriter::GetFacade()
{
    if (State.IsActive() && EncodingWriter->IsReady()) {
        return &Facade;
    }

    return nullptr;
}

TAsyncError TFileChunkWriter::GetReadyEvent()
{
    State.StartOperation();

    auto this_ = MakeStrong(this);
    EncodingWriter->GetReadyEvent().Subscribe(BIND([=](TError error){
        this_->State.FinishOperation(error);
    }));

    return State.GetOperationError();
}

void TFileChunkWriter::FlushBlock()
{
    if (Buffer.IsEmpty())
        return;

    LOG_INFO("Writing block (BlockIndex: %d)", BlockCount);
    auto* block = BlocksExt.add_blocks();
    block->set_size(Buffer.Size());

    EncodingWriter->WriteBlock(TSharedRef::FromBlob(std::move(Buffer)));

    Buffer.Clear();
    ++BlockCount;
}

TAsyncError TFileChunkWriter::Close()
{
    YCHECK(!State.IsClosed());

    State.StartOperation();

    FlushBlock();

    EncodingWriter->Flush().Subscribe(
        BIND(&TFileChunkWriter::OnFinalBlocksWritten, MakeWeak(this))
            .Via(TDispatcher::Get()->GetWriterInvoker()));

    return State.GetOperationError();
}

void TFileChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        State.FinishOperation(error);
        return;
    }

    Meta.set_type(EChunkType::File);
    Meta.set_version(FormatVersion);

    SetProtoExtension(Meta.mutable_extensions(), BlocksExt);

    MiscExt.set_uncompressed_data_size(EncodingWriter->GetUncompressedSize());
    MiscExt.set_compressed_data_size(EncodingWriter->GetCompressedSize());
    MiscExt.set_meta_size(Meta.ByteSize());
    MiscExt.set_compression_codec(Options->CompressionCodec);

    SetProtoExtension(Meta.mutable_extensions(), MiscExt);

    auto this_ = MakeStrong(this);
    ChunkWriter->Close(Meta).Subscribe(BIND([=] (const TError& error) {
        // ToDo(psushin): more verbose diagnostic.
        this_->State.Finish(error);
    }));
}

void TFileChunkWriter::Write(const TRef& data)
{
    LOG_DEBUG("Writing data (Size: %d)",
        static_cast<int>(data.Size()));

    if (data.Empty())
        return;

    if (Buffer.IsEmpty()) {
        Buffer.Reserve(static_cast<size_t>(Config->BlockSize));
    }

    size_t dataSize = data.Size();
    const char* dataPtr = data.Begin();
    while (dataSize != 0) {
        // Copy a part of data trying to fill up the current block.
        size_t remainingSize = static_cast<size_t>(Config->BlockSize) - Buffer.Size();
        size_t bytesToCopy = std::min(dataSize, remainingSize);
        Buffer.Append(dataPtr, bytesToCopy);
        dataPtr += bytesToCopy;
        dataSize -= bytesToCopy;

        // Flush the block if full.
        if (Buffer.Size() == Config->BlockSize) {
            FlushBlock();
        }
    }

    Size += data.Size();
}

i64 TFileChunkWriter::GetDataSize() const
{
    return EncodingWriter->GetCompressedSize() + Buffer.Size();
}

i64 TFileChunkWriter::GetMetaSize() const
{
    return sizeof(NChunkClient::NProto::TMiscExt) +
        BlockCount * sizeof(NProto::TBlockInfo) +
        sizeof(NChunkClient::NProto::TChunkMeta);
}

NChunkClient::NProto::TChunkMeta TFileChunkWriter::GetMasterMeta() const
{
    static const int masterMetaTagsArray[] = { TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value };
    static const yhash_set<int> masterMetaTags(masterMetaTagsArray, masterMetaTagsArray + 1);

    auto meta = Meta;
    FilterProtoExtensions(
        meta.mutable_extensions(),
        Meta.extensions(),
        masterMetaTags);
    return meta;
}

NChunkClient::NProto::TChunkMeta TFileChunkWriter::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

////////////////////////////////////////////////////////////////////////////////

TFileChunkWriterFacade::TFileChunkWriterFacade(TFileChunkWriter* writer)
    : Writer(writer)
{ }

void TFileChunkWriterFacade::Write(const TRef& data)
{
    Writer->Write(data);
}

////////////////////////////////////////////////////////////////////////////////

TFileChunkWriterProvider::TFileChunkWriterProvider(
    TFileChunkWriterConfigPtr config,
    NChunkClient::TEncodingWriterOptionsPtr options)
    : Config(config)
    , Options(options)
    , ActiveWriters(0)
{ }

TFileChunkWriterPtr TFileChunkWriterProvider::CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter)
{
    YCHECK(ActiveWriters == 0);
    ++ActiveWriters;
    return New<TFileChunkWriter>(Config, Options, chunkWriter);
}

void TFileChunkWriterProvider::OnChunkFinished()
{
    --ActiveWriters;
    YCHECK(ActiveWriters == 0);
}

void TFileChunkWriterProvider::OnChunkClosed(TFileChunkWriterPtr /*writer*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
