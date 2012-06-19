#include "stdafx.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    NChunkClient::IAsyncWriterPtr chunkWriter,
    TChunkWriterConfigPtr config)
    : Config(config)
    , ChunkWriter(chunkWriter)
    , CurrentBlockIndex(0)
    , UncompressedSize(0)
    , SentSize(0)
    , CompressionRatio(0)
    , DataWeight(0)
    , PendingSemaphore(2)
{
    VERIFY_INVOKER_AFFINITY(WriterThread->GetInvoker(), WriterThread);
    CompressionRatio = config->EstimatedCompressionRatio;
    Codec = GetCodec(Config->CodecId);
}

void TChunkWriterBase::CompressAndWriteBlock(const TSharedRef& block, NTableClient::NProto::TBlockInfo* blockInfo)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    blockInfo->set_block_index(CurrentBlockIndex);
    UncompressedSize += block.Size();

    auto data = Codec->Compress(block);

    SentSize += data.Size();

    CompressionRatio = SentSize / double(DataWeight + 1);

    ++CurrentBlockIndex;

    if (PendingBlocks.empty()) {
        if (ChunkWriter->TryWriteBlock(data)) {
            PendingSemaphore.Release();
            return;
        }

        ChunkWriter->GetReadyEvent().Subscribe(BIND(
            &TChunkWriterBase::WritePendingBlock,
            MakeWeak(this)));
    }

    PendingBlocks.push_back(data);
}

void TChunkWriterBase::WritePendingBlock(TError error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(!PendingBlocks.empty());

    if (!error.IsOK()) {
        // ToDo(psushin): add additional diagnostic message.
        State.Fail(error);
    }

    if (ChunkWriter->TryWriteBlock(PendingBlocks.front())) {
        PendingBlocks.pop_front();
        PendingSemaphore.Release();
    }

    if (!PendingBlocks.empty()) {
        ChunkWriter->GetReadyEvent().Subscribe(BIND(
            &TChunkWriterBase::WritePendingBlock,
            MakeWeak(this)));
    }
}

TAsyncError TChunkWriterBase::GetReadyEvent()
{
    if (!PendingSemaphore.IsReady()) {
        State.StartOperation();
        auto this_ = MakeStrong(this);
        PendingSemaphore.GetReadyEvent().Subscribe(BIND([=] () {
            this->State.FinishOperation();
        }));
    }

    return State.GetOperationError();
}

void TChunkWriterBase::FinaliseWriter()
{
    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);

    Meta.set_type(EChunkType::Table);

    {
        MiscExt.set_uncompressed_data_size(UncompressedSize);
        MiscExt.set_compressed_data_size(SentSize);
        MiscExt.set_meta_size(Meta.ByteSize());
        MiscExt.set_codec_id(Config->CodecId);
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    auto this_ = MakeStrong(this);
    ChunkWriter->AsyncClose(Meta).Subscribe(BIND([=] (TError error) {
        // ToDo(psushin): more verbose diagnostic.
        this_->State.Finish(error);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
