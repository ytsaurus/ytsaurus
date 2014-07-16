#include "stdafx.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "private.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/encoding_writer.h>

#include <core/misc/protobuf_helpers.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IWriterPtr chunkWriter)
    : Config(config)
    , Options(options)
    , ChunkWriter(chunkWriter)
    , EncodingWriter(New<TEncodingWriter>(Config, options, chunkWriter))
    , CurrentBlockIndex(0)
    , DataWeight(0)
    , RowCount(0)
    , ValueCount(0)
    , CurrentSize(0)
    , CurrentUncompressedSize(0)
    , CurrentBufferCapacity(0)
    , LargestBlockSize(0)
{
    VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);
}

const TNullable<TKeyColumns>& TChunkWriterBase::GetKeyColumns() const
{
    return Options->KeyColumns;
}

i64 TChunkWriterBase::GetRowCount() const
{
    return RowCount;
}

i64 TChunkWriterBase::GetDataSize() const
{
    return CurrentSize;
}

void TChunkWriterBase::CheckBufferCapacity()
{
    if (Config->MaxBufferSize < CurrentBufferCapacity) {
        State.Fail(TError(
            "\"max_buffer_size\" limit too low: %" PRId64 " < %" PRId64,
            Config->MaxBufferSize,
            CurrentBufferCapacity));
    }
}

void TChunkWriterBase::FinalizeWriter()
{
    Meta.set_type(EChunkType::Table);
    Meta.set_version(FormatVersion);

    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);

    {
        MiscExt.set_uncompressed_data_size(EncodingWriter->GetUncompressedSize());
        MiscExt.set_compressed_data_size(EncodingWriter->GetCompressedSize());
        MiscExt.set_meta_size(Meta.ByteSize());
        MiscExt.set_compression_codec(Options->CompressionCodec);
        MiscExt.set_data_weight(DataWeight);
        MiscExt.set_row_count(RowCount);
        MiscExt.set_value_count(ValueCount);
        MiscExt.set_max_block_size(LargestBlockSize);
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    auto this_ = MakeStrong(this);
    ChunkWriter->Close(Meta).Subscribe(BIND([=] (TError error) {
        // ToDo(psushin): more verbose diagnostic.
        this_->State.Finish(error);
    }));
}

TAsyncError TChunkWriterBase::GetReadyEvent()
{
    State.StartOperation();

    auto this_ = MakeStrong(this);
    EncodingWriter->GetReadyEvent().Subscribe(BIND([=](TError error){
        this_->State.FinishOperation(error);
    }));

    return State.GetOperationError();
}

bool TChunkWriterBase::IsLess(const TChannelWriter* lhs, const TChannelWriter* rhs)
{
    return lhs->GetDataSize() < rhs->GetDataSize();
}

void TChunkWriterBase::AdjustBufferHeap(int updatedBufferIndex)
{
    auto updatedHeapIndex = Buffers[updatedBufferIndex]->GetHeapIndex();
    while (updatedHeapIndex > 0) {
        auto parentHeapIndex = (updatedHeapIndex - 1) / 2;

        if (IsLess(BuffersHeap[parentHeapIndex], BuffersHeap[updatedHeapIndex])) {
            BuffersHeap[parentHeapIndex]->SetHeapIndex(updatedHeapIndex);
            BuffersHeap[updatedHeapIndex]->SetHeapIndex(parentHeapIndex);
            std::swap(BuffersHeap[parentHeapIndex], BuffersHeap[updatedHeapIndex]);
            updatedHeapIndex = parentHeapIndex;
        } else {
            return;
        }
    }
}

void TChunkWriterBase::PopBufferHeap()
{
    LOG_DEBUG("Block is finished (CurrentBufferCapacity: %" PRId64 ", CurrentBlockSize: %" PRId64 ")",
        CurrentBufferCapacity,
        BuffersHeap.front()->GetDataSize());

    int lastIndex = BuffersHeap.size() - 1;

    auto* currentBuffer = BuffersHeap[lastIndex];
    int currentIndex = 0;

    BuffersHeap[lastIndex] = BuffersHeap[0];

    BuffersHeap.back()->SetHeapIndex(lastIndex);
    CurrentBufferCapacity -= BuffersHeap.back()->GetCapacity();

    while (currentIndex < lastIndex) {
        int maxChild = 2 * currentIndex + 1;
        if (maxChild >= lastIndex) {
            break;
        }

        TChannelWriter* maxBuffer = BuffersHeap[maxChild];
        int rightChild = maxChild + 1;
        if (rightChild < lastIndex && IsLess(maxBuffer, BuffersHeap[rightChild])) {
            maxBuffer = BuffersHeap[rightChild];
            maxChild = rightChild;
        }

        if (IsLess(currentBuffer, maxBuffer)) {
            BuffersHeap[currentIndex] = maxBuffer;
            maxBuffer->SetHeapIndex(currentIndex);
            currentIndex = maxChild;
        } else {
            break;
        }
    }

    BuffersHeap[currentIndex] = currentBuffer;
    currentBuffer->SetHeapIndex(currentIndex);
}

NChunkClient::NProto::TDataStatistics TChunkWriterBase::GetDataStatistics() const
{
    NChunkClient::NProto::TDataStatistics result = NChunkClient::NProto::ZeroDataStatistics();
    if (RowCount > 0) {
        result.set_uncompressed_data_size(CurrentUncompressedSize);
        result.set_compressed_data_size(CurrentSize);
        result.set_row_count(RowCount);
        result.set_chunk_count(1);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
