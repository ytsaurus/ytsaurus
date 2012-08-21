#include "stdafx.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "private.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NChunkServer;

static NLog::TLogger& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    TChunkWriterConfigPtr config,
    NChunkClient::IAsyncWriterPtr chunkWriter,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , ChunkWriter(chunkWriter)
    , KeyColumns(keyColumns)
    , EncodingWriter(New<TEncodingWriter>(Config, chunkWriter))
    , CurrentBlockIndex(0)
    , DataWeight(0)
    , RowCount(0)
    , ValueCount(0)
    , CurrentSize(0)
    , CurrentBufferSize(0)
{
    VERIFY_INVOKER_AFFINITY(WriterThread->GetInvoker(), WriterThread);
}

const TNullable<TKeyColumns>& TChunkWriterBase::GetKeyColumns() const
{
    return KeyColumns;
}

void TChunkWriterBase::FinalizeWriter()
{
    Meta.set_type(EChunkType::Table);

    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);

    {
        MiscExt.set_uncompressed_data_size(EncodingWriter->GetUncompressedSize());
        MiscExt.set_compressed_data_size(EncodingWriter->GetCompressedSize());
        MiscExt.set_meta_size(Meta.ByteSize());
        MiscExt.set_codec_id(Config->CodecId);
        MiscExt.set_data_weight(DataWeight);
        MiscExt.set_row_count(RowCount);
        MiscExt.set_value_count(ValueCount);
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    auto this_ = MakeStrong(this);
    ChunkWriter->AsyncClose(Meta).Subscribe(BIND([=] (TError error) {
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
    return lhs->GetCurrentSize() < rhs->GetCurrentSize();
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
    LOG_DEBUG("Finish block (CurrentBufferSize: %"PRId64", CurrentBlockSize: %"PRId64")",
        CurrentBufferSize,
        BuffersHeap.front()->GetCurrentSize());

    int lastIndex = BuffersHeap.size() - 1;

    std::swap(BuffersHeap[0], BuffersHeap[lastIndex]);
    BuffersHeap.back()->SetHeapIndex(lastIndex);
    BuffersHeap.front()->SetHeapIndex(0);

    CurrentBufferSize -= BuffersHeap.back()->GetCurrentSize();

    int currentIndex = 0;
    while (currentIndex < lastIndex) {
        int leftChild = 2 * currentIndex + 1;
        int rightChild = leftChild + 1;
        if (!IsLess(BuffersHeap[currentIndex], BuffersHeap[leftChild]) &&
            !IsLess(BuffersHeap[currentIndex], BuffersHeap[rightChild])) {
            return;
        }

        if (IsLess(BuffersHeap[leftChild], BuffersHeap[rightChild])) {
            std::swap(BuffersHeap[currentIndex], BuffersHeap[rightChild]);
            BuffersHeap[rightChild]->SetHeapIndex(rightChild);
            BuffersHeap[currentIndex]->SetHeapIndex(currentIndex);
            currentIndex = rightChild;
        } else {
            std::swap(BuffersHeap[currentIndex], BuffersHeap[leftChild]);
            BuffersHeap[leftChild]->SetHeapIndex(leftChild);
            BuffersHeap[currentIndex]->SetHeapIndex(currentIndex);
            currentIndex = leftChild;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
