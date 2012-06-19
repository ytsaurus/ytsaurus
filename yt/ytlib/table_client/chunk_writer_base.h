#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/codec.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual TRefCounted
{
public:
    TAsyncError TChunkWriterBase::GetReadyEvent();


protected:
    TChunkWriterConfigPtr Config;
    NChunkClient::IAsyncWriterPtr ChunkWriter;
    ICodec* Codec;

    int CurrentBlockIndex;

    //! Uncompressed size of completed blocks.
    i64 UncompressedSize;
    //! Total size of completed and sent blocks.
    i64 SentSize;

    double CompressionRatio;

    //! Approximate data size counting all written rows.
    i64 DataWeight;

    //! Compressed blocks waiting to be sent to 
    std::deque<TSharedRef> PendingBlocks;
    TAsyncSemaphore PendingSemaphore;

    TAsyncStreamState State;

    NChunkHolder::NProto::TChunkMeta Meta;
    NChunkHolder::NProto::TMiscExt MiscExt;
    NProto::TChannelsExt ChannelsExt;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    TChunkWriterBase(
        NChunkClient::IAsyncWriterPtr chunkWriter,
        TChunkWriterConfigPtr config);
    void CompressAndWriteBlock(
        const TSharedRef& block, 
        NTableClient::NProto::TBlockInfo* blockInfo);
    void WritePendingBlock(TError error);

    void FinaliseWriter();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
