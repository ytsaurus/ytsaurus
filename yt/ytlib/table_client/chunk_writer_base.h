#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/async_stream_state.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual TRefCounted
{
public:
    TAsyncError GetReadyEvent();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

protected:
    TChunkWriterBase(
        TChunkWriterConfigPtr config,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        const TNullable<TKeyColumns>& keyColumns);

    const TChunkWriterConfigPtr Config;
    const NChunkClient::IAsyncWriterPtr ChunkWriter;
    const TNullable<TKeyColumns> KeyColumns;

    NChunkClient::TEncodingWriterPtr EncodingWriter;

    std::vector<TChannelWriterPtr> Buffers;
    std::vector<TChannelWriter*> BuffersHeap;

    int CurrentBlockIndex;

    //! Approximate data size counting all written rows.
    i64 DataWeight;

    //! Total number of written rows.
    i64 RowCount;

    //! Total number of values ("cells") in all written rows.
    i64 ValueCount;

    i64 CurrentSize;

    i64 CurrentBufferCapacity;

    TAsyncStreamState State;

    NChunkClient::NProto::TChunkMeta Meta;
    NChunkClient::NProto::TMiscExt MiscExt;
    NProto::TChannelsExt ChannelsExt;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    bool IsLess(const TChannelWriter* lhs, const TChannelWriter* rhs);
    void AdjustBufferHeap(int updatedBufferIndex);
    void PopBufferHeap();

    void CheckBufferCapacity();
    void FinalizeWriter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
