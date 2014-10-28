#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <core/concurrency/thread_affinity.h>

#include <core/misc/ref.h>
#include <core/misc/error.h>
#include <core/misc/async_stream_state.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual TRefCounted
{
public:
    TAsyncError GetReadyEvent();

    const TNullable<TKeyColumns>& GetKeyColumns() const;
    i64 GetRowCount() const;
    i64 GetDataSize() const;

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

protected:
    TChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter);

    const TChunkWriterConfigPtr Config;
    const TChunkWriterOptionsPtr Options;
    const NChunkClient::IChunkWriterPtr ChunkWriter;

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

    i64 CurrentUncompressedSize;

    i64 CurrentBufferCapacity;

    i64 LargestBlockSize;

    TAsyncStreamState State;

    NChunkClient::NProto::TChunkMeta Meta;
    NChunkClient::NProto::TMiscExt MiscExt;
    NProto::TChannelsExt ChannelsExt;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    static bool IsLess(const TChannelWriter* lhs, const TChannelWriter* rhs);
    void AdjustBufferHeap(int updatedBufferIndex);
    void PopBufferHeap();

    void CheckBufferCapacity();
    void FinalizeWriter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
