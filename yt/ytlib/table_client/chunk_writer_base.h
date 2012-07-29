#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_holder/chunk.pb.h>

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

protected:
    TChunkWriterBase(
        NChunkClient::IAsyncWriterPtr chunkWriter,
        TChunkWriterConfigPtr config);

    TChunkWriterConfigPtr Config;
    NChunkClient::IAsyncWriterPtr ChunkWriter;
    NChunkClient::TEncodingWriterPtr EncodingWriter;

    int CurrentBlockIndex;

    //! Approximate data size counting all written rows.
    i64 DataWeight;

    //! Total number of written rows.
    i64 RowCount;

    //! Total number of values ("cells") in all written rows.
    i64 ValueCount;

    i64 CurrentSize;

    TAsyncStreamState State;

    NChunkHolder::NProto::TChunkMeta Meta;
    NChunkHolder::NProto::TMiscExt MiscExt;
    NProto::TChannelsExt ChannelsExt;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    void FinalizeWriter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
