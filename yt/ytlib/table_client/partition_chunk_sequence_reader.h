#pragma once

#include "public.h"
#include "private.h"
#include "partition_chunk_reader.h"
#include "chunk_sequence_reader_base.h"

#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/async_stream_state.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkSequenceReader
    : public TChunkSequenceReaderBase<TPartitionChunkReader>
{
    DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);

public:
    TPartitionChunkSequenceReader(
        TChunkSequenceReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks);

private:
    virtual TPartitionChunkReaderPtr CreateNewReader(
        const NProto::TInputChunk& chunk,
        NChunkClient::IAsyncReaderPtr asyncReader);

    virtual bool KeepReaders() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT