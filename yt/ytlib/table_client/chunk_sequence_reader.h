#pragma once

#include "public.h"
#include "private.h"
#include "chunk_reader.h"
#include "chunk_sequence_reader_base.h"

#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/async_stream_state.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableChunkSequenceReader
    : public TChunkSequenceReaderBase<TTableChunkReader>
{
    DEFINE_BYVAL_RO_PROPERTY(i64, ValueCount);
    DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);

public:
    TTableChunkSequenceReader(
        TChunkSequenceReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const TReaderOptions& options = TReaderOptions());

    const TRow& GetRow() const;
    const TNonOwningKey& GetKey() const;
    //const NYTree::TYsonString& GetRowAttributes() const;

    double GetProgress() const;

private:
    TReaderOptions Options;

    //! Upper bound estimation.
    i64 TotalValueCount;

    //! Upper bound, estimation, becomes more precise as we start actually reading chunk.
    //! Is precise and equal to CurrentRowIndex when reading is complete.
    i64 TotalRowCount;

    virtual TTableChunkReaderPtr CreateNewReader(
        const NProto::TInputChunk& chunk,
        NChunkClient::IAsyncReaderPtr asyncReader);

    virtual void OnChunkSwitch(const TReaderPtr& nextReader);

    virtual bool KeepReaders() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
