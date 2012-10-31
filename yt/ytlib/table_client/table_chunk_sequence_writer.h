#pragma once

#include "public.h"
#include "chunk_sequence_writer_base.h"
#include "table_chunk_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableChunkSequenceWriter
    : public TChunkSequenceWriterBase<TTableChunkWriter>
{
public:
    TTableChunkSequenceWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkClient::TChunkListId& parentChunkList,
        const TChannels& channels,
        const TNullable<TKeyColumns>& keyColumns = Null);

    ~TTableChunkSequenceWriter();

    // Used internally by jobs that generate sorted output.
    bool TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key);
    bool TryWriteRowUnsafe(const TRow& row);

private:
    const TChannels Channels;

    virtual void InitCurrentSession(TSession nextSession) override;
    virtual void PrepareChunkWriter(TSession* newSession) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
