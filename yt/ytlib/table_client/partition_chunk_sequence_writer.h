#pragma once

#include "public.h"
#include "chunk_sequence_writer_base.h"
#include "partition_chunk_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkSequenceWriter
    : public TChunkSequenceWriterBase<TPartitionChunkWriter>
{
public:
    TPartitionChunkSequenceWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TNullable<Stroka>& account,
        const NChunkClient::TChunkListId& parentChunkListId,
        const TKeyColumns& keyColumns,
        IPartitioner* partitioner);

private:
    virtual void PrepareChunkWriter(TSession* newSession) override;

    IPartitioner* Partitioner;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
