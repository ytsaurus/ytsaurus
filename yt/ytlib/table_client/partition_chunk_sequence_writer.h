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
        TChunkSequenceWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& parentChunkList,
        std::vector<TChannel>&& channels,
        TKeyColumns&& keyColumns,
        std::vector<NProto::TKey>&& partitionKeys);

    ~TPartitionChunkSequenceWriter();

private:
    void PrepareChunkWriter(TSession& newSession);

    const std::vector<TChannel> Channels;
    const TKeyColumns KeyColumns;
    const std::vector<NProto::TKey> PartitionKeys;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
