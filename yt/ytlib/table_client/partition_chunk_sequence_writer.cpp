#include "stdafx.h"
#include "partition_chunk_sequence_writer.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkSequenceWriter::TPartitionChunkSequenceWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId,
    IPartitioner* partitioner)
    : TChunkSequenceWriterBase<TPartitionChunkWriter>(
        config,
        options,
        masterChannel,
        transactionId,
        parentChunkListId)
    , Partitioner(partitioner)
{ }

void TPartitionChunkSequenceWriter::PrepareChunkWriter(TSession* newSession)
{
    newSession->ChunkWriter = New<TPartitionChunkWriter>(
        Config,
        Options,
        newSession->RemoteWriter,
        Partitioner);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
