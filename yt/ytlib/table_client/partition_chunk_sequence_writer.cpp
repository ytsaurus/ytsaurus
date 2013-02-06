#include "stdafx.h"
#include "partition_chunk_sequence_writer.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkSequenceWriter::TPartitionChunkSequenceWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const Stroka& account,
    const NChunkClient::TChunkListId& parentChunkListId,
    const TKeyColumns& keyColumns,
    IPartitioner* partitioner)
    : TChunkSequenceWriterBase<TPartitionChunkWriter>(
        config, 
        masterChannel, 
        transactionId, 
        account,
        parentChunkListId,
        keyColumns)
    , Partitioner(partitioner)
{ }

void TPartitionChunkSequenceWriter::PrepareChunkWriter(TSession* newSession)
{
    newSession->ChunkWriter = New<TPartitionChunkWriter>(
        Config,
        newSession->RemoteWriter,
        KeyColumns.Get(),
        Partitioner);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
