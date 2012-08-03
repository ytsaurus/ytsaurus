#include "stdafx.h"

#include "partition_chunk_sequence_writer.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkSequenceWriter::TPartitionChunkSequenceWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkServer::TChunkListId& parentChunkList,
    const TKeyColumns& keyColumns,
    IPartitioner* partitioner)
    : TChunkSequenceWriterBase<TPartitionChunkWriter>(
        config, 
        masterChannel, 
        transactionId, 
        parentChunkList,
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
