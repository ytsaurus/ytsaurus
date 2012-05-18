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
    TChunkSequenceWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkServer::TChunkListId& parentChunkList,
    std::vector<TChannel>&& channels,
    TKeyColumns&& keyColumns,
    std::vector<NProto::TKey>&& partitionKeys)
    : TChunkSequenceWriterBase<TPartitionChunkWriter>(
        config, 
        masterChannel, 
        transactionId, 
        parentChunkList)
    , Channels(channels)
    , KeyColumns(keyColumns)
    , PartitionKeys(partitionKeys)
{ }

TPartitionChunkSequenceWriter::~TPartitionChunkSequenceWriter()
{ }

void TPartitionChunkSequenceWriter::PrepareChunkWriter(TSession& newSession)
{
    newSession.ChunkWriter = New<TPartitionChunkWriter>(
        Config->ChunkWriter,
        newSession.RemoteWriter,
        Channels,
        KeyColumns,
        PartitionKeys);
}

TAsyncError TPartitionChunkSequenceWriter::AsyncWriteRow(const TRow& row)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();

    // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
    auto asyncResult = CurrentSession.ChunkWriter->AsyncWriteRow(row);
    auto error = asyncResult.TryGet();
    if (error) {
        OnRowWritten(error.Get());
    } else {
        asyncResult.Subscribe(BIND(
            &TPartitionChunkSequenceWriter::OnRowWritten, 
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
