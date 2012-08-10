#include "stdafx.h"
#include "table_chunk_sequence_writer.h"
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

TTableChunkSequenceWriter::TTableChunkSequenceWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkList,
    const std::vector<TChannel>& channels,
    const TNullable<TKeyColumns>& keyColumns)
    : TChunkSequenceWriterBase<TTableChunkWriter>(
        config, 
        masterChannel, 
        transactionId, 
        parentChunkList,
        keyColumns)
    , Channels(channels)
{ }

TTableChunkSequenceWriter::~TTableChunkSequenceWriter()
{ }

void TTableChunkSequenceWriter::PrepareChunkWriter(TSession* newSession)
{
    newSession->ChunkWriter = New<TTableChunkWriter>(
        Config,
        newSession->RemoteWriter,
        Channels,
        KeyColumns);

    newSession->ChunkWriter->AsyncOpen();
}

void TTableChunkSequenceWriter::InitCurrentSession(TSession nextSession)
{
    if (!CurrentSession.IsNull()) {
        nextSession.ChunkWriter->SetLastKey(CurrentSession.ChunkWriter->GetLastKey());
    }

    TChunkSequenceWriterBase<TTableChunkWriter>::InitCurrentSession(nextSession);
}

bool TTableChunkSequenceWriter::TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key)
{
    if (!CurrentSession.ChunkWriter) {
        return false;
    }

    if (!CurrentSession.ChunkWriter->TryWriteRowUnsafe(row, key)) {
        return false;
    }

    OnRowWritten();

    return true;
}

bool TTableChunkSequenceWriter::TryWriteRowUnsafe(const TRow& row)
{
    if (!CurrentSession.ChunkWriter) {
        return false;
    }

    if (!CurrentSession.ChunkWriter->TryWriteRowUnsafe(row)) {
        return false;
    }

    OnRowWritten();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
