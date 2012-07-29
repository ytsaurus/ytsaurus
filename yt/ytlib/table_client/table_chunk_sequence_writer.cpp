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
    TChunkSequenceWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkList,
    const std::vector<TChannel>& channels,
    const TNullable<TKeyColumns>& keyColumns)
    : TChunkSequenceWriterBase<TTableChunkWriter>(
        config, 
        masterChannel, 
        transactionId, 
        parentChunkList)
    , Channels(channels)
    , KeyColumns(keyColumns)
    , RowCount(0)
{ }

TTableChunkSequenceWriter::~TTableChunkSequenceWriter()
{ }

void TTableChunkSequenceWriter::PrepareChunkWriter(TSession& newSession)
{
    newSession.ChunkWriter = New<TTableChunkWriter>(
        Config->ChunkWriter,
        newSession.RemoteWriter,
        Channels,
        KeyColumns);

    newSession.ChunkWriter->AsyncOpen();
}

void TTableChunkSequenceWriter::InitCurrentSession(TSession nextSession)
{
    if (!CurrentSession.IsNull()) {
        nextSession.ChunkWriter->SetLastKey(
            CurrentSession.ChunkWriter->GetLastKey());
    }

    TChunkSequenceWriterBase<TTableChunkWriter>::InitCurrentSession(nextSession);
}

bool TTableChunkSequenceWriter::TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

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
    VERIFY_THREAD_AFFINITY(ClientThread);

    if (!CurrentSession.ChunkWriter) {
        return false;
    }

    if (!CurrentSession.ChunkWriter->TryWriteRowUnsafe(row)) {
        return false;
    }

    OnRowWritten();

    return true;
}

const TOwningKey& TTableChunkSequenceWriter::GetLastKey() const
{
    YASSERT(!State.HasRunningOperation());
    return CurrentSession.ChunkWriter->GetLastKey();
}

const TNullable<TKeyColumns>& TTableChunkSequenceWriter::GetKeyColumns() const
{
    return KeyColumns;
}

i64 TTableChunkSequenceWriter::GetRowCount() const
{
    YASSERT(!State.HasRunningOperation());
    return RowCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
