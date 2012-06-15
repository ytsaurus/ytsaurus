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
        nextSession.ChunkWriter->LastKey = 
            CurrentSession.ChunkWriter->GetLastKey();
    }

    TChunkSequenceWriterBase<TTableChunkWriter>::InitCurrentSession(nextSession);
}

TAsyncError TTableChunkSequenceWriter::AsyncWriteRow(TRow& row, const TNonOwningKey& key)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    ++RowCount;

    // This is a performance-critical spot. Try to avoid using callbacks for synchronously written rows.
    auto asyncResult = CurrentSession.ChunkWriter->AsyncWriteRow(row, key);
    auto error = asyncResult.TryGet();
    if (error) {
        OnRowWritten(error.Get());
    } else {
        asyncResult.Subscribe(BIND(&TTableChunkSequenceWriter::OnRowWritten, MakeWeak(this)));
    }

    return State.GetOperationError();
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
