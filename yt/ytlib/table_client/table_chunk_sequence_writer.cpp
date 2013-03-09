#include "stdafx.h"
#include "table_chunk_sequence_writer.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTableChunkSequenceWriter::TTableChunkSequenceWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId)
    : TChunkSequenceWriterBase<TTableChunkWriter>(
        config,
        options,
        masterChannel,
        transactionId,
        parentChunkListId)
{
    // Create keys.
    BoundaryKeys.mutable_start();
    BoundaryKeys.mutable_end();
}

TTableChunkSequenceWriter::~TTableChunkSequenceWriter()
{ }

void TTableChunkSequenceWriter::PrepareChunkWriter(TSession* newSession)
{
    newSession->ChunkWriter = New<TTableChunkWriter>(
        Config,
        Options,
        newSession->RemoteWriter);

    newSession->ChunkWriter->AsyncOpen();
}

void TTableChunkSequenceWriter::InitCurrentSession(TSession nextSession)
{
    if (!CurrentSession.IsNull()) {
        nextSession.ChunkWriter->SetLastKey(CurrentSession.ChunkWriter->GetLastKey());
    }

    TChunkSequenceWriterBase<TTableChunkWriter>::InitCurrentSession(nextSession);
}

bool TTableChunkSequenceWriter::TryWriteRow(const TRow& row)
{
    if (!CurrentSession.ChunkWriter) {
        return false;
    }

    if (!CurrentSession.ChunkWriter->TryWriteRow(row)) {
        return false;
    }

    // Collect boundary keys in safe writer only.
    if (GetRowCount() == 0) {
        *BoundaryKeys.mutable_start() = CurrentSession.ChunkWriter->GetLastKey().ToProto();
    }

    OnRowWritten();

    return true;
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

TAsyncError TTableChunkSequenceWriter::AsyncClose()
{
    *BoundaryKeys.mutable_end() = CurrentSession.ChunkWriter->GetLastKey().ToProto();

    LOG_DEBUG_IF(Options->KeyColumns, "Boundary keys determined (Start: %s, End: %s)",
        ~ToString(BoundaryKeys.start()),
        ~ToString(BoundaryKeys.end()));
    return TBase::AsyncClose();
}

const NProto::TBoundaryKeysExt& TTableChunkSequenceWriter::GetBoundaryKeys() const
{
    return BoundaryKeys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
