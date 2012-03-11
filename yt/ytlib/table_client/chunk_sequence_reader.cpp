#include "stdafx.h"
#include "chunk_sequence_reader.h"

#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceReader::TChunkSequenceReader(
    TConfig* config,
    const TChannel& channel,
    const NObjectServer::TTransactionId& transactionId,
    NRpc::IChannel* masterChannel,
    NChunkClient::IBlockCache* blockCache,
    const yvector<NProto::TChunkSlice>& chunkSlices)
    : Config(config)
    , Channel(channel)
    , BlockCache(blockCache)
    , TransactionId(transactionId)
    , ChunkSlices(chunkSlices)
    , MasterChannel(masterChannel)
    , NextChunkIndex(-1)
    , NextReader(New< TFuture<TChunkReader::TPtr> >())
{
    PrepareNextChunk();
}

void TChunkSequenceReader::PrepareNextChunk()
{
    YASSERT(!NextReader->IsSet());
    YASSERT(NextChunkIndex < ChunkSlices.ysize());

    ++NextChunkIndex;
    if (NextChunkIndex == ChunkSlices.ysize()) {
        NextReader->Set(NULL);
        return;
    }

    auto& chunkSlice = ChunkSlices[NextChunkIndex];
    auto remoteReader = CreateRemoteReader(
        ~Config->RemoteReader,
        ~BlockCache,
        ~MasterChannel,
        TChunkId::FromProto(chunkSlice.chunk_id()));

    auto chunkReader = New<TChunkReader>(
        ~Config->SequentialReader,
        Channel,
        ~remoteReader,
        chunkSlice.start_row(),
        chunkSlice.end_row());

    chunkReader->AsyncOpen()->Subscribe(FromMethod(
        &TChunkSequenceReader::OnNextReaderOpened,
        TWeakPtr<TChunkSequenceReader>(this),
        chunkReader));
}

void TChunkSequenceReader::OnNextReaderOpened(
    TError error, 
    TChunkReader::TPtr reader)
{
    YASSERT(!NextReader->IsSet());

    if (error.IsOK()) {
        NextReader->Set(reader);
        return;
    }

    State.Fail(error);
    NextReader->Set(NULL);
}

TAsyncError::TPtr TChunkSequenceReader::AsyncOpen()
{
    YASSERT(NextChunkIndex == 0);
    YASSERT(!State.HasRunningOperation());

    if (ChunkSlices.ysize() != 0) {
        State.StartOperation();
        NextReader->Subscribe(FromMethod(
            &TChunkSequenceReader::SetCurrentChunk,
            TWeakPtr<TChunkSequenceReader>(this)));
    }

    return State.GetOperationError();
}

void TChunkSequenceReader::SetCurrentChunk(TChunkReader::TPtr nextReader)
{
    CurrentReader = nextReader;
    if (nextReader) {
        NextReader = New< TFuture<TChunkReader::TPtr> >();
        PrepareNextChunk();

        if (NextChunkIndex > 1) {
            // Current chunk is not the first one.
            YASSERT(CurrentReader->HasNextRow());
            CurrentReader->AsyncNextRow()->Subscribe(FromMethod(
                &TChunkSequenceReader::OnNextRow,
                TWeakPtr<TChunkSequenceReader>(this)));
            return;
        }

    } else {
        YASSERT(!State.IsActive());
    }

    // Finishing AsyncOpen.
    State.FinishOperation();
}

void TChunkSequenceReader::OnNextRow(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
    }

    State.FinishOperation();
}

bool TChunkSequenceReader::HasNextRow() const
{

    YASSERT(!State.HasRunningOperation());

    if (NextChunkIndex == 0)
        return false;

    YASSERT(NextChunkIndex > 0);
    return NextChunkIndex < ChunkSlices.ysize() || CurrentReader->HasNextRow();
}

TAsyncError::TPtr TChunkSequenceReader::AsyncNextRow()
{
    YASSERT(HasNextRow());
    if (CurrentReader->HasNextRow()) {
        return CurrentReader->AsyncNextRow();
    } else {
        State.StartOperation();

        NextReader->Subscribe(FromMethod(
            &TChunkSequenceReader::SetCurrentChunk,
            TWeakPtr<TChunkSequenceReader>(this)));

        return State.GetOperationError();
    }
}

bool NYT::NTableClient::TChunkSequenceReader::NextColumn()
{
    return CurrentReader->NextColumn();
}

TValue TChunkSequenceReader::GetValue() const
{
    return CurrentReader->GetValue();
}

TColumn TChunkSequenceReader::GetColumn() const
{
    return CurrentReader->GetColumn();
}

void TChunkSequenceReader::Cancel(const TError& error)
{
    State.Cancel(error);
    CurrentReader->Cancel(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
