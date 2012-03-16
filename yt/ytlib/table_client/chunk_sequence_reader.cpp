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
    std::vector<NProto::TChunkSlice>& chunkSlices)
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
    int chunkSlicesSize = static_cast<int>(ChunkSlices.size());
    YASSERT(NextChunkIndex < chunkSlicesSize);

    ++NextChunkIndex;
    if (NextChunkIndex == chunkSlicesSize) {
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
        chunkSlice.start_limit(),
        chunkSlice.end_limit());

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

    if (ChunkSlices.size() != 0) {
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

        if (!CurrentReader->IsValid()) {
            NextReader->Subscribe(FromMethod(
                &TChunkSequenceReader::SetCurrentChunk,
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
        return;
    }

    if (!CurrentReader->IsValid()) {
        NextReader->Subscribe(FromMethod(
            &TChunkSequenceReader::SetCurrentChunk,
            TWeakPtr<TChunkSequenceReader>(this)));
        return;
    }

    State.FinishOperation();
}

bool TChunkSequenceReader::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    if (!CurrentReader)
        return false;

    return CurrentReader->IsValid();
}

const TRow& TChunkSequenceReader::GetCurrentRow() const
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(CurrentReader);
    YASSERT(CurrentReader->IsValid());

    return CurrentReader->GetCurrentRow();
}

TAsyncError::TPtr TChunkSequenceReader::AsyncNextRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    State.StartOperation();
    
    CurrentReader->AsyncNextRow()->Subscribe(FromMethod(
        &TChunkSequenceReader::OnNextRow,
        TWeakPtr<TChunkSequenceReader>(this)));

    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
