#include "stdafx.h"
#include "chunk_sequence_reader.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceReader::TChunkSequenceReader(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    NChunkClient::IBlockCache* blockCache,
    const std::vector<NProto::TInputChunk>& fetchedChunks,
    TChunkReader::TOptions options)
    : Config(config)
    , BlockCache(blockCache)
    , InputChunks(fetchedChunks)
    , MasterChannel(masterChannel)
    , NextChunkIndex(-1)
    , NextReader(New< TFuture<TChunkReader::TPtr> >())
    , Options(options)
{
    PrepareNextChunk();
}

void TChunkSequenceReader::PrepareNextChunk()
{
    YASSERT(!NextReader->IsSet());
    int chunkSlicesSize = static_cast<int>(InputChunks.size());
    YASSERT(NextChunkIndex < chunkSlicesSize);

    ++NextChunkIndex;
    if (NextChunkIndex == chunkSlicesSize) {
        NextReader->Set(NULL);
        return;
    }

    const auto& inputChunk = InputChunks[NextChunkIndex];
    const auto& slice = inputChunk.slice();
    auto remoteReader = CreateRemoteReader(
        ~Config->RemoteReader,
        ~BlockCache,
        ~MasterChannel,
        TChunkId::FromProto(inputChunk.slice().chunk_id()),
        FromProto<Stroka>(inputChunk.holder_addresses()));

    auto chunkReader = New<TChunkReader>(
        ~Config->SequentialReader,
        TChannel::FromProto(inputChunk.channel()),
        ~remoteReader,
        slice.start_limit(),
        slice.end_limit(),
        "", // ToDo(psushin): pass row attributes here.
        Options);

    chunkReader->AsyncOpen()->Subscribe(BIND(
        &TChunkSequenceReader::OnNextReaderOpened,
        MakeWeak(this),
        chunkReader));
}

void TChunkSequenceReader::OnNextReaderOpened(
    TChunkReader::TPtr reader,
    TError error)
{
    YASSERT(!NextReader->IsSet());

    if (error.IsOK()) {
        NextReader->Set(reader);
        return;
    }

    State.Fail(error);
    NextReader->Set(NULL);
}

TAsyncError TChunkSequenceReader::AsyncOpen()
{
    YASSERT(NextChunkIndex == 0);
    YASSERT(!State.HasRunningOperation());

    if (InputChunks.size() != 0) {
        State.StartOperation();
        NextReader->Subscribe(BIND(
            &TChunkSequenceReader::SetCurrentChunk,
            MakeWeak(this)));
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
            NextReader->Subscribe(BIND(
                &TChunkSequenceReader::SetCurrentChunk,
                MakeWeak(this)));
            return;
        }
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
        NextReader->Subscribe(BIND(
            &TChunkSequenceReader::SetCurrentChunk,
            MakeWeak(this)));
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

const TKey& TChunkSequenceReader::GetCurrentKey() const
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(CurrentReader);
    YASSERT(CurrentReader->IsValid());

    return CurrentReader->GetCurrentKey();
}

TAsyncError TChunkSequenceReader::AsyncNextRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    State.StartOperation();
    
    CurrentReader->AsyncNextRow()->Subscribe(BIND(
        &TChunkSequenceReader::OnNextRow,
        MakeWeak(this)));

    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
