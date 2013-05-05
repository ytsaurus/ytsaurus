#ifndef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_sequential_reader.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TMultiChunkSequentialReader<TChunkReader>::TMultiChunkSequentialReader(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TInputChunk>&& inputChunks,
    typename TBase::TProviderPtr readerProvider)
    : TMultiChunkReaderBase<TChunkReader>(
        config,
        masterChannel,
        blockCache,
        nodeDirectory,
        std::move(inputChunks),
        readerProvider)
    , CurrentReaderIndex(-1)
{
    LOG_DEBUG("Multi chunk sequential reader created (ChunkCount: %d)",
        static_cast<int>(InputChunks.size()));

    Sessions.reserve(InputChunks.size());
    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        Sessions.push_back(NewPromise<typename TBase::TSession>());
    }
}

template <class TChunkReader>
TAsyncError TMultiChunkSequentialReader<TChunkReader>::AsyncOpen()
{
    YCHECK(CurrentReaderIndex == -1);
    YCHECK(!State.HasRunningOperation());

    for (int i = 0; i < PrefetchWindow; ++i) {
        TBase::PrepareNextChunk();
    }

    ++CurrentReaderIndex;

    if (CurrentReaderIndex < InputChunks.size()) {
        State.StartOperation();
        Sessions[CurrentReaderIndex].Subscribe(
            BIND(&TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk, MakeWeak(this))
            .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
    }

    return State.GetOperationError();
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::OnReaderOpened(
    const typename TBase::TSession& session,
    TError error)
{
    if (!error.IsOK()) {
        TBase::AddFailedChunk(session);
        State.Fail(error);
    } else {
        LOG_DEBUG("Chunk opened (ChunkIndex: %d)", session.ChunkIndex);
        TBase::ProcessOpenedReader(session);
    }
    Sessions[session.ChunkIndex].Set(session);
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk(
    typename TBase::TSession nextSession)
{
    if (CurrentReaderIndex > 0 && !ReaderProvider->KeepInMemory()) {
        Sessions[CurrentReaderIndex - 1].Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReaderIndex);
    YCHECK(!CurrentSession.Reader);

    if (nextSession.Reader) {
        CurrentSession = nextSession;
        TBase::PrepareNextChunk();

        if (!ValidateReader())
            return;
    }

    // Finishing AsyncOpen.
    State.FinishOperation();
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::ValidateReader()
{
    if (!CurrentSession.Reader->GetFacade()) {
        TBase::ProcessFinishedReader(CurrentSession);
        CurrentSession = typename TBase::TSession();

        ++CurrentReaderIndex;
        if (CurrentReaderIndex < InputChunks.size()) {
            if (!State.HasRunningOperation())
                State.StartOperation();

            Sessions[CurrentReaderIndex].Subscribe(
                BIND(&TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk, MakeWeak(this))
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
            return false;
        }
    }

    return true;
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::FetchNext()
{
    YCHECK(!State.HasRunningOperation());
    YCHECK(TBase::GetFacade());

    if (CurrentSession.Reader->FetchNext()) {
        return ValidateReader();
    } else {
        State.StartOperation();
        CurrentSession.Reader->GetReadyEvent().Subscribe(BIND(
            IgnoreResult(&TMultiChunkSequentialReader<TChunkReader>::OnItemFetched),
            MakeWeak(this)));
        return false;
    }
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::OnItemFetched(TError error)
{
    YCHECK(State.HasRunningOperation());

    if (!error.IsOK()) {
        TBase::AddFailedChunk(CurrentSession);
        State.Fail(error);
        return;
    }

    if (ValidateReader()) {
        State.FinishOperation();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
