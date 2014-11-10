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
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
    typename TBase::TProviderPtr readerProvider)
    : TMultiChunkReaderBase<TChunkReader>(
        config,
        masterChannel,
        blockCache,
        nodeDirectory,
        std::move(chunkSpecs),
        readerProvider)
    , CurrentReaderIndex(-1)
{
    LOG_DEBUG("Multi chunk sequential reader created (ChunkCount: %d)",
        static_cast<int>(ChunkSpecs.size()));

    Sessions.reserve(ChunkSpecs.size());
    for (int i = 0; i < static_cast<int>(ChunkSpecs.size()); ++i) {
        Sessions.push_back(NewPromise<typename TBase::TSession>());
    }
}

template <class TChunkReader>
TAsyncError TMultiChunkSequentialReader<TChunkReader>::AsyncOpen()
{
    YCHECK(CurrentReaderIndex == -1);
    YCHECK(!State.HasRunningOperation());

    if (ChunkSpecs.size() > 0) {
        TBase::PrepareNextChunk();
        for (int i = 0; i < PrefetchWindow; ++i) {
            TBase::PrepareNextChunk();
        }

        ++CurrentReaderIndex;

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

        TBase::PrepareNextChunk();

        ++CurrentReaderIndex;
        if (CurrentReaderIndex < ChunkSpecs.size()) {
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
    // Reader may have already failed, e.g. if prefetched chunk failed to open.
    if (!State.IsActive()) {
        return;
    }

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
