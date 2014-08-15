#ifndef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include old_multi_chunk_sequential_reader.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TOldMultiChunkSequentialReader<TChunkReader>::TOldMultiChunkSequentialReader(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
    typename TBase::TProviderPtr readerProvider)
    : TOldMultiChunkReaderBase<TChunkReader>(
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
TAsyncError TOldMultiChunkSequentialReader<TChunkReader>::AsyncOpen()
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
            BIND(&TOldMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk, MakeWeak(this))
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
    }

    return State.GetOperationError();
}

template <class TChunkReader>
void TOldMultiChunkSequentialReader<TChunkReader>::OnReaderOpened(
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
void TOldMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk(
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
bool TOldMultiChunkSequentialReader<TChunkReader>::ValidateReader()
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
                BIND(&TOldMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk, MakeWeak(this))
                    .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
            return false;
        }
    }

    return true;
}

template <class TChunkReader>
bool TOldMultiChunkSequentialReader<TChunkReader>::FetchNext()
{
    YCHECK(!State.HasRunningOperation());
    YCHECK(TBase::GetFacade());

    if (CurrentSession.Reader->FetchNext()) {
        return ValidateReader();
    } else {
        State.StartOperation();
        CurrentSession.Reader->GetReadyEvent().Subscribe(BIND(
            IgnoreResult(&TOldMultiChunkSequentialReader<TChunkReader>::OnItemFetched),
            MakeWeak(this)));
        return false;
    }
}

template <class TChunkReader>
void TOldMultiChunkSequentialReader<TChunkReader>::OnItemFetched(TError error)
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
