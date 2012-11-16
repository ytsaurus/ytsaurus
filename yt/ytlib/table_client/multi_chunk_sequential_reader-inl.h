#ifndef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_sequential_reader.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template<class TChunkReader>
TMultiChunkSequentialReader<TChunkReader>::TMultiChunkSequentialReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks,
    const typename TBase::TProviderPtr& readerProvider)
    : TMultiChunkReaderBase<TChunkReader>(
        config, 
        masterChannel,
        blockCache,
        MoveRV(inputChunks),
        readerProvider)
    , CurrentReaderIndex(-1)
{
    LOG_DEBUG("Multi chunk sequential reader created (ChunkCount: %d)", 
        static_cast<int>(TBase::InputChunks.size()));

    Sessions.reserve(TBase::InputChunks.size());
    for (int i = 0; i < static_cast<int>(TBase::InputChunks.size()); ++i) {
        Sessions.push_back(NewPromise<typename TBase::TSession>());
    }
}

template<class TChunkReader>
TAsyncError TMultiChunkSequentialReader<TChunkReader>::AsyncOpen()
{
    YCHECK(CurrentReaderIndex == -1);
    YCHECK(!State.HasRunningOperation());

    for (int i = 0; i < TBase::Config->PrefetchWindow; ++i) {
        TBase::PrepareNextChunk();
    }

    ++CurrentReaderIndex;

    if (CurrentReaderIndex < TBase::InputChunks.size()) {
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
        State.Fail(error);
        Sessions[session.ChunkIndex].Set(session);
        TBase::AddFailedChunk(session);
    } else {
        LOG_DEBUG("Chunk opened (ChunkIndex: %d)", session.ChunkIndex);
        TBase::ProcessOpenedReader(session);
        YCHECK(!Sessions[session.ChunkIndex].IsSet());
    }
    Sessions[session.ChunkIndex].Set(session);
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk(
    typename TBase::TSession nextSession)
{
    if (CurrentReaderIndex > 0 && !TBase::ReaderProvider->KeepInMemory()) {
        auto reader = Sessions[CurrentReaderIndex - 1].Get().Reader;
        reader.Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReaderIndex);
    YCHECK(!TBase::CurrentSession.Reader);

    if (nextSession.Reader) {
        TBase::CurrentSession = nextSession;
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
    if (!TBase::CurrentSession.Reader->IsValid()) {
        TBase::ProcessFinishedReader(TBase::CurrentSession);
        TBase::CurrentSession = typename TBase::TSession();

        ++CurrentReaderIndex;
        if (CurrentReaderIndex < TBase::InputChunks.size()) {
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
bool TMultiChunkSequentialReader<TChunkReader>::FetchNextItem()
{
    YCHECK(!State.HasRunningOperation());
    YCHECK(IsValid());

    if (TBase::CurrentSession.Reader->FetchNextItem()) {
        if (!ValidateReader()) {
            return false;
        }

        if (TBase::CurrentSession.Reader) {
            ++TBase::ItemIndex_;
        }
        return true;
    } else {
        State.StartOperation();
        TBase::CurrentSession.Reader->GetReadyEvent().Subscribe(
            BIND(IgnoreResult(&TMultiChunkSequentialReader<TChunkReader>::OnItemFetched), MakeWeak(this)));
        return false;
    }
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::OnItemFetched(TError error)
{
    YCHECK(State.HasRunningOperation());
    if (!error.IsOK()) {
        State.Fail(error);
        TBase::AddFailedChunk(TBase::CurrentSession);
        return;
    }

    if (ValidateReader()) {
        ++TBase::ItemIndex_;
        State.FinishOperation();
    }
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::IsValid() const
{
    YCHECK(!State.HasRunningOperation());
    return TBase::CurrentSession.Reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
