#ifndef MULTI_CHUNK_PARALLEL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_parallel_reader.h"
#endif
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TMultiChunkParallelReader<TChunkReader>::TMultiChunkParallelReader(
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
    , CompleteReaderCount(0)
{
    ReadySessions.reserve(std::min(
        static_cast<int>(TBase::InputChunks.size()), 
        TBase::Config->PrefetchWindow));

    if (TBase::ReaderProvider->KeepInMemory()) {
        CompleteSessions.reserve(TBase::InputChunks.size());
    }
}

template <class TChunkReader>
TAsyncError TMultiChunkParallelReader<TChunkReader>::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    if (TBase::InputChunks.size() != 0) {
        State.StartOperation();

        for (int i = 0; i < TBase::Config->PrefetchWindow; ++i) {
            TBase::PrepareNextChunk();
        }
    }

    return State.GetOperationError();
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::OnReaderOpened(
    const typename TBase::TSession& session, 
    TError error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    if (!error.IsOK()) {
        State.Fail(error);
        TBase::AddFailedChunk(session);
        return;
    }

    TBase::ProcessOpenedReader(session);
    ProcessReadyReader(session);
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::ProcessReadyReader(
    typename TBase::TSession session)
{
    if (!session.Reader->IsValid()) {
        // Reader is not valid - shift window.
        TBase::PrepareNextChunk();
        FinishReader(session);
        session = typename TBase::TSession();
    }

    bool finishOperation = false;
    bool isReadingComplete = false;
    {
        TGuard<TSpinLock> guard(SpinLock);

        finishOperation = !CurrentSession.Reader;

        if (!session.Reader) {
            isReadingComplete = (++CompleteReaderCount == TBase::InputChunks.size());
        } else if (!CurrentSession.Reader) {
            ++TBase::ItemIndex_;
            CurrentSession = session;
        } else {
            YCHECK(session.Reader);
            // This is quick operation - no reallocation here due to reserve in ctor.
            ReadySessions.push_back(session);
        }
    }

    if ((session.Reader || isReadingComplete) && finishOperation) {
        YCHECK(State.HasRunningOperation());
        YCHECK(!CurrentSession.Reader || IsValid());
        State.FinishOperation();
    }
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::FinishReader(
    const typename TBase::TSession& session)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);

    LOG_DEBUG("Reader finished (CompleteReaderCount: %d)", CompleteReaderCount);

    if (TBase::ReaderProvider->KeepInMemory()) {
        CompleteSessions.push_back(session);
    }
    TBase::ProcessFinishedReader(session);
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::OnReaderReady(
    const typename TBase::TSession& session, 
    TError error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    if (!error.IsOK()) {
        State.Fail(error);
        TBase::AddFailedChunk(session);
        return;
    }

    ProcessReadyReader(session);
}

template <class TChunkReader>
bool TMultiChunkParallelReader<TChunkReader>::FetchNextItem()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    bool isReaderComplete = false;
    if (CurrentSession.Reader->FetchNextItem()) {
        if (CurrentSession.Reader->IsValid()) {
            ++TBase::ItemIndex_;
            return true;
        }

        isReaderComplete = true;
        NChunkClient::TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
            &TMultiChunkParallelReader<TChunkReader>::FinishReader,
            MakeWeak(this),
            CurrentSession));
        TBase::PrepareNextChunk();
    } else {
        CurrentSession.Reader->GetReadyEvent().Subscribe(
            BIND(&TMultiChunkParallelReader<TChunkReader>::OnReaderReady,
                MakeWeak(this),
                CurrentSession)
            .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
    }


    TGuard<TSpinLock> guard(SpinLock);
    if (isReaderComplete) {
        if (++CompleteReaderCount == TBase::InputChunks.size()) {
            return true;
        }
    }

    if (ReadySessions.empty()) {
        CurrentSession = typename TBase::TSession();
        State.StartOperation();
        return false;
    } else {
        ++TBase::ItemIndex_;
        CurrentSession = ReadySessions.back();
        ReadySessions.pop_back();
        return true;
    }
}

template <class TChunkReader>
bool TMultiChunkParallelReader<TChunkReader>::IsValid() const
{
    if (CompleteReaderCount == TBase::InputChunks.size())
        return false;

    YCHECK(CurrentSession.Reader);
    YCHECK(CurrentSession.Reader->IsValid());

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
