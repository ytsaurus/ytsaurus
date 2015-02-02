#ifndef MULTI_CHUNK_PARALLEL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include old_multi_chunk_parallel_reader.h"
#endif
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
TOldMultiChunkParallelReader<TChunkReader>::TOldMultiChunkParallelReader(
    TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr compressedBlockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
    typename TBase::TProviderPtr readerProvider)
    : TOldMultiChunkReaderBase<TChunkReader>(
        config,
        masterChannel,
        compressedBlockCache,
        nodeDirectory,
        std::move(chunkSpecs),
        readerProvider)
    , CompleteReaderCount(0)
{
    srand(time(nullptr));
    std::random_shuffle(ChunkSpecs.begin(), ChunkSpecs.end());

    ReadySessions.reserve(std::min(
        static_cast<int>(ChunkSpecs.size()),
        PrefetchWindow));

    if (ReaderProvider->KeepInMemory()) {
        CompleteSessions.resize(ChunkSpecs.size());
    }
}

template <class TChunkReader>
TFuture<void> TOldMultiChunkParallelReader<TChunkReader>::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    if (ChunkSpecs.size() != 0) {
        State.StartOperation();

        TBase::PrepareNextChunk();
        for (int i = 0; i < TBase::PrefetchWindow; ++i) {
            TBase::PrepareNextChunk();
        }
    }

    return State.GetOperationError();
}

template <class TChunkReader>
void TOldMultiChunkParallelReader<TChunkReader>::OnReaderOpened(
    const typename TBase::TSession& session,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    if (!error.IsOK()) {
        TBase::AddFailedChunk(session);
        State.Fail(error);
        return;
    }

    TBase::ProcessOpenedReader(session);
    ProcessReadyReader(session);
}

template <class TChunkReader>
void TOldMultiChunkParallelReader<TChunkReader>::ProcessReadyReader(
    typename TBase::TSession session)
{
    if (!session.Reader->GetFacade()) {
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
            ++CompleteReaderCount;
            isReadingComplete = (CompleteReaderCount == ChunkSpecs.size());
        } else if (finishOperation) {
            CurrentSession = session;
        } else {
            // This is quick - no reallocation here due to reserve in ctor.
            ReadySessions.push_back(session);
        }
    }

    if ((session.Reader || isReadingComplete) && finishOperation) {
        YCHECK(!CurrentSession.Reader || CurrentSession.Reader->GetFacade());
        State.FinishOperation();
    }
}

template <class TChunkReader>
void TOldMultiChunkParallelReader<TChunkReader>::FinishReader(
    const typename TBase::TSession& session)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);

    LOG_DEBUG("Reader finished (ChunkIndex: %v)", session.ChunkIndex);

    if (ReaderProvider->KeepInMemory()) {
        CompleteSessions[session.ChunkIndex] = session;
    }
    TBase::ProcessFinishedReader(session);
}

template <class TChunkReader>
void TOldMultiChunkParallelReader<TChunkReader>::OnReaderReady(
    const typename TBase::TSession& session,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    if (!error.IsOK()) {
        TBase::AddFailedChunk(session);
        State.Fail(error);
        return;
    }

    ProcessReadyReader(session);
}

template <class TChunkReader>
bool TOldMultiChunkParallelReader<TChunkReader>::FetchNext()
{
    YASSERT(!State.HasRunningOperation());

    bool isReaderComplete = false;
    if (CurrentSession.Reader->FetchNext()) {
        if (CurrentSession.Reader->GetFacade()) {
            return true;
        }

        isReaderComplete = true;
        NChunkClient::TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
            &TOldMultiChunkParallelReader<TChunkReader>::FinishReader,
            MakeWeak(this),
            CurrentSession));
        TBase::PrepareNextChunk();
    } else {
        CurrentSession.Reader->GetReadyEvent().Subscribe(
            BIND(&TOldMultiChunkParallelReader<TChunkReader>::OnReaderReady,
                MakeWeak(this),
                CurrentSession)
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
    }

    TGuard<TSpinLock> guard(SpinLock);
    if (isReaderComplete) {
        ++CompleteReaderCount;
        if (CompleteReaderCount == ChunkSpecs.size()) {
            return true;
        }
    }

    if (ReadySessions.empty()) {
        CurrentSession = typename TBase::TSession();
        State.StartOperation();
        return false;
    } else {
        CurrentSession = ReadySessions.back();
        ReadySessions.pop_back();
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
