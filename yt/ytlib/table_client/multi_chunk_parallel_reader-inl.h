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
    ReadyReaders.reserve(std::min(
        static_cast<int>(TBase::InputChunks.size()), 
        TBase::Config->PrefetchWindow));

    if (TBase::ReaderProvider->KeepInMemory()) {
        CompleteReaders.reserve(TBase::InputChunks.size());
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
    const typename TBase::TReaderPtr& chunkReader, 
    int inputChunkIndex,
    TError error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    CHECK_ERROR(error);

    TBase::ProcessOpenedReader(chunkReader, inputChunkIndex);
    ProcessReadyReader(chunkReader);
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::ProcessReadyReader(
    typename TBase::TReaderPtr chunkReader)
{
    if (!chunkReader->IsValid()) {
        // Reader is not valid - shift window.
        TBase::PrepareNextChunk();
        FinishReader(chunkReader);
        chunkReader.Reset();
    }

    bool finishOperation = false;
    bool isReadingComplete = false;
    {
        TGuard<TSpinLock> guard(SpinLock);

        finishOperation = !CurrentReader_;

        if (!chunkReader) {
            isReadingComplete = (++CompleteReaderCount == TBase::InputChunks.size());
        } else if (!CurrentReader_) {
            ++TBase::ItemIndex_;
            CurrentReader_ = chunkReader;
        } else if (chunkReader) {
            // This is quick operation - no reallocation here due to reserve in ctor.
            ReadyReaders.push_back(chunkReader);
        }
    }

    if ((chunkReader || isReadingComplete) && finishOperation) {
        YCHECK(State.HasRunningOperation());
        YCHECK(!CurrentReader_ || IsValid());
        State.FinishOperation();
    }
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::FinishReader(
    const typename TBase::TReaderPtr& chunkReader)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);

    LOG_DEBUG("Reader finished (CompleteReaderCount: %d)", CompleteReaderCount);

    if (TBase::ReaderProvider->KeepInMemory()) {
        CompleteReaders.push_back(chunkReader);
    }
    TBase::ProcessFinishedReader(chunkReader);
}

template <class TChunkReader>
void TMultiChunkParallelReader<TChunkReader>::OnReaderReady(
    const typename TBase::TReaderPtr& chunkReader, 
    TError error)
{
    VERIFY_THREAD_AFFINITY(TBase::ReaderThread);
    CHECK_ERROR(error);

    ProcessReadyReader(chunkReader);
}

template <class TChunkReader>
bool TMultiChunkParallelReader<TChunkReader>::FetchNextItem()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    bool isReaderComplete = false;
    if (CurrentReader_->FetchNextItem()) {
        if (CurrentReader_->IsValid()) {
            ++TBase::ItemIndex_;
            return true;
        }

        isReaderComplete = true;
        NChunkClient::ReaderThread->GetInvoker()->Invoke(BIND(
            &TMultiChunkParallelReader<TChunkReader>::FinishReader,
            MakeWeak(this),
            CurrentReader_));
        TBase::PrepareNextChunk();
    } else {
        CurrentReader_->GetReadyEvent().Subscribe(BIND(
            &TMultiChunkParallelReader<TChunkReader>::OnReaderReady,
            MakeWeak(this),
            CurrentReader_).Via(NChunkClient::ReaderThread->GetInvoker()));
    }


    TGuard<TSpinLock> guard(SpinLock);
    if (isReaderComplete) {
        if (++CompleteReaderCount == TBase::InputChunks.size())
            return true;
    }

    if (ReadyReaders.empty()) {
        CurrentReader_.Reset();
        State.StartOperation();
        return false;
    } else {
        ++TBase::ItemIndex_;
        CurrentReader_ = ReadyReaders.back();
        ReadyReaders.pop_back();
        return true;
    }
}

template <class TChunkReader>
bool TMultiChunkParallelReader<TChunkReader>::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    if (CompleteReaderCount == TBase::InputChunks.size())
        return false;

    YASSERT(CurrentReader_);
    YASSERT(CurrentReader_->IsValid());

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
