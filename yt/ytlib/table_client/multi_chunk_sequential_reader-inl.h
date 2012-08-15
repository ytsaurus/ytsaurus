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
    const TProviderPtr& readerProvider)
    : TMultiChunkReaderBase<TChunkReader>(
        Config, 
        masterChannel,
        blockCache,
        MoveRV(inputChunks),
        readerProvider)
    , CurrentReaderIndex(-1)
{
    LOG_DEBUG("Multi chunk sequential reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    Readers.reserve(InputChunks.size());
    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        Readers.push_back(NewPromise<TReaderPtr>());
    }
}

template<class TChunkReader>
TAsyncError TMultiChunkSequentialReader<TChunkReader>::AsyncOpen()
{
    YCHECK(CurrentReaderIndex == -1);
    YCHECK(!State.HasRunningOperation());

    for (int i = 0; i < Config->PrefetchWindow; ++i) {
        PrepareNextChunk();
    }

    ++CurrentReaderIndex;

    if (CurrentReaderIndex < InputChunks.size()) {
        State.StartOperation();
        Readers[CurrentReaderIndex].Subscribe(BIND(
            &TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk,
            MakeWeak(this)).Via(NChunkClient::ReaderThread->GetInvoker()));
    }

    return State.GetOperationError();
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::OnReaderOpened(
    const TReaderPtr& chunkReader,
    int chunkIndex,
    TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        Readers[CurrentReaderIndex].Set(TReaderPtr());
        return;
    }

    LOG_DEBUG("Chunk opened (ChunkIndex: %d)", chunkIndex);

    TMultiChunkReaderBase<TChunkReader>::ProcessOpenedReader(chunkReader, chunkIndex);

    YCHECK(!Readers[chunkIndex].IsSet());
    Readers[chunkIndex].Set(chunkReader);
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk(TReaderPtr nextReader)
{
    if (CurrentReaderIndex > 0 && !ReaderProvider->KeepInMemory()) {
        Readers[CurrentReaderIndex - 1].Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReaderIndex);
    CurrentReader_.Reset();

    if (nextReader) {
        CurrentReader_ = nextReader;
        PrepareNextChunk();

        if (!ValidateReader())
            return;
    }

    // Finishing AsyncOpen.
    State.FinishOperation();
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::ValidateReader()
{
    if (!CurrentReader_->IsValid()) {
        ProcessFinishedReader(CurrentReader_);

        ++CurrentReaderIndex;
        if (CurrentReaderIndex < InputChunks.size()) {
            if (!State.HasRunningOperation())
                State.StartOperation();

            Readers[CurrentReaderIndex].Subscribe(BIND(
                &TMultiChunkSequentialReader<TChunkReader>::SwitchCurrentChunk,
                MakeWeak(this)));
            return false;
        }
    }

    return true;
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::FetchNextItem()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    if (CurrentReader_->FetchNextItem()) {
        if (!ValidateReader()) {
            return false;
        } else {
            ++ItemIndex_;
            return true;
        }
    } else {
        State.StartOperation();
        CurrentReader_->GetReadyEvent().Subscribe(BIND(
            IgnoreResult(&TMultiChunkSequentialReader<TChunkReader>::OnItemFetched), 
            MakeWeak(this)));
        return false;
    }
}

template <class TChunkReader>
void TMultiChunkSequentialReader<TChunkReader>::OnItemFetched(TError error)
{
    YASSERT(State.HasRunningOperation());
    CHECK_ERROR(error);

    if (ValidateReader()) {
        ++ItemIndex_;
        State.FinishOperation();
    }
}

template <class TChunkReader>
bool TMultiChunkSequentialReader<TChunkReader>::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    return CurrentReader_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
