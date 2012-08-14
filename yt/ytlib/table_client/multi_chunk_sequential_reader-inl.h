#ifndef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include multi_chunk_sequential_reader.h"
#endif
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template<class TReaderProvider>
TMultiChunkSequentialReader<TReaderProvider>::TMultiChunkSequentialReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks,
    const TReaderProviderPtr& readerProvider)
    : TMultiChunkReaderBase<TReaderProvider>(
        Config, 
        masterChannel,
        blockCache,
        inputChunks,
        readerProvider)
    , CurrentReaderIndex(-1)
{
    LOG_DEBUG("Multi chunk sequential reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    Readers.reserve(InputChunk.size());
    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        Readers.push_back(NewPromise<TReaderPtr>());
    }
}

template<class TReaderProvider>
TAsyncError TMultiChunkSequentialReader<TReaderProvider>::AsyncOpen()
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
            &TMultiChunkSequentialReader<TReaderProvider>::SwitchCurrentChunk,
            MakeWeak(this))->Via(ReaderThread->GetInvoker()));
    }

    return State.GetOperationError();
}

template <class TReaderProvider>
void TMultiChunkSequentialReader<TReaderProvider>::OnReaderOpened(
    TReaderPtr chunkReader,
    int chunkIndex,
    TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        Readers[newReader].Set(TReaderPtr());
        return;
    }

    LOG_DEBUG("Chunk opened (ChunkIndex: %d)", chunkIndex);

    TMultiChunkReaderBase<TReaderProvider>::ProcessOpenedChunk(chunkReader);

    YCHECK(!Readers[chunkIndex].IsSet());
    Readers[chunkIndex].Set(chunkReader);
}

template <class TReaderProvider>
void TMultiChunkSequentialReader<TReaderProvider>::SwitchCurrentChunk(TReaderPtr nextReader)
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

template <class TReaderProvider>
bool TMultiChunkSequentialReader<TReaderProvider>::ValidateReader()
{
    if (!CurrentReader_->IsValid()) {
        ProcessFinishedReader(CurrentReader_);

        ++CurrentReaderIndex;
        if (CurrentReaderIndex < InputChunks.size()) {
            if (!State.HasRunningOperation())
                State.StartOperation();

            Readers[CurrentReaderIndex].Subscribe(BIND(
                &TMultiChunkSequentialReader<TReaderProvider>::SwitchCurrentChunk,
                MakeWeak(this)));
            return false;
        }
    }

    return true;
}

template <class TReaderProvider>
bool TMultiChunkSequentialReader<TReaderProvider>::FetchNextItem()
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
            IgnoreResult(&TMultiChunkSequentialReader<TReaderProvider>::OnItemFetched), 
            MakeWeak(this)));
        return false;
    }
}

template <class TReaderProvider>
void TMultiChunkSequentialReader<TReaderProvider>::OnItemFetched(TError error)
{
    YASSERT(State.HasRunningOperation());
    CHECK_ERROR(error);

    if (ValidateReader()) {
        ++ItemIndex_;
        State.FinishOperation();
    }
}

template <class TReaderProvider>
bool TMultiChunkSequentialReader<TReaderProvider>::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    return CurrentReader_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
