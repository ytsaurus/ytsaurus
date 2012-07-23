#ifndef CHUNK_SEQUENCE_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_sequence_reader_base.h"
#endif
#undef CHUNK_SEQUENCE_READER_BASE_INL_H_

#include "private.h"
#include "config.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TReader>
TChunkSequenceReaderBase<TReader>::TChunkSequenceReaderBase(
    TChunkSequenceReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks)
    : Config(config)
    , BlockCache(blockCache)
    , InputChunks(inputChunks)
    , MasterChannel(masterChannel)
    , CurrentReaderIndex(-1)
    , LastInitializedReader(-1)
    , LastPreparedReader(-1)
    , ItemIndex_(0)
    , Logger(TableReaderLogger)
{
    LOG_DEBUG("Chunk sequence reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        Readers.push_back(NewPromise<TReaderPtr>());
    }
}

template <class TReader>
TAsyncError TChunkSequenceReaderBase<TReader>::AsyncOpen()
{
    YASSERT(CurrentReaderIndex == -1);
    YASSERT(!State.HasRunningOperation());

    for (int i = 0; i < Config->PrefetchWindow; ++i) {
        PrepareNextChunk();
    }

    ++CurrentReaderIndex;

    if (CurrentReaderIndex < InputChunks.size()) {
        State.StartOperation();
        Readers[CurrentReaderIndex].Subscribe(BIND(
            &TChunkSequenceReaderBase<TReader>::SwitchCurrentChunk,
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

template <class TReader>
void TChunkSequenceReaderBase<TReader>::PrepareNextChunk()
{
    int chunkSlicesSize = static_cast<int>(InputChunks.size());

    ++LastPreparedReader;
    if (LastPreparedReader >= chunkSlicesSize)
        return;

    const auto& inputChunk = InputChunks[LastPreparedReader];
    auto chunkId = NChunkServer::TChunkId::FromProto(inputChunk.slice().chunk_id());

    LOG_DEBUG("Opening chunk (ChunkIndex: %d, ChunkId: %s)", 
        LastPreparedReader,
        ~chunkId.ToString());

    auto remoteReader = CreateRemoteReader(
        Config->RemoteReader,
        BlockCache,
        MasterChannel,
        chunkId,
        FromProto<Stroka>(inputChunk.node_addresses()));

    auto chunkReader = CreateNewReader(inputChunk, remoteReader);
    chunkReader->AsyncOpen().Subscribe(BIND(
        &TChunkSequenceReaderBase<TReader>::OnReaderOpened,
        MakeWeak(this),
        chunkReader,
        LastPreparedReader).Via(NChunkClient::ReaderThread->GetInvoker()));
}

template <class TReader>
void TChunkSequenceReaderBase<TReader>::OnReaderOpened(
    TReaderPtr reader, 
    int chunkIndex, 
    TError error)
{
    ++LastInitializedReader;

    LOG_DEBUG("Chunk opened (ChunkIndex: %d, ReaderIndex: %d)", 
        chunkIndex, 
        LastInitializedReader);

    YASSERT(!Readers[LastInitializedReader].IsSet());

    if (error.IsOK()) {
        Readers[LastInitializedReader].Set(reader);
        return;
    }

    State.Fail(error);
    Readers[LastInitializedReader].Set(TReaderPtr());
}

template <class TReader>
void TChunkSequenceReaderBase<TReader>::SwitchCurrentChunk(TReaderPtr nextReader)
{
    if (CurrentReaderIndex > 0 && !KeepReaders()) {
        Readers[CurrentReaderIndex - 1].Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReaderIndex);
    CurrentReader_.Reset();

    if (nextReader) {
        OnChunkSwitch(nextReader);

        CurrentReader_ = nextReader;

        PrepareNextChunk();

        if (!ValidateReader())
            return;
    }

    // Finishing AsyncOpen.
    State.FinishOperation();
}

template <class TReader>
bool TChunkSequenceReaderBase<TReader>::ValidateReader()
{
    if (!CurrentReader_->IsValid()) {
        ++CurrentReaderIndex;
        if (CurrentReaderIndex < InputChunks.size()) {
            if (!State.HasRunningOperation())
                State.StartOperation();

            Readers[CurrentReaderIndex].Subscribe(BIND(
                &TChunkSequenceReaderBase<TReader>::SwitchCurrentChunk,
                MakeWeak(this)));
            return false;
        }
    }

    return true;
}

template <class TReader>
bool TChunkSequenceReaderBase<TReader>::FetchNextItem()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    if (CurrentReader_->FetchNextItem()) {
        return OnItemFetched(TError());
    } else {
        State.StartOperation();
        CurrentReader_->GetReadyEvent().Subscribe(BIND(
            IgnoreResult(&TChunkSequenceReaderBase<TReader>::OnItemFetched), 
            MakeWeak(this)));
        return false;
    }
}

template <class TReader>
TAsyncError TChunkSequenceReaderBase<TReader>::GetReadyEvent()
{
    return State.GetOperationError();
}

template <class TReader>
bool TChunkSequenceReaderBase<TReader>::OnItemFetched(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return true;
    }

    if (!ValidateReader())
        return false;

    ++ItemIndex_;
    if (State.HasRunningOperation())
        State.FinishOperation();

    return true;
}

template <class TReader>
bool TChunkSequenceReaderBase<TReader>::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    if (CurrentReaderIndex >= InputChunks.size())
        return false;

    return CurrentReader_->IsValid();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
