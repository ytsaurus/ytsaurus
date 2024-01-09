#include "multi_reader_manager_base.h"

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NTableClient;

using NYT::FromProto;
using NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

TMultiReaderManagerBase::TMultiReaderManagerBase(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
    : Id_(TGuid::Create())
    , Config_(config)
    , Options_(options)
    , ReaderFactories_(std::move(readerFactories))
    , MultiReaderMemoryManager_(std::move(multiReaderMemoryManager))
    , Logger(ChunkClientLogger.WithTag("MultiReaderId: %v", Id_))
    , UncancelableCompletionError_(CompletionError_.ToFuture().ToUncancelable())
    , ReaderInvoker_(CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker()))
{
    YT_LOG_DEBUG("Creating multi reader (ReaderCount: %v)",
        ReaderFactories_.size());

    MultiReaderMemoryManager_->AddChunkReaderInfo(Id_);

    UncancelableCompletionError_.Subscribe(
        BIND([Logger = Logger] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Multi reader completed");
            } else {
                YT_LOG_WARNING(error, "Multi reader failed");
            }
        }));

    if (ReaderFactories_.empty()) {
        CompletionError_.Set();
        SetReadyEvent(UncancelableCompletionError_);
        return;
    }

    NonOpenedReaderIndexes_.reserve(ReaderFactories_.size());
    for (int i = 0; i < static_cast<int>(ReaderFactories_.size()); ++i) {
        YT_VERIFY(NonOpenedReaderIndexes_.insert(i).second);
    }
}

TMultiReaderManagerBase::~TMultiReaderManagerBase()
{
    YT_UNUSED_FUTURE(MultiReaderMemoryManager_->Finalize());

    YT_LOG_DEBUG("Multi reader manager data statistics (DataStatistics: %v)", TMultiReaderManagerBase::GetDataStatistics());
    YT_LOG_DEBUG("Multi reader manager decompression codec statistics (CodecStatistics: %v)", TMultiReaderManagerBase::GetDecompressionStatistics());
    YT_LOG_DEBUG("Multi reader manager timing statistics (TimingStatistics: %v)", TMultiReaderManagerBase::GetTimingStatistics());
}

void TMultiReaderManagerBase::Open()
{
    SetReadyEvent(CombineCompletionError(BIND(&TMultiReaderManagerBase::DoOpen, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()));
}

const NLogging::TLogger& TMultiReaderManagerBase::GetLogger() const
{
    return Logger;
}

TMultiReaderManagerSession& TMultiReaderManagerBase::GetCurrentSession()
{
    return CurrentSession_;
}

TDataStatistics TMultiReaderManagerBase::GetDataStatistics() const
{
    auto guard = Guard(ActiveReadersLock_);
    auto dataStatistics = DataStatistics_;
    for (const auto& reader : ActiveReaders_) {
        dataStatistics += reader->GetDataStatistics();
    }
    return dataStatistics;
}

TCodecStatistics TMultiReaderManagerBase::GetDecompressionStatistics() const
{
    auto guard = Guard(ActiveReadersLock_);
    auto result = DecompressionStatistics_;
    for (const auto& reader : ActiveReaders_) {
        result += reader->GetDecompressionStatistics();
    }
    return result;
}

NTableClient::TTimingStatistics TMultiReaderManagerBase::GetTimingStatistics() const
{
    NTableClient::TTimingStatistics result;
    result.WaitTime = GetWaitTime();
    // Chunk readers do not provide Read().
    result.ReadTime = TDuration::Zero();
    result.IdleTime = TotalTimer_.GetElapsedTime() - result.WaitTime;
    return result;
}

bool TMultiReaderManagerBase::IsFetchingCompleted() const
{
    if (OpenedReaderCount_ == std::ssize(ReaderFactories_)) {
        auto guard = Guard(ActiveReadersLock_);
        for (const auto& reader : ActiveReaders_) {
            if (!reader->IsFetchingCompleted()) {
                return false;
            }
        }
    }
    return true;
}

std::vector<TChunkId> TMultiReaderManagerBase::GetFailedChunkIds() const
{
    auto guard = Guard(FailedChunksLock_);
    return std::vector<TChunkId>(FailedChunks_.begin(), FailedChunks_.end());
}

void TMultiReaderManagerBase::OpenNextReaders()
{
    auto guard = Guard(PrefetchLock_);

    if (PrefetchIndex_ >= std::ssize(ReaderFactories_)) {
        return;
    }

    if (CreatingReader_) {
        return;
    }

    if (!ReaderFactories_[PrefetchIndex_]->CanCreateReader() &&
        ActiveReaderCount_ > 0 &&
        !Options_->KeepInMemory)
    {
        return;
    }

    if (ActiveReaderCount_ >= Config_->MaxParallelReaders) {
        return;
    }

    ++ActiveReaderCount_;

    YT_LOG_DEBUG("Scheduling next reader creation (Index: %v, ActiveReaderCount: %v)",
        PrefetchIndex_,
        ActiveReaderCount_.load());

    ReaderInvoker_->Invoke(
        BIND(&TMultiReaderManagerBase::DoCreateReader, MakeWeak(this), PrefetchIndex_));

    ++PrefetchIndex_;
}

void TMultiReaderManagerBase::OnNextReaderCreated(const TError& /*error*/)
{
    OpenNextReaders();
}

void TMultiReaderManagerBase::DoCreateReader(int index)
{
    if (CompletionError_.IsSet()) {
        return;
    }

    YT_LOG_DEBUG("Creating reader (Index: %v)", index);

    try {
        // NB: MakeStrong here delays MultiReaderMemoryManager finalization until child reader is fully created.
        ReaderFactories_[index]->CreateReader()
            .Subscribe(BIND(&TMultiReaderManagerBase::OnReaderCreated, MakeStrong(this), index)
                .Via(ReaderInvoker_));
    } catch (const std::exception& ex) {
        OnReaderCreated(index, ex);
    }
}

void TMultiReaderManagerBase::OnReaderCreated(
    int index,
    const TErrorOr<IReaderBasePtr>& readerOrError)
{
    if (CompletionError_.IsSet()) {
        return;
    }

    {
        auto guard = Guard(PrefetchLock_);
        CreatingReader_ = false;
    }

    OpenNextReaders();

    if (!readerOrError.IsOK()) {
        std::vector<TChunkId> chunkIds;
        const auto& descriptor = ReaderFactories_[index]->GetDataSliceDescriptor();
        for (const auto& chunkSpec : descriptor.ChunkSpecs) {
            chunkIds.push_back(FromProto<TChunkId>(chunkSpec.chunk_id()));
        }

        YT_LOG_WARNING(readerOrError, "Failed to create reader (Index: %v, ChunkIds: %v)",
            index,
            chunkIds);

        {
            auto guard = Guard(FailedChunksLock_);
            FailedChunks_.insert(chunkIds.begin(), chunkIds.end());
        }

        CompletionError_.TrySet(readerOrError);
        return;
    }

    const auto& reader = readerOrError.Value();
    reader->GetReadyEvent()
        .Subscribe(BIND(&TMultiReaderManagerBase::OnReaderReady, MakeWeak(this), reader, index)
            .Via(ReaderInvoker_));
}

void TMultiReaderManagerBase::OnReaderReady(
    const IReaderBasePtr& reader,
    int index,
    const TError& error)
{
    if (CompletionError_.IsSet()) {
        return;
    }

    if (!error.IsOK()) {
        RegisterFailedReader(reader);
        CompletionError_.TrySet(error);
        return;
    }

    OnReaderOpened(reader, index);

    int nonOpenedReaderCount;
    {
        auto guard = Guard(ActiveReadersLock_);
        YT_VERIFY(NonOpenedReaderIndexes_.erase(index) == 1);
        YT_VERIFY(ActiveReaders_.insert(reader).second);
        nonOpenedReaderCount = NonOpenedReaderIndexes_.size();
    }

    if (nonOpenedReaderCount == 0) {
        // All readers have been opened.
        YT_UNUSED_FUTURE(MultiReaderMemoryManager_->Finalize());
    }
}

void TMultiReaderManagerBase::OnReaderFinished()
{
    if (Options_->KeepInMemory) {
        FinishedReaders_.push_back(CurrentSession_.Reader);
    }

    {
        auto guard = Guard(ActiveReadersLock_);
        DataStatistics_ += CurrentSession_.Reader->GetDataStatistics();
        DecompressionStatistics_ += CurrentSession_.Reader->GetDecompressionStatistics();
        YT_VERIFY(ActiveReaders_.erase(CurrentSession_.Reader) == 1);
    }

    --ActiveReaderCount_;

    YT_LOG_DEBUG("Reader finished (Index: %v, ActiveReaderCount: %v)",
        CurrentSession_.Index,
        ActiveReaderCount_.load());

    CurrentSession_.Reset();
    OpenNextReaders();
}

bool TMultiReaderManagerBase::OnEmptyRead(bool readerFinished)
{
    if (readerFinished) {
        OnReaderFinished();
        return !CompletionError_.IsSet() || !CompletionError_.Get().IsOK();
    } else {
        OnReaderBlocked();
        return true;
    }
}

TFuture<void> TMultiReaderManagerBase::CombineCompletionError(TFuture<void> future)
{
    return AnySet(
        std::vector{std::move(future), UncancelableCompletionError_},
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
}

void TMultiReaderManagerBase::RegisterFailedReader(const IReaderBasePtr& reader)
{
    auto chunkIds = reader->GetFailedChunkIds();
    YT_LOG_WARNING("Chunk reader failed (ChunkIds: %v)", chunkIds);

    {
        auto guard = Guard(FailedChunksLock_);
        FailedChunks_.insert(chunkIds.begin(), chunkIds.end());
    }
}

void TMultiReaderManagerBase::Interrupt()
{
    CompletionError_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
