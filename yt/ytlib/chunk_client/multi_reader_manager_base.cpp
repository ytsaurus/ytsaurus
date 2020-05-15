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
    const std::vector<IReaderFactoryPtr>& readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
    : Id_(TGuid::Create())
    , Config_(config)
    , Options_(options)
    , ReaderFactories_(readerFactories)
    , MultiReaderMemoryManager_(std::move(multiReaderMemoryManager))
    , Logger(NLogging::TLogger(ChunkClientLogger)
        .AddTag("MultiReaderId: %v", Id_))
    , UncancelableCompletionError_(CompletionError_.ToFuture().ToUncancelable())
    , ReaderInvoker_(CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker()))
{
    CurrentSession_.Reset();

    YT_LOG_DEBUG("Creating multi reader (ReaderCount: %v)",
        readerFactories.size());

    MultiReaderMemoryManager_->AddChunkReaderInfo(Id_);

    UncancelableCompletionError_.Subscribe(BIND([Logger = this->Logger, multiReaderMemoryManager = MultiReaderMemoryManager_] (const TError& error) {
        if (error.IsOK()) {
            YT_LOG_DEBUG("Reading completed");
        } else {
            YT_LOG_INFO(error, "Reading completed with error");
        }

        multiReaderMemoryManager->Finalize();
    }));

    if (readerFactories.empty()) {
        CompletionError_.Set(TError());
        ReadyEvent_ = UncancelableCompletionError_;
        return;
    }
    NonOpenedReaderIndexes_.reserve(readerFactories.size());
    for (int i = 0; i < static_cast<int>(readerFactories.size()); ++i) {
        NonOpenedReaderIndexes_.insert(i);
    }
}

void TMultiReaderManagerBase::Open()
{
    ReadyEvent_ = CombineCompletionError(BIND(
            &TMultiReaderManagerBase::DoOpen,
            MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run());
}

TFuture<void> TMultiReaderManagerBase::GetReadyEvent()
{
    return ReadyEvent_;
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
    TGuard<TSpinLock> guard(ActiveReadersLock_);
    auto dataStatistics = DataStatistics_;
    for (const auto& reader : ActiveReaders_) {
        dataStatistics += reader->GetDataStatistics();
    }
    return dataStatistics;
}

TCodecStatistics TMultiReaderManagerBase::GetDecompressionStatistics() const
{
    TGuard<TSpinLock> guard(ActiveReadersLock_);
    auto result = DecompressionStatistics_;
    for (const auto& reader : ActiveReaders_) {
        result += reader->GetDecompressionStatistics();
    }
    return result;
}

bool TMultiReaderManagerBase::IsFetchingCompleted() const
{
    if (OpenedReaderCount_ == ReaderFactories_.size()) {
        TGuard<TSpinLock> guard(ActiveReadersLock_);
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
    TGuard<TSpinLock> guard(FailedChunksLock_);
    return std::vector<TChunkId>(FailedChunks_.begin(), FailedChunks_.end());
}

void TMultiReaderManagerBase::OpenNextChunks()
{
    TGuard<TSpinLock> guard(PrefetchLock_);
    for (; PrefetchIndex_ < ReaderFactories_.size(); ++PrefetchIndex_) {
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

        YT_LOG_DEBUG("Creating next reader (Index: %v, ActiveReaderCount: %v)",
            PrefetchIndex_,
            ActiveReaderCount_.load());

        BIND(
            &TMultiReaderManagerBase::DoOpenReader,
            MakeWeak(this),
            PrefetchIndex_)
        .Via(ReaderInvoker_)
        .Run();
    }
}

void TMultiReaderManagerBase::DoOpenReader(int index)
{
    YT_LOG_DEBUG("Opening reader (Index: %v)", index);

    if (CompletionError_.IsSet()) {
        return;
    }

    IReaderBasePtr reader;
    TError error;
    try {
        reader = ReaderFactories_[index]->CreateReader();
        error = WaitFor(reader->GetReadyEvent());
    } catch (const std::exception& ex) {
        error = ex;
    }

    if (!error.IsOK()) {
        if (reader) {
            RegisterFailedReader(reader);
        } else {
            const auto& descriptor = ReaderFactories_[index]->GetDataSliceDescriptor();
            std::vector<TChunkId> chunkIds;
            for (const auto& chunkSpec : descriptor.ChunkSpecs) {
                chunkIds.push_back(FromProto<TChunkId>(chunkSpec.chunk_id()));
            }
            YT_LOG_WARNING("Failed to open reader (Index: %v, ChunkIds: %v)", index, chunkIds);

            TGuard<TSpinLock> guard(FailedChunksLock_);
            FailedChunks_.insert(chunkIds.begin(), chunkIds.end());
        }

        CompletionError_.TrySet(error);
    }

    if (CompletionError_.IsSet()) {
        return;
    }

    OnReaderOpened(reader, index);

    TGuard<TSpinLock> guard(ActiveReadersLock_);
    YT_VERIFY(NonOpenedReaderIndexes_.erase(index));
    YT_VERIFY(ActiveReaders_.insert(reader).second);
}

void TMultiReaderManagerBase::OnReaderFinished()
{
    if (Options_->KeepInMemory) {
        FinishedReaders_.push_back(CurrentSession_.Reader);
    }

    {
        TGuard<TSpinLock> guard(ActiveReadersLock_);
        DataStatistics_ += CurrentSession_.Reader->GetDataStatistics();
        DecompressionStatistics_ += CurrentSession_.Reader->GetDecompressionStatistics();
        YT_VERIFY(ActiveReaders_.erase(CurrentSession_.Reader));
    }

    --ActiveReaderCount_;

    YT_LOG_DEBUG("Reader finished (Index: %v, ActiveReaderCount: %v)",
        CurrentSession_.Index,
        static_cast<int>(ActiveReaderCount_));

    CurrentSession_.Reset();
    OpenNextChunks();
}

bool TMultiReaderManagerBase::OnEmptyRead(bool readerFinished)
{
    if (readerFinished || CompletionError_.IsSet()) {
        OnReaderFinished();
        return !CompletionError_.IsSet() || !CompletionError_.Get().IsOK();
    } else {
        OnReaderBlocked();
        return true;
    }
}

TFuture<void> TMultiReaderManagerBase::CombineCompletionError(TFuture<void> future)
{
    auto promise = NewPromise<void>();
    promise.TrySetFrom(UncancelableCompletionError_);
    promise.TrySetFrom(future);
    return promise.ToFuture();
}

void TMultiReaderManagerBase::RegisterFailedReader(IReaderBasePtr reader)
{
    auto chunkIds = reader->GetFailedChunkIds();
    YT_LOG_WARNING("Chunk reader failed (ChunkIds: %v)", chunkIds);

    TGuard<TSpinLock> guard(FailedChunksLock_);
    for (auto chunkId : chunkIds) {
        FailedChunks_.insert(chunkId);
    }
}

void TMultiReaderManagerBase::Interrupt()
{
    CompletionError_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
