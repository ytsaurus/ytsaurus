#include "multi_reader_manager_base.h"

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NTableClient;

using NYT::FromProto;
using NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

class TSequentialMultiReaderManager
    : public TMultiReaderManagerBase
{
public:
    TSequentialMultiReaderManager(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        std::vector<IReaderFactoryPtr> readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    TMultiReaderManagerUnreadState GetUnreadState() const override;

private:
    int NextReaderIndex_ = 0;
    int FinishedReaderCount_ = 0;
    std::vector<TPromise<IReaderBasePtr>> NextReaders_;

    TFuture<void> DoOpen() override;

    void OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex) override;
    void OnReaderBlocked() override;
    void OnReaderFinished() override;

    TFuture<void> WaitForNextReader();
    void OnGotNextReader(const IReaderBasePtr& reader);

    TFuture<void> WaitForCurrentReader();
    void OnCurrentReaderReady(const TError& error);

    void PropagateError(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TSequentialMultiReaderManager::TSequentialMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
    : TMultiReaderManagerBase(
        std::move(config),
        std::move(options),
        std::move(readerFactories),
        std::move(multiReaderMemoryManager))
{
    YT_LOG_DEBUG("Multi chunk reader is sequential");
    NextReaders_.reserve(ReaderFactories_.size());
    for (int i = 0; i < std::ssize(ReaderFactories_); ++i) {
        NextReaders_.push_back(NewPromise<IReaderBasePtr>());
    }

    UncancelableCompletionError_.Subscribe(
        BIND(&TSequentialMultiReaderManager::PropagateError, MakeWeak(this))
            .Via(ReaderInvoker_));
}

TFuture<void> TSequentialMultiReaderManager::DoOpen()
{
    OpenNextReaders();
    return WaitForNextReader();
}

void TSequentialMultiReaderManager::OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex)
{
    // May have already been set in case of error.
    NextReaders_[chunkIndex].TrySet(std::move(chunkReader));
}

void TSequentialMultiReaderManager::PropagateError(const TError& error)
{
    if (error.IsOK()) {
        return;
    }

    for (const auto& nextReader : NextReaders_) {
        if (nextReader) {
            nextReader.TrySet(error);
        }
    }
}

void TSequentialMultiReaderManager::OnReaderBlocked()
{
    SetReadyEvent(CombineCompletionError(BIND(&TSequentialMultiReaderManager::WaitForCurrentReader, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()));
}

void TSequentialMultiReaderManager::OnReaderFinished()
{
    TMultiReaderManagerBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == std::ssize(ReaderFactories_)) {
        CompletionError_.TrySet(TError());
        return;
    }

    SetReadyEvent(CombineCompletionError(BIND(&TSequentialMultiReaderManager::WaitForNextReader, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()));
}

TFuture<void> TSequentialMultiReaderManager::WaitForNextReader()
{
    if (NextReaderIndex_ == std::ssize(ReaderFactories_)) {
        return VoidFuture;
    }

    return NextReaders_[NextReaderIndex_].ToFuture().Apply(
        BIND(&TSequentialMultiReaderManager::OnGotNextReader, MakeWeak(this))
            .AsyncVia(ReaderInvoker_));
}

void TSequentialMultiReaderManager::OnGotNextReader(const IReaderBasePtr& reader)
{
    {
        auto guard = Guard(ActiveReadersLock_);
        CurrentSession_.Index = NextReaderIndex_;
        CurrentSession_.Reader = reader;
        ++NextReaderIndex_;
    }

    // Avoid memory leaks, drop smart pointer reference.
    NextReaders_[CurrentSession_.Index].Reset();

    ReaderSwitched_.Fire();
}

TFuture<void> TSequentialMultiReaderManager::WaitForCurrentReader()
{
    return CurrentSession_.Reader->GetReadyEvent().Apply(
        BIND(&TSequentialMultiReaderManager::OnCurrentReaderReady, MakeWeak(this))
            .AsyncVia(ReaderInvoker_));
}

void TSequentialMultiReaderManager::OnCurrentReaderReady(const TError& error)
{
    if (!error.IsOK()) {
        RegisterFailedReader(CurrentSession_.Reader);
        CompletionError_.TrySet(error);
    }
}

TMultiReaderManagerUnreadState TSequentialMultiReaderManager::GetUnreadState() const
{
    auto guard = Guard(ActiveReadersLock_);
    TMultiReaderManagerUnreadState state;
    state.CurrentReader = CurrentSession_.Reader;
    for (int index = NextReaderIndex_; index < static_cast<int>(ReaderFactories_.size()); ++index) {
        state.ReaderFactories.push_back(ReaderFactories_[index]);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

IMultiReaderManagerPtr CreateSequentialMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    return New<TSequentialMultiReaderManager>(
        std::move(config),
        std::move(options),
        std::move(readerFactories),
        std::move(multiReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
