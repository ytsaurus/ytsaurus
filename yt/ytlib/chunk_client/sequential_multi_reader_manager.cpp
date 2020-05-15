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
        const std::vector<IReaderFactoryPtr>& readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    virtual TMultiReaderManagerUnreadState GetUnreadState() const override;

private:
    int NextReaderIndex_ = 0;
    int FinishedReaderCount_ = 0;
    std::vector<TPromise<IReaderBasePtr>> NextReaders_;

    virtual void DoOpen() override;

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    void WaitForNextReader();

    void WaitForCurrentReader();

    void PropagateError(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TSequentialMultiReaderManager::TSequentialMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
    : TMultiReaderManagerBase(
        std::move(config),
        std::move(options),
        readerFactories,
        std::move(multiReaderMemoryManager))
{
    YT_LOG_DEBUG("Multi chunk reader is sequential");
    NextReaders_.reserve(ReaderFactories_.size());
    for (int i = 0; i < ReaderFactories_.size(); ++i) {
        NextReaders_.push_back(NewPromise<IReaderBasePtr>());
    }

    UncancelableCompletionError_.Subscribe(
        BIND(&TSequentialMultiReaderManager::PropagateError, MakeWeak(this))
            .Via(ReaderInvoker_));
}

void TSequentialMultiReaderManager::DoOpen()
{
    OpenNextChunks();
    WaitForNextReader();
}

void TSequentialMultiReaderManager::OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex)
{
    // May have already been set in case of error.
    NextReaders_[chunkIndex].TrySet(chunkReader);
}

void TSequentialMultiReaderManager::PropagateError(const TError& error)
{
    if (error.IsOK()) {
        return;
    }

    for (auto nextReader : NextReaders_) {
        if (nextReader) {
            nextReader.TrySet(error);
        }
    }
}

void TSequentialMultiReaderManager::OnReaderBlocked()
{
    ReadyEvent_ = CombineCompletionError(BIND(
        &TSequentialMultiReaderManager::WaitForCurrentReader,
        MakeStrong(this))
    .AsyncVia(ReaderInvoker_)
    .Run());
}

void TSequentialMultiReaderManager::OnReaderFinished()
{
    TMultiReaderManagerBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == ReaderFactories_.size()) {
        CompletionError_.TrySet(TError());
        return;
    }

    ReadyEvent_ = CombineCompletionError(BIND(
        &TSequentialMultiReaderManager::WaitForNextReader,
        MakeStrong(this))
    .AsyncVia(ReaderInvoker_)
    .Run());
}

void TSequentialMultiReaderManager::WaitForNextReader()
{
    if (NextReaderIndex_ == ReaderFactories_.size()) {
        return;
    }

    auto currentReader = WaitFor(NextReaders_[NextReaderIndex_].ToFuture())
        .ValueOrThrow();

    {
        TGuard<TSpinLock> guard(ActiveReadersLock_);
        CurrentSession_.Index = NextReaderIndex_;
        CurrentSession_.Reader = currentReader;

        ++NextReaderIndex_;
    }

    // Avoid memory leaks, drop smart pointer reference.
    NextReaders_[CurrentSession_.Index].Reset();

    ReaderSwitched_.Fire();
}

void TSequentialMultiReaderManager::WaitForCurrentReader()
{
    auto error = WaitFor(CurrentSession_.Reader->GetReadyEvent());
    if (!error.IsOK()) {
        RegisterFailedReader(CurrentSession_.Reader);
        CompletionError_.TrySet(error);
    }
}

TMultiReaderManagerUnreadState TSequentialMultiReaderManager::GetUnreadState() const
{
    TMultiReaderManagerUnreadState state;
    TGuard<TSpinLock> guard(ActiveReadersLock_);

    state.CurrentReader = CurrentSession_.Reader;
    for (int index = NextReaderIndex_; index < static_cast<int>(ReaderFactories_.size()); ++index) {
        state.ReaderFactories.emplace_back(ReaderFactories_[index]);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

IMultiReaderManagerPtr CreateSequentialMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    return New<TSequentialMultiReaderManager>(
        std::move(config),
        std::move(options),
        readerFactories,
        std::move(multiReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
