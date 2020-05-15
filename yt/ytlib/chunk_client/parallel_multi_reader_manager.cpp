#include "multi_reader_manager_base.h"

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NTableClient;

using NYT::FromProto;
using NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

class TParallelMultiReaderManager
    : public TMultiReaderManagerBase
{
public:
    TParallelMultiReaderManager(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        const std::vector<IReaderFactoryPtr>& readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    virtual TMultiReaderManagerUnreadState GetUnreadState() const override;

private:
    typedef NConcurrency::TNonblockingQueue<TMultiReaderManagerSession> TMultiReaderManagerSessionQueue;

    TMultiReaderManagerSessionQueue ReadySessions_;
    int FinishedReaderCount_ = 0;

    virtual void DoOpen() override;

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    void WaitForReadyReader();

    void WaitForReader(TMultiReaderManagerSession session);

    void PropagateError(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TParallelMultiReaderManager::TParallelMultiReaderManager(
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
    YT_LOG_DEBUG("Multi chunk reader is parallel");
    UncancelableCompletionError_.Subscribe(
        BIND(&TParallelMultiReaderManager::PropagateError, MakeWeak(this)));
}

void TParallelMultiReaderManager::DoOpen()
{
    OpenNextChunks();
    WaitForReadyReader();
}

void TParallelMultiReaderManager::OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex)
{
    TMultiReaderManagerSession session;
    session.Reader = chunkReader;
    session.Index = chunkIndex;

    ReadySessions_.Enqueue(session);
}

void TParallelMultiReaderManager::OnReaderBlocked()
{
    BIND(
        &TParallelMultiReaderManager::WaitForReader,
        MakeStrong(this),
        CurrentSession_)
    .AsyncVia(ReaderInvoker_)
    .Run();

    CurrentSession_.Reset();

    ReadyEvent_ = CombineCompletionError(BIND(
        &TParallelMultiReaderManager::WaitForReadyReader,
        MakeStrong(this))
    .AsyncVia(ReaderInvoker_)
    .Run());
}

void TParallelMultiReaderManager::OnReaderFinished()
{
    TMultiReaderManagerBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == ReaderFactories_.size()) {
        ReadySessions_.Enqueue(TError(NYT::EErrorCode::Canceled, "Multi reader finished"));
        CompletionError_.TrySet(TError());
    } else {
        ReadyEvent_ = CombineCompletionError(BIND(
            &TParallelMultiReaderManager::WaitForReadyReader,
            MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run());
    }
}

void TParallelMultiReaderManager::PropagateError(const TError& error)
{
    // Someone may wait for this future.
    if (error.IsOK()) {
        ReadySessions_.Enqueue(TError(NYT::EErrorCode::Canceled, "Multi reader finished"));
    } else {
        ReadySessions_.Enqueue(TError("Multi reader failed") << error);
    }
}

void TParallelMultiReaderManager::WaitForReadyReader()
{
    auto asyncReadySession = ReadySessions_.Dequeue();
    auto errorOrSession = WaitFor(asyncReadySession);

    if (errorOrSession.IsOK()) {
        CurrentSession_ = errorOrSession.Value();
        ReaderSwitched_.Fire();
    } else if (errorOrSession.FindMatching(NYT::EErrorCode::Canceled)) {
        // Do nothing, this is normal reader termination, e.g. during interrupt.
    } else {
        THROW_ERROR errorOrSession;
    }
}

void TParallelMultiReaderManager::WaitForReader(TMultiReaderManagerSession session)
{
    auto error = WaitFor(session.Reader->GetReadyEvent());
    if (error.IsOK()) {
        ReadySessions_.Enqueue(session);
        return;
    }

    RegisterFailedReader(session.Reader);
    CompletionError_.TrySet(error);
}

TMultiReaderManagerUnreadState TParallelMultiReaderManager::GetUnreadState() const
{
    TMultiReaderManagerUnreadState state;
    TGuard<TSpinLock> guard(ActiveReadersLock_);

    state.CurrentReader = CurrentSession_.Reader;
    state.ActiveReaders.reserve(ActiveReaders_.size());
    for (const auto& reader : ActiveReaders_) {
        if (reader != CurrentSession_.Reader) {
            state.ActiveReaders.emplace_back(reader);
        }
    }
    for (int index : NonOpenedReaderIndexes_) {
        state.ReaderFactories.emplace_back(ReaderFactories_[index]);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

IMultiReaderManagerPtr CreateParallelMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    return New<TParallelMultiReaderManager>(
        std::move(config),
        std::move(options),
        readerFactories,
        std::move(multiReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
