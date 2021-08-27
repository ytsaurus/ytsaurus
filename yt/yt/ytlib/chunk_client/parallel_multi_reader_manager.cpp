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
        std::vector<IReaderFactoryPtr> readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    TMultiReaderManagerUnreadState GetUnreadState() const override;

private:
    typedef NConcurrency::TNonblockingQueue<TMultiReaderManagerSession> TMultiReaderManagerSessionQueue;

    TMultiReaderManagerSessionQueue ReadySessions_;
    int FinishedReaderCount_ = 0;

    TFuture<void> DoOpen() override;

    void OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex) override;
    void OnReaderBlocked() override;
    void OnReaderFinished() override;

    TFuture<void> WaitForReadySession();
    void OnSessionReady(const TErrorOr<TMultiReaderManagerSession>& errorOrSession);

    void WaitForReader(const TMultiReaderManagerSession& session);
    void OnReaderReady(const TMultiReaderManagerSession& session, const TError& error);

    void PropagateError(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TParallelMultiReaderManager::TParallelMultiReaderManager(
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
    YT_LOG_DEBUG("Multi chunk reader is parallel");
    UncancelableCompletionError_.Subscribe(
        BIND(&TParallelMultiReaderManager::PropagateError, MakeWeak(this)));
}

TFuture<void> TParallelMultiReaderManager::DoOpen()
{
    OpenNextReaders();
    return WaitForReadySession();
}

void TParallelMultiReaderManager::OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex)
{
    TMultiReaderManagerSession session;
    session.Reader = std::move(chunkReader);
    session.Index = chunkIndex;
    ReadySessions_.Enqueue(session);
}

void TParallelMultiReaderManager::OnReaderBlocked()
{
    WaitForReader(CurrentSession_);
    CurrentSession_.Reset();

    SetReadyEvent(CombineCompletionError(BIND(&TParallelMultiReaderManager::WaitForReadySession, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()));
}

void TParallelMultiReaderManager::OnReaderFinished()
{
    TMultiReaderManagerBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == std::ssize(ReaderFactories_)) {
        ReadySessions_.Enqueue(TError(NYT::EErrorCode::Canceled, "Multi reader finished"));
        CompletionError_.TrySet();
    } else {
        SetReadyEvent(CombineCompletionError(BIND(&TParallelMultiReaderManager::WaitForReadySession, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run()));
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

TFuture<void> TParallelMultiReaderManager::WaitForReadySession()
{
    return ReadySessions_.Dequeue().Apply(
        BIND(&TParallelMultiReaderManager::OnSessionReady, MakeWeak(this))
            .AsyncVia(ReaderInvoker_));
}


void TParallelMultiReaderManager::OnSessionReady(const TErrorOr<TMultiReaderManagerSession>& errorOrSession)
{
    if (errorOrSession.IsOK()) {
        CurrentSession_ = errorOrSession.Value();
        ReaderSwitched_.Fire();
    } else if (errorOrSession.FindMatching(NYT::EErrorCode::Canceled)) {
        // Do nothing, this is normal reader termination, e.g. during interrupt.
    } else {
        THROW_ERROR errorOrSession;
    }
}

void TParallelMultiReaderManager::WaitForReader(const TMultiReaderManagerSession& session)
{
    session.Reader->GetReadyEvent().Subscribe(
        BIND(&TParallelMultiReaderManager::OnReaderReady, MakeWeak(this), session)
            .Via(ReaderInvoker_));
}

void TParallelMultiReaderManager::OnReaderReady(const TMultiReaderManagerSession& session, const TError& error)
{
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
    auto guard = Guard(ActiveReadersLock_);

    state.CurrentReader = CurrentSession_.Reader;
    state.ActiveReaders.reserve(ActiveReaders_.size());
    for (const auto& reader : ActiveReaders_) {
        if (reader != CurrentSession_.Reader) {
            state.ActiveReaders.push_back(reader);
        }
    }
    for (int index : NonOpenedReaderIndexes_) {
        state.ReaderFactories.push_back(ReaderFactories_[index]);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

IMultiReaderManagerPtr CreateParallelMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    return New<TParallelMultiReaderManager>(
        std::move(config),
        std::move(options),
        std::move(readerFactories),
        std::move(multiReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
