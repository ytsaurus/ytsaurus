#include "multi_reader_manager_base.h"

#include <library/cpp/yt/misc/variant.h>

#include <variant>

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
    struct TReaderFinishedEvent
    { };

    using TSessionReadyEvent = TErrorOr<TMultiReaderManagerSession>;
    using TEvent = std::variant<TReaderFinishedEvent, TSessionReadyEvent>;
    using TEventQueue = TNonblockingQueue<TEvent>;

    TEventQueue EventQueue_;
    int FinishedReaderCount_ = 0;

    TFuture<void> DoOpen() override;

    void OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex) override;
    void OnReaderBlocked() override;
    void OnReaderFinished() override;

    TFuture<void> WaitForReadySession();
    void OnEvent(const TEvent& event);

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
    EventQueue_.Enqueue(TSessionReadyEvent(session));
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
        EventQueue_.Enqueue(TReaderFinishedEvent{});
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
        EventQueue_.Enqueue(TReaderFinishedEvent{});
    } else {
        EventQueue_.Enqueue(TSessionReadyEvent(TError("Multi reader failed") << error));
    }
}

TFuture<void> TParallelMultiReaderManager::WaitForReadySession()
{
    return EventQueue_.Dequeue().Apply(
        BIND(&TParallelMultiReaderManager::OnEvent, MakeWeak(this))
            .AsyncVia(ReaderInvoker_));
}


void TParallelMultiReaderManager::OnEvent(const TEvent& event)
{
    Visit(
        event,
        [] (const TReaderFinishedEvent& /*event*/) {
            // Do nothing, this is normal reader termination, e.g. during interrupt.
        },
        [&] (const TSessionReadyEvent& errorOrSession) {
            if (errorOrSession.IsOK()) {
                CurrentSession_ = errorOrSession.Value();
                ReaderSwitched_.Fire();
            } else {
                THROW_ERROR errorOrSession;
            }
        });
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
        EventQueue_.Enqueue(TSessionReadyEvent(session));
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
