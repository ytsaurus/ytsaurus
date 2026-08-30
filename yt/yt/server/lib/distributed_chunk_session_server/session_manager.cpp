#include "session_manager.h"

#include "private.h"
#include "sequencer.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NThreading;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit const auto Logger = DistributedChunkSessionServiceLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionManager
    : public IDistributedChunkSessionManager
{
public:
    TDistributedChunkSessionManager(
        IInvokerPtr invoker,
        IConnectionPtr connection)
        : Invoker_(std::move(invoker))
        , Connection_(std::move(connection))
    { }

    IDistributedChunkSessionSequencerPtr GetSequencerOrThrow(TSessionId sessionId) const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(SequencerMapLock_);
        return DoGetSequencerOrThrow(sessionId)->first;
    }

    TFuture<void> StartSession(
        TSessionId sessionId,
        TDuration sessionTimeout,
        TChunkReplicaWithMediumList targets,
        TJournalChunkWriterOptionsPtr options,
        TJournalChunkWriterConfigPtr config) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        {
            auto guard = WriterGuard(SequencerMapLock_);
            if (Sequencers_.contains(sessionId) || !StartingSessions_.insert(sessionId).second) {
                guard.Release();

                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::SessionAlreadyExists,
                    "Chunk write session %v has already been registered",
                    sessionId.ChunkId);
            }
        }

        // NB: Until the continuation below owns the reservation, nothing else would ever
        // release it and the session id would stay rejected for good.
        auto reservationGuard = Finally([&] {
            auto guard = WriterGuard(SequencerMapLock_);
            EraseOrCrash(StartingSessions_, sessionId);
        });

        auto sequencer = CreateDistributedChunkSessionSequencer(
            sessionId,
            std::move(targets),
            std::move(options),
            std::move(config),
            Connection_,
            Invoker_);

        // NB: The sequencer is published only once it is open, so it can never be closed
        // before it is opened and no session setup runs under SequencerMapLock_. The id is
        // reserved meanwhile, so a concurrent start does not open a second writer for it.
        auto sessionStarted = sequencer->Open().Apply(BIND(
            &TDistributedChunkSessionManager::RegisterOpenedSequencer,
            MakeStrong(this),
            sessionId,
            sessionTimeout,
            sequencer));

        reservationGuard.Release();

        return sessionStarted;
    }

    IDistributedChunkSessionSequencerPtr RenewSessionLeaseAndGetSequencerOrThrow(
        TSessionId sessionId) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto [sequencer, lease] = [&] {
            auto guard = ReaderGuard(SequencerMapLock_);
            return *DoGetSequencerOrThrow(sessionId);
        }();

        THROW_ERROR_EXCEPTION_IF(
            !TLeaseManager::RenewLease(std::move(lease)),
            NChunkClient::EErrorCode::NoSuchSession,
            "Lease of chunk write session %v has expired",
            sessionId);
        return std::move(sequencer);
    }

private:
    const IInvokerPtr Invoker_;
    const IConnectionPtr Connection_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, SequencerMapLock_);
    THashMap<TSessionId, std::pair<IDistributedChunkSessionSequencerPtr, TLease>> Sequencers_;
    THashSet<TSessionId> StartingSessions_;

    void RegisterOpenedSequencer(
        TSessionId sessionId,
        TDuration sessionTimeout,
        const IDistributedChunkSessionSequencerPtr& sequencer,
        const TError& openError)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!openError.IsOK()) {
            YT_TLOG_INFO("Failed to open sequencer")
                .With("SessionId", sessionId)
                .With(openError);

            auto guard = WriterGuard(SequencerMapLock_);
            EraseOrCrash(StartingSessions_, sessionId);
            guard.Release();

            THROW_ERROR(openError);
        }

        auto lease = TLeaseManager::CreateLease(
            sessionTimeout,
            BIND_NO_PROPAGATE(&TDistributedChunkSessionManager::OnSequencerLeaseExpired,
                MakeWeak(this),
                sessionId)
                .Via(Invoker_));

        {
            auto guard = WriterGuard(SequencerMapLock_);
            EraseOrCrash(StartingSessions_, sessionId);
            EmplaceOrCrash(Sequencers_, sessionId, std::pair(sequencer, std::move(lease)));
        }

        // NB: A sequencer that closed between the insert and this subscription is still
        // erased, since Subscribe runs the handler right away on an already set future.
        sequencer->GetClosedFuture().Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionManager::OnSequencerFinished,
            MakeWeak(this),
            sessionId));

        YT_TLOG_INFO("Sequencer started")
            .With("SessionId", sessionId)
            .With("SessionTimeout", sessionTimeout);
    }

    IDistributedChunkSessionSequencerPtr FindSequencer(TSessionId sessionId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(SequencerMapLock_);
        const auto* sequencer = DoFindSequencerGuarded(sessionId);
        return sequencer ? sequencer->first : nullptr;
    }

    const std::pair<IDistributedChunkSessionSequencerPtr, TLease>* DoFindSequencerGuarded(TSessionId sessionId) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SequencerMapLock_);

        auto it = Sequencers_.find(sessionId);
        return it == Sequencers_.end() ? nullptr : &it->second;
    }

    const std::pair<IDistributedChunkSessionSequencerPtr, TLease>* DoGetSequencerOrThrow(TSessionId sessionId) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SequencerMapLock_);

        const auto* sequencer = DoFindSequencerGuarded(sessionId);
        if (!sequencer) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchSession,
                "Chunk write session %v is invalid or expired",
                sessionId);
        }
        return sequencer;
    }

    void OnSequencerLeaseExpired(TSessionId sessionId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto sequencer = FindSequencer(sessionId);
        if (!sequencer) {
            return;
        }
        YT_TLOG_INFO("Sequencer lease expired, closing")
            .With("SessionId", sessionId);
        sequencer->Close().AsVoid().Subscribe(BIND_NO_PROPAGATE([sessionId] (const TError& error) {
            YT_TLOG_INFO("Sequencer session has been closed")
                .With("SessionId", sessionId)
                .With(error);
        }));
    }

    void OnSequencerFinished(TSessionId sessionId, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_TLOG_INFO("Sequencer finished")
            .With("SessionId", sessionId)
            .With(error);

        // NB: The sequencer and its lease are moved out so that neither is destroyed
        // under the lock.
        auto entry = [&] {
            auto guard = WriterGuard(SequencerMapLock_);
            auto it = Sequencers_.find(sessionId);
            YT_VERIFY(it != Sequencers_.end());
            auto entry = std::move(it->second);
            Sequencers_.erase(it);
            return entry;
        }();

        TLeaseManager::CloseLease(std::move(entry.second));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IDistributedChunkSessionManagerPtr CreateDistributedChunkSessionManager(
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionManager>(
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
