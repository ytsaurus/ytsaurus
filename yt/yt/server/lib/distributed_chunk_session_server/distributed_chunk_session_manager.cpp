#include "distributed_chunk_session_manager.h"

#include "distributed_chunk_session_sequencer.h"
#include "private.h"

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

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

        auto guard = WriterGuard(SequencerMapLock_);

        if (Sequencers_.contains(sessionId)) {
            guard.Release();

            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::SessionAlreadyExists,
                "Chunk write session %v has already been registered",
                sessionId.ChunkId);
        }

        auto lease = TLeaseManager::CreateLease(
            sessionTimeout,
            BIND_NO_PROPAGATE(&TDistributedChunkSessionManager::OnSequencerLeaseExpired,
                MakeWeak(this),
                sessionId)
                .Via(Invoker_));

        auto sequencer = CreateDistributedChunkSessionSequencer(
            sessionId,
            std::move(targets),
            std::move(options),
            std::move(config),
            Connection_,
            Invoker_);

        sequencer->GetClosedFuture().Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionManager::OnSequencerFinished,
            MakeWeak(this),
            sessionId));

        EmplaceOrCrash(Sequencers_, sessionId, std::pair(sequencer, std::move(lease)));
        YT_LOG_INFO(
            "Sequencer started (SessionId: %v, SessionTimeout: %v)",
            sessionId,
            sessionTimeout);

        return sequencer->Open().Apply(BIND([sessionId] () {
            YT_LOG_INFO("Sequencer opened (SessionId: %v)", sessionId);
            return;
        }));
    }

    void RenewSessionLease(TSessionId sessionId) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TLease lease;
        {
            auto guard = ReaderGuard(SequencerMapLock_);
            lease = DoGetSequencerOrThrow(sessionId)->second;
        }

        TLeaseManager::RenewLease(std::move(lease));
    }

private:
    const IInvokerPtr Invoker_;
    const IConnectionPtr Connection_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, SequencerMapLock_);
    THashMap<TSessionId, std::pair<IDistributedChunkSessionSequencerPtr, TLease>> Sequencers_;

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
        YT_LOG_INFO("Sequencer lease expired, closing (SessionId: %v)", sessionId);
        sequencer->Close().Subscribe(BIND([sessionId] (const TError& error) {
            YT_LOG_INFO(error, "Sequencer session has been closed (SessionId: %v)", sessionId);
        }));
    }

    void OnSequencerFinished(TSessionId sessionId, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_INFO(error, "Sequencer finished (SessionId: %v)", sessionId);

        IDistributedChunkSessionSequencerPtr sequencer;
        auto guard = WriterGuard(SequencerMapLock_);
        auto it = Sequencers_.find(sessionId);
        if (it != Sequencers_.end()) {
            // Prevent destruction under lock.
            std::swap(sequencer, it->second.first);
            Sequencers_.erase(it);
        }
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
