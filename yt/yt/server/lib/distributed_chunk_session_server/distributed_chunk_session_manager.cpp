#include "distributed_chunk_session_manager.h"

#include "config.h"
#include "distributed_chunk_session_coordinator.h"
#include "private.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/lease_manager.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NThreading;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DistributedChunkSessionServiceLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionManager
    : public IDistributedChunkSessionManager
{
public:
    TDistributedChunkSessionManager(
        TDistributedChunkSessionServiceConfigPtr config,
        IInvokerPtr invoker,
        IConnectionPtr connection)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , Connection_(std::move(connection))
    { }

    IDistributedChunkSessionCoordinatorPtr FindCoordinator(TSessionId sessionId) const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(CoordinatorMapLock_);
        const auto* coordinator = DoFindCoordinatorGuarded(sessionId);
        return coordinator ? coordinator->first : nullptr;
    }

    IDistributedChunkSessionCoordinatorPtr GetCoordinatorOrThrow(TSessionId sessionId) const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(CoordinatorMapLock_);
        return DoGetCoordinatorOrThrow(sessionId)->first;
    }

    IDistributedChunkSessionCoordinatorPtr StartSession(
        TSessionId sessionId,
        std::vector<TNodeDescriptor> targets) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(CoordinatorMapLock_);

        if (Coordinators_.contains(sessionId)) {
            guard.Release();

            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::SessionAlreadyExists,
                "Chunk write session %v has already been registered",
                sessionId.ChunkId);
        }

        auto lease = TLeaseManager::CreateLease(
            Config_->SessionTimeout,
            BIND_NO_PROPAGATE(&TDistributedChunkSessionManager::OnCoordinatorLeaseExpired, MakeStrong(this), sessionId)
                .Via(Invoker_));

        auto session = CreateDistributedChunkSessionCoordinator(
            Config_,
            sessionId,
            std::move(targets),
            Invoker_,
            Connection_);

        session->StartSession().Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionManager::OnCoordinatorFinished,
            MakeStrong(this),
            sessionId));

        EmplaceOrCrash(Coordinators_, sessionId, std::pair(session, std::move(lease)));
        YT_LOG_INFO("Coordinator started (SessionId: %v)", sessionId);

        return session;
    }

    void RenewSessionLease(TSessionId sessionId) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TLease lease;
        {
            auto guard = ReaderGuard(CoordinatorMapLock_);
            lease = DoGetCoordinatorOrThrow(sessionId)->second;
        }

        TLeaseManager::RenewLease(std::move(lease));
    }

private:
    const TDistributedChunkSessionServiceConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IConnectionPtr Connection_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, CoordinatorMapLock_);
    THashMap<TSessionId, std::pair<IDistributedChunkSessionCoordinatorPtr, TLease>> Coordinators_;

    const std::pair<IDistributedChunkSessionCoordinatorPtr, TLease>* DoFindCoordinatorGuarded(TSessionId sessionId) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(CoordinatorMapLock_);

        auto it = Coordinators_.find(sessionId);
        return it == Coordinators_.end() ? nullptr : &it->second;
    }

    const std::pair<IDistributedChunkSessionCoordinatorPtr, TLease>* DoGetCoordinatorOrThrow(TSessionId sessionId) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(CoordinatorMapLock_);

        const auto* coordinator = DoFindCoordinatorGuarded(sessionId);
        if (!coordinator) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchSession,
                "Chunk write session %v is invalid or expired",
                sessionId);
        }
        return coordinator;
    }

    void OnCoordinatorLeaseExpired(TSessionId sessionId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto coordinator = FindCoordinator(sessionId);
        if (!coordinator) {
            return;
        }
        YT_LOG_INFO("Coordinator lease expired, closing (SessionId: %v)", sessionId);
        coordinator->Close(/*force*/ true).Subscribe(BIND([sessionId] (const TError& error) {
            YT_LOG_INFO(error, "Coordinator session has been closed (SessionId: %v)", sessionId);
        }));
    }

    void OnCoordinatorFinished(TSessionId sessionId, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_INFO(error, "Coordinator finished (SessionId: %v)", sessionId);

        IDistributedChunkSessionCoordinatorPtr coordinator;
        {
            auto guard = WriterGuard(CoordinatorMapLock_);
            auto it = Coordinators_.find(sessionId);
            if (it != Coordinators_.end()) {
                // Prevent destruction under lock.
                coordinator = std::move(it->second.first);
                Coordinators_.erase(it);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionManagerPtr CreateDistributedChunkSessionManager(
    TDistributedChunkSessionServiceConfigPtr config,
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionManager>(
        std::move(config),
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
