#include "session_manager.h"

#include "private.h"
#include "bus.h"
#include "commands.h"
#include "connection.h"
#include "driver.h"
#include "protocol.h"
#include "session.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;

static const auto& Logger = ZookeeperLogger;

////////////////////////////////////////////////////////////////////////////////

class TSessionManager
    : public ISessionManager
{
public:
    explicit TSessionManager(IDriverPtr driver, IPollerPtr poller, IInvokerPtr invoker)
        : Driver_(std::move(driver))
        , Poller_(std::move(poller))
        , Invoker_(std::move(invoker))
    { }

    void OnConnectionAccepted(const NNet::IConnectionPtr& connection) override
    {
        try {
            GuardedOnConnectionAccepted(connection);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG("Failed to start session, terminating connaction (RemoteAddress: %v)",
                connection->RemoteAddress());

            // Fire and forget.
            connection->Abort();
        }
    }

private:
    const IDriverPtr Driver_;
    const IPollerPtr Poller_;
    const IInvokerPtr Invoker_;

    THashMap<TSessionId, ISessionPtr> Sessions_;
    THashMap<TSessionId, IConnectionPtr> Connections_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SessionsLock_);

    ISessionPtr FindSession(TSessionId sessionId) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SessionsLock_);

        auto sessionIt = Sessions_.find(sessionId);
        if (sessionIt == Sessions_.end()) {
            return nullptr;
        } else {
            return sessionIt->second;
        }
    }

    IConnectionPtr FindConnection(TSessionId sessionId) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SessionsLock_);

        auto connectionIt = Connections_.find(sessionId);
        if (connectionIt == Connections_.end()) {
            return nullptr;
        } else {
            return connectionIt->second;
        }
    }

    TSessionId GenerateSessionId()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // TODO(gritukan): Use RNG with guaranteed period.
        return RandomNumber<ui64>();
    }

    void GuardedOnConnectionAccepted(const NNet::IConnectionPtr& connection)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto rawRequest = ReceiveMessage(connection);
        auto reader = CreateZookeeperProtocolReader(rawRequest);
        auto req = New<TReqStartSession>();
        req->Deserialize(reader.get());

        auto rsp = DoStartSession(req, connection);
        auto writer = CreateZookeeperProtocolWriter();
        rsp->Serialize(writer.get());

        PostMessage(writer->Finish(), connection);
    }

    TRspStartSessionPtr DoStartSession(
        TReqStartSessionPtr req,
        const NNet::IConnectionPtr& netConnection)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Starting Zookeeper session "
            "(ProtocolVersion: %v, LastZxidSeend: %v, Timeout: %v, SessionId: %v, "
            "Password: %v, ReadOnly: %v)",
            req->ProtocolVersion,
            req->LastZxidSeen,
            req->Timeout,
            req->SessionId,
            req->Password,
            req->ReadOnly);

        auto sessionId = GenerateSessionId();
        req->SessionId = sessionId;

        auto timeout = req->Timeout;

        auto lease = TLeaseManager::CreateLease(
            timeout,
            BIND(&TSessionManager::OnSessionLeaseExpited, MakeWeak(this), sessionId));

        auto session = CreateSession(req, lease);
        auto connection = CreateConnection(
            netConnection,
            Poller_,
            Invoker_,
            BIND(&TSessionManager::HandleRequest, MakeStrong(this), sessionId),
            BIND(&TSessionManager::HandleConnectionFailure, MakeStrong(this), sessionId),
            /*loggingTag*/ Format("SessionId: %v", sessionId));
        connection->Start();

        RegisterSession(session, connection);

        auto rsp = New<TRspStartSession>();
        rsp->ProtocolVersion = 1;
        rsp->Timeout = timeout;
        rsp->SessionId = sessionId;
        rsp->Password = "very_secure_password";
        rsp->ReadOnly = false;

        return rsp;
    }

    void RegisterSession(
        const ISessionPtr& session,
        const IConnectionPtr& connection)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SessionsLock_);

        auto sessionId = session->GetId();

        EmplaceOrCrash(Sessions_, sessionId, session);
        EmplaceOrCrash(Connections_, sessionId, connection);

        YT_LOG_DEBUG("Session registered (SessionId: %v)",
            sessionId);

        // NB: Race between lease creation and session registration is possible.
        if (!TLeaseManager::RenewLease(session->GetLease())) {
            OnSessionLeaseExpited(session->GetId());
        }
    }

    void UnregisterSession(TSessionId sessionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SessionsLock_);

        if (!Sessions_.contains(sessionId)) {
            return;
        }

        EraseOrCrash(Sessions_, sessionId);
        EraseOrCrash(Connections_, sessionId);

        YT_LOG_DEBUG("Session unregistered (SessionId: %v)",
            sessionId);
    }

    void TerminateSession(TSessionId sessionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto connection = FindConnection(sessionId)) {
            auto error = WaitFor(connection->Stop());
            if (!error.IsOK()) {
                YT_LOG_WARNING(error, "Failed to termitate connection (SessionId: %v)",
                    sessionId);
            }
        }

        UnregisterSession(sessionId);
    }

    void OnSessionLeaseExpited(TSessionId sessionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto session = FindSession(sessionId)) {
            YT_LOG_DEBUG("Session lease expired, terminating "
                "(SessionId: %v, Timeout: %v)",
                session->GetId(),
                session->GetTimeout());

            TerminateSession(sessionId);
        }
    }

    TSharedRef HandleRequest(TSessionId sessionId, TSharedRef request) noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto session = FindSession(sessionId)) {
            TLeaseManager::RenewLease(session->GetLease());

            return Driver_->ExecuteRequest(session, request);
        } else {
            YT_LOG_DEBUG("Received request for unregistered session, ignored (SessionId: %v)",
                sessionId);
            return {};
        }
    }

    void HandleConnectionFailure(TSessionId sessionId, TError error) noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG(error, "Session connection failed, termintating (SessionId: %v)",
            sessionId);

        TerminateSession(sessionId);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateSessionManager(IDriverPtr driver, IPollerPtr poller, IInvokerPtr invoker)
{
    return New<TSessionManager>(std::move(driver), std::move(poller), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
