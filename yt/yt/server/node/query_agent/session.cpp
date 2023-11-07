#include "private.h"
#include "session.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryAgent {

using namespace NConcurrency;
using namespace NLogging;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedSession)

class TDistributedSession
    : public IDistributedSession
{
public:
    TDistributedSession(
        TSessionId id,
        TLease lease)
        : Id_(id)
        , Lease_(std::move(lease))
    { }

    void RenewLease() const override
    {
        if (Lease_) {
            TLeaseManager::RenewLease(Lease_);
        }
    }

    void PropagateToNode(TString address) override
    {
        auto guard = Guard(SessionLock_);

        PropagationAddresses_.insert(std::move(address));
    }

    std::vector<TString> CopyPropagationRequests() const override
    {
        auto guard = Guard(SessionLock_);

        return std::vector(PropagationAddresses_.begin(), PropagationAddresses_.end());
    }

    void FulfillPropagationRequests(const std::vector<TString>& addresses) override
    {
        auto guard = Guard(SessionLock_);

        for (const auto& address : addresses) {
            PropagationAddresses_.erase(address);
        }
    }

    ~TDistributedSession()
    {
        TLeaseManager::CloseLease(Lease_);
    }

private:
    const NQueryClient::TSessionId Id_;
    const NConcurrency::TLease Lease_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SessionLock_);
    THashSet<TString> PropagationAddresses_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedSession)

////////////////////////////////////////////////////////////////////////////////

class TSessionManager
    : public ISessionManager
{
public:
    TSessionManager(
        IInvokerPtr invoker,
        const TLogger logger)
        : Logger(logger)
        , Invoker_(std::move(invoker))
    { }

    TWeakPtr<IDistributedSession> GetOrCreate(TSessionId id, TDuration retentionTime) override
    {
        THashMap<TSessionId, TDistributedSessionPtr>::iterator it;
        {
            auto guard = Guard(SessionMapLock_);

            it = SessionMap_.find(id);

            if (it != SessionMap_.end()) {
                return it->second;
            }
        }

        auto lease = TLeaseManager::CreateLease(
            retentionTime,
            BIND(&TSessionManager::UponSessionLeaseExpiration, MakeWeak(this), id)
                .Via(Invoker_));
        auto session = New<TDistributedSession>(id, std::move(lease));

        {
            auto guard = Guard(SessionMapLock_);

            it = SessionMap_.find(id);

            if (it == SessionMap_.end()) {
                InsertOrCrash(SessionMap_, std::pair{id, std::move(session)});
                return session;
            } else {
                return it->second;
            }
        }
    }

    TWeakPtr<IDistributedSession> GetOrThrow(TSessionId id) override
    {
        auto guard = Guard(SessionMapLock_);
        auto it = SessionMap_.find(id);

        THROW_ERROR_EXCEPTION_IF(it == SessionMap_.end(), "Distributed query session %v not found",
            id);

        return it->second;
    }

    bool InvalidateIfExists(TSessionId id) override
    {
        auto guard = Guard(SessionMapLock_);

        auto it = SessionMap_.find(id);

        if (it != SessionMap_.end()) {
            SessionMap_.erase(it);
            return true;
        } else {
            return false;
        }
    }

    void UponSessionLeaseExpiration(TSessionId id) override
    {
        YT_LOG_INFO("Distributed query session lease expired (SessionId: %v)", id);

        if (InvalidateIfExists(id)) {
            YT_LOG_INFO("Session closed by expiration (SessionId: %v)", id);
        }
    }

private:
    const NLogging::TLogger Logger;
    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SessionMapLock_);
    THashMap<TSessionId, TDistributedSessionPtr> SessionMap_;
};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateNewSessionManager(
    IInvokerPtr invoker,
    const NLogging::TLogger logger)
{
    return New<TSessionManager>(std::move(invoker), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
