#include "session_manager.h"
#include "session.h"
#include "private.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryAgent {

using namespace NCompression;
using namespace NConcurrency;
using namespace NLogging;
using namespace NQueryClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedSessionManager
    : public IDistributedSessionManager
{
public:
    explicit TDistributedSessionManager(IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
    { }

    IDistributedSessionPtr GetDistributedSessionOrCreate(
        TDistributedSessionId sessionId,
        TDuration retentionTime,
        ECodec codecId) override
    {
        THashMap<TDistributedSessionId, IDistributedSessionPtr>::iterator it;
        {
            auto guard = Guard(DistributedSessionMapLock_);

            it = DistributedSessionMap_.find(sessionId);
            if (it != DistributedSessionMap_.end()) {
                return it->second;
            }
        }

        auto lease = TLeaseManager::CreateLease(
            retentionTime,
            BIND(&TDistributedSessionManager::OnDistributedSessionLeaseExpired, MakeWeak(this), sessionId)
                .Via(Invoker_));
        auto session = CreateDistributedSession(sessionId, std::move(lease), codecId, retentionTime);

        {
            auto guard = Guard(DistributedSessionMapLock_);

            it = DistributedSessionMap_.find(sessionId);
            if (it == DistributedSessionMap_.end()) {
                EmplaceOrCrash(DistributedSessionMap_, sessionId, std::move(session));
                return session;
            } else {
                return it->second;
            }
        }
    }

    IDistributedSessionPtr GetDistributedSessionOrThrow(TDistributedSessionId sessionId) override
    {
        auto guard = Guard(DistributedSessionMapLock_);
        auto it = DistributedSessionMap_.find(sessionId);

        THROW_ERROR_EXCEPTION_IF(it == DistributedSessionMap_.end(), "Distributed query session %v not found",
            sessionId);

        return it->second;
    }

    bool CloseDistributedSession(TDistributedSessionId sessionId) override
    {
        auto guard = Guard(DistributedSessionMapLock_);

        auto it = DistributedSessionMap_.find(sessionId);
        if (it != DistributedSessionMap_.end()) {
            DistributedSessionMap_.erase(it);
            return true;
        } else {
            return false;
        }
    }

    void OnDistributedSessionLeaseExpired(TDistributedSessionId sessionId) override
    {
        YT_LOG_DEBUG("Distributed query session lease expired (SessionId: %v)",
            sessionId);

        if (CloseDistributedSession(sessionId)) {
            YT_LOG_DEBUG("Distributed query session closed by expiration (SessionId: %v)",
                sessionId);
        }
    }

private:
    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, DistributedSessionMapLock_);
    THashMap<TDistributedSessionId, IDistributedSessionPtr> DistributedSessionMap_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedSessionManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionManagerPtr CreateDistributedSessionManager(IInvokerPtr invoker)
{
    return New<TDistributedSessionManager>(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
