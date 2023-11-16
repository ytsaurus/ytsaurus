#include "session.h"
#include "private.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryAgent {

using namespace NConcurrency;
using namespace NLogging;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TDistributedSession
    : public IDistributedSession
{
public:
    TDistributedSession(
        TDistributedSessionId sessionId,
        TLease lease)
        : SessionId_(sessionId)
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

        PropagationAddressQueue_.insert(std::move(address));
    }

    std::vector<TString> GetPropagationAddresses() const override
    {
        auto guard = Guard(SessionLock_);

        return std::vector(PropagationAddressQueue_.begin(), PropagationAddressQueue_.end());
    }

    void ErasePropagationAddresses(const std::vector<TString>& addresses) override
    {
        auto guard = Guard(SessionLock_);

        for (const auto& address : addresses) {
            PropagationAddressQueue_.erase(address);
        }
    }

private:
    const NQueryClient::TDistributedSessionId SessionId_;
    const NConcurrency::TLease Lease_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SessionLock_);
    THashSet<TString> PropagationAddressQueue_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(TDistributedSessionId sessionId, TLease lease)
{
    return New<TDistributedSession>(sessionId, std::move(lease));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
