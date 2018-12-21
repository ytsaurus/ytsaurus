#include "lease_manager.h"
#include "delayed_executor.h"
#include "thread_affinity.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

const TLease NullLease = TLease();

////////////////////////////////////////////////////////////////////////////////

struct TLeaseEntry
    : public TIntrinsicRefCounted
{
    bool IsValid = true;
    TDuration Timeout;
    TClosure OnExpired;
    NConcurrency::TDelayedExecutorCookie Cookie;
    TSpinLock SpinLock;

    TLeaseEntry(TDuration timeout, TClosure onExpired)
        : Timeout(timeout)
        , OnExpired(std::move(onExpired))
    { }
};

DEFINE_REFCOUNTED_TYPE(TLeaseEntry)

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager::TImpl
    : private TNonCopyable
{
public:
    static TLease CreateLease(TDuration timeout, TClosure onExpired)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Y_ASSERT(onExpired);

        auto lease = New<TLeaseEntry>(timeout, std::move(onExpired));
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            timeout);
        return lease;
    }

    static bool RenewLease(TLease lease, std::optional<TDuration> timeout)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Y_ASSERT(lease);

        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid)
            return false;

        TDelayedExecutor::CancelAndClear(lease->Cookie);
        if (timeout) {
            lease->Timeout = *timeout;
        }
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            lease->Timeout);
        return true;
    }

    static bool CloseLease(TLease lease)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!lease) {
            return false;
        }

        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid) {
            return false;
        }

        InvalidateLease(lease);
        return true;
    }

private:
    static void OnLeaseExpired(TLease lease, bool /*aborted*/)
    {
        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid) {
            return;
        }

        auto onExpired = lease->OnExpired;
        InvalidateLease(lease);
        guard.Release();

        onExpired.Run();
    }

    static void InvalidateLease(TLease lease)
    {
        VERIFY_SPINLOCK_AFFINITY(lease->SpinLock);

        TDelayedExecutor::CancelAndClear(lease->Cookie);
        lease->IsValid = false;
        lease->OnExpired.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TLease TLeaseManager::CreateLease(TDuration timeout, TClosure onExpired)
{
    return TImpl::CreateLease(timeout, std::move(onExpired));
}

bool TLeaseManager::RenewLease(TLease lease, std::optional<TDuration> timeout)
{
    return TImpl::RenewLease(std::move(lease), timeout);
}

bool TLeaseManager::CloseLease(TLease lease)
{
    return TImpl::CloseLease(std::move(lease));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency::NYT


