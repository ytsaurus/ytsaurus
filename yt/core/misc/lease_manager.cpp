#include "stdafx.h"
#include "lease_manager.h"

#include <core/actions/bind.h>

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/thread_affinity.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TLeaseManager::TLease TLeaseManager::NullLease = TLeaseManager::TLease();

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager::TImpl
    : private TNonCopyable
{
public:
    typedef TLeaseManager::TLease TLease;

    static TLease CreateLease(TDuration timeout, const TClosure& onExpired)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(onExpired);

        auto lease = New<TEntry>(timeout, onExpired);
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            timeout);
        return lease;
    }

    static bool RenewLease(TLease lease, TNullable<TDuration> timeout)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(lease);

        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid)
            return false;

        TDelayedExecutor::Cancel(lease->Cookie);
        if (timeout) {
            lease->Timeout = timeout.Get();
        }
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            lease->Timeout);
        return true;
    }

    static bool CloseLease(TLease lease)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(lease);

        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid)
            return false;

        InvalidateLease(lease);
        return true;
    }

private:
    static void OnLeaseExpired(TLease lease)
    {
        TGuard<TSpinLock> guard(lease->SpinLock);
        if (!lease->IsValid)
            return;

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

TLeaseManager::TLease TLeaseManager::CreateLease(TDuration timeout, const TClosure& onExpired)
{
    return TImpl::CreateLease(timeout, onExpired);
}

bool TLeaseManager::RenewLease(TLease lease, TNullable<TDuration> timeout)
{
    return TImpl::RenewLease(lease, timeout);
}

bool TLeaseManager::CloseLease(TLease lease)
{
    return TImpl::CloseLease(lease);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

