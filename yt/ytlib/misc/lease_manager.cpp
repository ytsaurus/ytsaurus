#include "stdafx.h"
#include "lease_manager.h"

#include "../misc/delayed_invoker.h"
#include "../misc/thread_affinity.h"
#include "../actions/action_util.h"

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

TLeaseManager* TLeaseManager::Get()
{
    return Singleton<TLeaseManager>();
}

TLeaseManager::TLease TLeaseManager::CreateLease(
    TDuration timeout,
    IAction::TPtr onExpire)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~onExpire != NULL);

    auto lease = New<TEntry>(timeout, onExpire);
    lease->Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaseManager::ExpireLease, this, lease),
        timeout);
    return lease;
}

bool TLeaseManager::RenewLease(TLease lease)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~lease != NULL);

    TGuard<TSpinLock> guard(lease->SpinLock);
    if (!lease->IsValid)
        return false;

    TDelayedInvoker::Get()->Cancel(lease->Cookie);
    lease->Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaseManager::ExpireLease, this, lease),
        lease->Timeout);
    return true;
}

bool TLeaseManager::CloseLease(TLease lease)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~lease != NULL);

    TGuard<TSpinLock> guard(lease->SpinLock);
    if (!lease->IsValid)
        return false;
    
    InvalidateLease(lease);
    return true;
}

void TLeaseManager::ExpireLease(TLease lease)
{
    TGuard<TSpinLock> guard(lease->SpinLock);
    if (lease->IsValid) {
        IAction::TPtr onExpire = lease->OnExpire;
        InvalidateLease(lease);
        guard.Release();
        onExpire->Do();
    }
}

void TLeaseManager::InvalidateLease(TLease lease)
{
    VERIFY_SPINLOCK_AFFINITY(lease->SpinLock);

    TDelayedInvoker::Get()->Cancel(lease->Cookie);
    lease->Cookie = TDelayedInvoker::TCookie();
    lease->IsValid = false;
    lease->OnExpire.Reset();
}

////////////////////////////////////////////////////////////////////////////////

}

