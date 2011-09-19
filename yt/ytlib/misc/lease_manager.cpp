#include "lease_manager.h"

#include "../actions/action_util.h"

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

void TLeaseManager::ExpireLease(TLease lease)
{
    if (EraseLease(lease)) {
        lease->OnExpire->Do();
    }
}

TLeaseManager::TLease TLeaseManager::CreateLease(TDuration timeout, IAction::TPtr onExpire)
{
    TLease entry = New<TEntry>(timeout, onExpire);
    entry->Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaseManager::ExpireLease, TPtr(this), entry),
        timeout);
    {
        TGuard<TSpinLock> guard(SpinLock);
        Leases.insert(entry);
    }
    return entry;
}

bool TLeaseManager::RenewLease(TLease lease)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!Leases.has(lease))
        return false;

    TDelayedInvoker::Get()->Cancel(lease->Cookie);
    lease->Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaseManager::ExpireLease, this, lease),
        lease->Timeout);
    return true;
}

bool TLeaseManager::EraseLease(TLease lease)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = Leases.find(lease);
    if (it == Leases.end())
        return false;

    Leases.erase(it);
    return true;
}

bool TLeaseManager::CloseLease(TLease lease)
{
    if (!EraseLease(lease)) {
        return false;
    }
    TDelayedInvoker::Get()->Cancel(lease->Cookie);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

}

