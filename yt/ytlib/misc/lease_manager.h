#pragma once

#include "hash.h"

#include "../misc/delayed_invoker.h"

#include "../actions/action.h"
#include "../actions/invoker.h"

namespace NYT 
{

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager
    : public TRefCountedBase
{
    struct TEntry
        : public TRefCountedBase
    {
        TDuration Timeout;
        IAction::TPtr OnExpire;
        TDelayedInvoker::TCookie Cookie;

        TEntry(TDuration delay, IAction::TPtr onExpire)
            : Timeout(delay)
            , OnExpire(onExpire)
        {}
    };

public:
    typedef TIntrusivePtr<TLeaseManager> TPtr;
    typedef TIntrusivePtr<TEntry> TLease;

    TLease CreateLease(TDuration timeout, IAction::TPtr expiryCallback);
    bool RenewLease(TLease lease);
    bool CloseLease(TLease lease);

private:
    typedef yhash_set<TLease> TLeases;

    TSpinLock SpinLock;
    TLeases Leases;

    bool EraseLease(TLease lease);
    void ExpireLease(TLease lease);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
